package apisports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultTimeout bounds a single upstream call.
const DefaultTimeout = 20 * time.Second

// Envelope is the response shape every API-Sports endpoint returns.
//
// Errors is the field that matters. API-Sports answers a malformed or
// unauthorised request with HTTP 200 and the reason in this field, so a client
// that trusts the status code alone silently treats a broken query as an empty
// result. Worse, it does not have a stable type: a successful call returns
// `"errors": []` and a failed one `"errors": {"live": "..."}`. Both were
// observed live, which is why Errors is json.RawMessage and decoded by hand.
type Envelope struct {
	Get        string          `json:"get"`
	Parameters json.RawMessage `json:"parameters"`
	Errors     json.RawMessage `json:"errors"`
	Results    int             `json:"results"`
	Paging     Paging          `json:"paging"`
	Response   json.RawMessage `json:"response"`

	// Header is the response's HTTP headers, carried alongside the body so a
	// caller can read the provider's Date — the only clock signal this API
	// offers, and the basis of the drift measurement. Not part of the wire
	// document, hence excluded from JSON.
	Header http.Header `json:"-"`
	// ReceivedAt is when the response was read.
	ReceivedAt time.Time `json:"-"`
}

// Paging is the provider's pagination block.
type Paging struct {
	Current int `json:"current"`
	Total   int `json:"total"`
}

// APIError is a provider-reported failure carried inside a 200 response.
type APIError struct {
	Endpoint string
	Fields   map[string]string
}

// IsPlanRestriction reports whether the provider refused because the account's
// plan does not cover the request — a date outside the free window, a season it
// does not carry.
//
// This matters because it is PERMANENT for the rest of the day, unlike every
// other error the client sees. Retrying it on the normal cadence spends real
// quota to be told the same thing again, so the scheduler treats it as a reason
// to stand the vertical down rather than as a transient failure.
func (e *APIError) IsPlanRestriction() bool {
	for k, v := range e.Fields {
		if strings.EqualFold(k, "plan") {
			return true
		}
		if strings.Contains(strings.ToLower(v), "do not have access") {
			return true
		}
	}
	return false
}

func (e *APIError) Error() string {
	keys := make([]string, 0, len(e.Fields))
	for k := range e.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s: %s", k, e.Fields[k]))
	}
	return fmt.Sprintf("apisports: %s returned %s", e.Endpoint, strings.Join(parts, "; "))
}

// decodeErrors normalises both observed shapes of the errors field.
func decodeErrors(raw json.RawMessage) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	// The success case: an empty array.
	var asArray []any
	if err := json.Unmarshal(raw, &asArray); err == nil {
		if len(asArray) == 0 {
			return nil
		}
		out := map[string]string{}
		for i, v := range asArray {
			out[strconv.Itoa(i)] = fmt.Sprint(v)
		}
		return out
	}
	var asObject map[string]any
	if err := json.Unmarshal(raw, &asObject); err == nil && len(asObject) > 0 {
		out := make(map[string]string, len(asObject))
		for k, v := range asObject {
			out[k] = fmt.Sprint(v)
		}
		return out
	}
	return nil
}

// Quota is the rate-limit state the provider reports on every response.
//
// The real header names, confirmed against a live call — they are not what the
// documentation implies and not what a reasonable person would guess:
//
//	x-ratelimit-limit                 per-MINUTE ceiling      (10 on free)
//	x-ratelimit-remaining             per-MINUTE remaining
//	x-ratelimit-requests-limit        per-DAY ceiling         (100 on free)
//	x-ratelimit-requests-remaining    per-DAY remaining
//
// Note the asymmetry: the unqualified pair is the minute window and the
// `requests`-qualified pair is the day. Reading `x-ratelimit-remaining` as the
// daily figure — the obvious misreading, and the one the brief's shorthand
// invites — would have the limiter believe it has 9 requests left for the day
// when it has 99, and throttle a venue to almost nothing.
type Quota struct {
	MinuteLimit     int
	MinuteRemaining int
	DayLimit        int
	DayRemaining    int
	// Observed is when these values were read; they age out with the window.
	Observed time.Time
	// Present is false when the response carried no rate-limit headers at all,
	// so a caller can tell "no headers" from "zero remaining".
	Present bool
}

// ParseQuota reads the rate-limit headers off a response.
func ParseQuota(h http.Header, now time.Time) Quota {
	q := Quota{Observed: now}
	get := func(name string) (int, bool) {
		v := strings.TrimSpace(h.Get(name))
		if v == "" {
			return 0, false
		}
		// Some edge nodes return a float ("9.0"); tolerate it.
		if n, err := strconv.Atoi(v); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int(f), true
		}
		return 0, false
	}
	if n, ok := get("x-ratelimit-limit"); ok {
		q.MinuteLimit, q.Present = n, true
	}
	if n, ok := get("x-ratelimit-remaining"); ok {
		q.MinuteRemaining, q.Present = n, true
	}
	if n, ok := get("x-ratelimit-requests-limit"); ok {
		q.DayLimit, q.Present = n, true
	}
	if n, ok := get("x-ratelimit-requests-remaining"); ok {
		q.DayRemaining, q.Present = n, true
	}
	return q
}

// DayFractionRemaining is the share of the daily allowance still unspent, or -1
// when the response carried no daily headers.
func (q Quota) DayFractionRemaining() float64 {
	if !q.Present || q.DayLimit <= 0 {
		return -1
	}
	return float64(q.DayRemaining) / float64(q.DayLimit)
}

// Observer is notified of every completed call, with the quota the provider
// reported. The limiter implements this to steer itself from real headers
// rather than from the tier's assumed ceilings.
type Observer interface {
	ObserveQuota(v Vertical, q Quota)
	ObserveRequest(v Vertical, status int, latency time.Duration, err error)
	ObserveThrottle(v Vertical, retryAfter time.Duration)
}

// Limiter gates outbound calls. The concrete implementation lives in
// pkg/ingest; the client takes the interface so it stays testable without one.
type Limiter interface {
	// Wait blocks until the vertical may issue a request, or the context ends.
	Wait(ctx context.Context, v Vertical) error
}

// Config configures a Client.
type Config struct {
	// APIKey authenticates every vertical.
	APIKey string
	// HTTP is the underlying client; defaults to one with DefaultTimeout.
	HTTP *http.Client
	// Limiter gates requests. Optional, but production always sets one.
	Limiter Limiter
	// Observer receives quota and outcome telemetry. Optional.
	Observer Observer
	// MaxRetries bounds the 429 backoff loop.
	MaxRetries int
	// Logger; defaults to slog.Default().
	Logger *slog.Logger
	// Now is injected for tests.
	Now func() time.Time
	// Rand seeds backoff jitter; nil uses a private source.
	Rand *rand.Rand
	// BaseURLOverride replaces every vertical's host. Tests point this at an
	// httptest server; it is empty in production.
	BaseURLOverride string
}

// Client is the unified API-Sports adapter.
//
// One client covers all twelve verticals: the credential and header structure
// is identical across hosts, so the only per-vertical state is which host to
// address and which quota bucket the call spends.
type Client struct {
	key        string
	http       *http.Client
	limiter    Limiter
	observer   Observer
	maxRetries int
	log        *slog.Logger
	now        func() time.Time
	rnd        *rand.Rand
	baseAllow  string
}

// DefaultMaxRetries is how many times a 429 is retried before giving up.
//
// Four, not more. A 429 on this API means the per-minute window is exhausted,
// and the window is sixty seconds wide; with the backoff schedule below, four
// attempts already span more than a minute. Retrying further would queue work
// behind a limit that a well-configured limiter should have prevented reaching
// in the first place — the retry loop is a safety net, not a strategy.
const DefaultMaxRetries = 4

// New builds a client.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, errors.New("apisports: no API key")
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: DefaultTimeout}
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Client{
		key: cfg.APIKey, http: cfg.HTTP, limiter: cfg.Limiter,
		observer: cfg.Observer, maxRetries: cfg.MaxRetries, log: cfg.Logger,
		now: cfg.Now, rnd: cfg.Rand, baseAllow: cfg.BaseURLOverride,
	}, nil
}

// Get issues one request against a vertical and returns the decoded envelope.
func (c *Client) Get(ctx context.Context, v Vertical, path string, params map[string]string) (*Envelope, error) {
	spec, ok := SpecFor(v)
	if !ok {
		return nil, fmt.Errorf("apisports: unknown vertical %q", v)
	}
	base := spec.BaseURL()
	if c.baseAllow != "" {
		base = c.baseAllow
	}

	q := url.Values{}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	// Sorted, so the URL is deterministic and cache/log friendly.
	sort.Strings(keys)
	for _, k := range keys {
		q.Set(k, params[k])
	}
	target := base + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		// The limiter is consulted before every attempt, retries included: a
		// retry spends quota exactly like a first try, and letting retries
		// bypass the bucket is how a throttle turns into a stampede.
		if c.limiter != nil {
			if err := c.limiter.Wait(ctx, v); err != nil {
				return nil, err
			}
		}
		env, retryAfter, explicit, err := c.attempt(ctx, v, target)
		if err == nil {
			return env, nil
		}
		lastErr = err

		var throttled *ThrottleError
		if !errors.As(err, &throttled) {
			return nil, err
		}
		if attempt == c.maxRetries {
			break
		}
		delay := c.backoff(attempt, retryAfter, explicit)
		c.log.Warn("apisports: throttled, backing off",
			"vertical", v, "attempt", attempt+1, "delay", delay.Round(time.Millisecond))
		if c.observer != nil {
			c.observer.ObserveThrottle(v, delay)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, lastErr
}

// ThrottleError is a 429 from the provider.
type ThrottleError struct {
	Vertical   Vertical
	RetryAfter time.Duration
}

func (e *ThrottleError) Error() string {
	return fmt.Sprintf("apisports: %s returned 429 (retry after %s)", e.Vertical, e.RetryAfter)
}

// attempt performs a single HTTP call.
func (c *Client) attempt(ctx context.Context, v Vertical, target string) (*Envelope, time.Duration, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, 0, false, err
	}
	// The unified credential structure. x-apisports-key is the direct-account
	// header; the RapidAPI-fronted variant uses x-rapidapi-key and is not what
	// this deployment uses.
	req.Header.Set("x-apisports-key", c.key)
	req.Header.Set("Accept", "application/json")

	start := c.now()
	resp, err := c.http.Do(req)
	latency := c.now().Sub(start)
	if err != nil {
		if c.observer != nil {
			c.observer.ObserveRequest(v, 0, latency, err)
		}
		return nil, 0, false, err
	}
	defer resp.Body.Close()

	quota := ParseQuota(resp.Header, c.now())
	if c.observer != nil {
		if quota.Present {
			c.observer.ObserveQuota(v, quota)
		}
		c.observer.ObserveRequest(v, resp.StatusCode, latency, nil)
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		wait, explicit := retryAfterOf(resp.Header)
		return nil, wait, explicit, &ThrottleError{Vertical: v, RetryAfter: wait}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, 0, false, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, false, fmt.Errorf("apisports: %s returned HTTP %d: %s",
			v, resp.StatusCode, truncate(body, 200))
	}

	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, 0, false, fmt.Errorf("apisports: %s returned undecodable JSON: %w", v, err)
	}
	// The 200-with-errors case.
	if fields := decodeErrors(env.Errors); len(fields) > 0 {
		return nil, 0, false, &APIError{Endpoint: env.Get, Fields: fields}
	}
	env.Header = resp.Header
	env.ReceivedAt = c.now()
	return &env, 0, false, nil
}

// backoff is exponential with full jitter, honouring Retry-After when sent.
//
// Jitter matters more than usual here. Every vertical shares one key and one
// minute window; without it, a dozen sport workers throttled by the same burst
// would all wake at the identical instant and re-throttle each other forever.
func (c *Client) backoff(attempt int, retryAfter time.Duration, explicit bool) time.Duration {
	// An explicit Retry-After wins, including a zero one: a server that says
	// "retry now" has told us more than our exponential guess knows.
	if explicit {
		return retryAfter
	}
	const base = 2 * time.Second
	const max = 60 * time.Second
	d := time.Duration(math.Pow(2, float64(attempt))) * base
	if d > max {
		d = max
	}
	// Full jitter: uniform over (0, d]. Keeps the mean sensible while making a
	// synchronised retry storm impossible.
	return time.Duration(float64(d) * (0.5 + 0.5*c.rnd.Float64()))
}

// retryAfterOf reads Retry-After, reporting whether the header was present at
// all. The distinction matters: "Retry-After: 0" is an instruction to retry
// immediately, and collapsing it into "absent" turns a fast recovery into a
// two-second exponential wait.
func retryAfterOf(h http.Header) (time.Duration, bool) {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second, true
	}
	if when, err := http.ParseTime(v); err == nil {
		if d := time.Until(when); d > 0 {
			return d, true
		}
		return 0, true
	}
	return 0, false
}

func truncate(b []byte, n int) string {
	s := strings.Join(strings.Fields(string(b)), " ")
	if len(s) > n {
		s = s[:n] + "…"
	}
	return s
}

// Status calls /status, the cheapest way to confirm a key and read a plan.
type Status struct {
	Account struct {
		FirstName string `json:"firstname"`
		LastName  string `json:"lastname"`
		Email     string `json:"email"`
	} `json:"account"`
	Subscription struct {
		Plan   string    `json:"plan"`
		End    time.Time `json:"end"`
		Active bool      `json:"active"`
	} `json:"subscription"`
	Requests struct {
		Current  int `json:"current"`
		LimitDay int `json:"limit_day"`
	} `json:"requests"`
}

// Status reports the plan and usage the provider has on record for a vertical.
//
// Worth knowing: /status reflects the daily counter lazily and does not always
// include the call being made. The response HEADERS on a real request are the
// authoritative live figure — this is for confirming the plan, not for metering.
func (c *Client) Status(ctx context.Context, v Vertical) (*Status, error) {
	env, err := c.Get(ctx, v, "/status", nil)
	if err != nil {
		return nil, err
	}
	var st Status
	if err := json.Unmarshal(env.Response, &st); err != nil {
		return nil, fmt.Errorf("apisports: decoding status for %s: %w", v, err)
	}
	return &st, nil
}
