package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/generators"
)

// ReceiverConfig configures the inbound webhook endpoint.
type ReceiverConfig struct {
	Addr string // listen address, e.g. ":8088"
	Path string // webhook path, defaults to /webhook
	// MaxBodyBytes caps a single request body. Defaults to 8 MiB.
	MaxBodyBytes int64
	// Secret, when set, is compared against the X-Offload-Signature header.
	Secret string
	// Sport and Kind label inbound payloads, since a raw provider push carries
	// no routing metadata of its own.
	Sport generators.Sport
	Kind  generators.FeedKind
	// PublishTimeout bounds the downstream publish for one request.
	PublishTimeout time.Duration
}

// Receiver accepts pushed events over HTTP and forwards them to the sink. It is
// the mirror of Emitter: use it when a provider (or an upstream load generator)
// pushes into this service rather than the other way round.
type Receiver struct {
	cfg  ReceiverConfig
	sink Publisher
	srv  *http.Server

	requests atomic.Int64
	events   atomic.Int64
	rejected atomic.Int64
	errs     atomic.Int64
}

// NewReceiver builds a receiver bound to cfg.Addr. Call Run to serve.
func NewReceiver(cfg ReceiverConfig, sink Publisher) (*Receiver, error) {
	if cfg.Addr == "" {
		return nil, errors.New("webhook: receiver address is required")
	}
	if cfg.Path == "" {
		cfg.Path = "/webhook"
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 8 << 20
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 10 * time.Second
	}
	if cfg.Kind == "" {
		cfg.Kind = generators.FeedTelemetry
	}

	r := &Receiver{cfg: cfg, sink: sink}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+cfg.Path, r.handle)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.Stats())
	})

	r.srv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return r, nil
}

// Handler exposes the mux for embedding in an existing server.
func (r *Receiver) Handler() http.Handler { return r.srv.Handler }

// Run serves until ctx is cancelled, then shuts down gracefully.
func (r *Receiver) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		err := r.srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return r.srv.Shutdown(shutdownCtx)
	}
}

// handle decodes a batch (or a single event) and forwards it to the sink.
func (r *Receiver) handle(w http.ResponseWriter, req *http.Request) {
	r.requests.Add(1)

	if r.cfg.Secret != "" && req.Header.Get("X-Offload-Signature") != r.cfg.Secret {
		r.rejected.Add(1)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	body := http.MaxBytesReader(w, req.Body, r.cfg.MaxBodyBytes)
	defer body.Close()

	raw, err := io.ReadAll(body)
	if err != nil {
		r.rejected.Add(1)
		http.Error(w, "read body: "+err.Error(), http.StatusRequestEntityTooLarge)
		return
	}

	msgs, err := r.decode(raw)
	if err != nil {
		r.rejected.Add(1)
		http.Error(w, "decode: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), r.cfg.PublishTimeout)
	defer cancel()

	if err := r.sink.Publish(ctx, msgs...); err != nil {
		r.errs.Add(1)
		http.Error(w, "publish: "+err.Error(), http.StatusBadGateway)
		return
	}
	r.events.Add(int64(len(msgs)))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]int{"accepted": len(msgs)})
}

// decode accepts either a JSON array of documents or a single document, since
// providers differ on which they push. The body is kept opaque: whatever the
// provider sent is by definition the authentic shape, so it is forwarded
// unmodified rather than being coerced into one of our models.
func (r *Receiver) decode(raw []byte) ([]generators.Message, error) {
	seq := r.requests.Load()

	var batch []json.RawMessage
	if err := json.Unmarshal(raw, &batch); err == nil {
		out := make([]generators.Message, 0, len(batch))
		for k, item := range batch {
			m, err := r.message(item, seq+int64(k))
			if err != nil {
				return nil, err
			}
			out = append(out, m)
		}
		return out, nil
	}

	m, err := r.message(raw, seq)
	if err != nil {
		return nil, err
	}
	return []generators.Message{m}, nil
}

func (r *Receiver) message(raw []byte, seq int64) (generators.Message, error) {
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return generators.Message{}, err
	}
	return generators.Message{
		Sport:     r.cfg.Sport,
		Kind:      r.cfg.Kind,
		Endpoint:  r.cfg.Path,
		Model:     "inbound",
		FixtureID: fixtureFrom(payload),
		Sequence:  seq,
		Emitted:   time.Now().UTC(),
		Payload:   payload,
	}, nil
}

// fixtureFrom pulls a partition key out of an inbound document by looking for
// the identifier fields SportsDataIO uses. Casing varies per API — NFL uses
// GameID, Soccer v4 uses GameId — so both spellings are checked.
func fixtureFrom(payload any) string {
	obj, ok := payload.(map[string]any)
	if !ok {
		return "unknown"
	}
	// A box score nests the identifiers one level down.
	for _, wrapper := range []string{"Game", "Score", "Match", "Event"} {
		if inner, ok := obj[wrapper].(map[string]any); ok {
			if id := scanIDs(inner); id != "" {
				return id
			}
		}
	}
	if id := scanIDs(obj); id != "" {
		return id
	}
	return "unknown"
}

func scanIDs(obj map[string]any) string {
	for _, key := range []string{
		"GameID", "GameId", "ScoreID", "MatchId", "MatchID",
		"RaceID", "TournamentID", "EventId", "FightId", "GlobalGameID", "GlobalGameId",
	} {
		switch v := obj[key].(type) {
		case float64:
			return strconv.FormatInt(int64(v), 10)
		case string:
			if v != "" {
				return v
			}
		}
	}
	return ""
}

// ReceiverStats is a snapshot of inbound traffic.
type ReceiverStats struct {
	Requests int64 `json:"requests"`
	Events   int64 `json:"events"`
	Rejected int64 `json:"rejected"`
	Errors   int64 `json:"errors"`
}

func (r *Receiver) Stats() ReceiverStats {
	return ReceiverStats{
		Requests: r.requests.Load(),
		Events:   r.events.Load(),
		Rejected: r.rejected.Load(),
		Errors:   r.errs.Load(),
	}
}
