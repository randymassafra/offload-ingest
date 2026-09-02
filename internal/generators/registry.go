package generators

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Feed simulates one fixture and renders the payload a single provider
// endpoint would return. Implementations are NOT safe for concurrent use: give
// each poller worker and each burst emitter its own feed.
type Feed interface {
	// Endpoint describes the provider endpoint this feed imitates.
	Endpoint() Endpoint
	// FixtureID is the provider's identifier for the event being simulated. It
	// is the Kafka partition key and is stable until Reset.
	FixtureID() string
	// Next advances the simulation and renders the payload.
	Next() Message
	// Done reports whether the fixture has reached a terminal state.
	Done() bool
	// Reset abandons the current fixture and starts a fresh one.
	Reset()
}

// sim is the per-sport simulation state behind one or more feeds.
type sim interface {
	// advance moves the fixture forward by one tick.
	advance()
	// done reports whether the fixture is over.
	done() bool
	// reset starts a new fixture.
	reset()
	// fixtureID returns the provider identifier for the current fixture.
	fixtureID() string
}

// renderer builds the payload for one endpoint from a sim's current state.
//
// It returns the wire model to marshal; the model name comes from the Endpoint
// unless the renderer overrides it (soccer's timeline emits three record types).
//
// ok reports whether this tick produced something new. An event-driven feed —
// soccer's incident timeline, for example — has nothing to say on most ticks,
// and returning ok=false makes the feed advance the simulation again rather
// than republishing the previous record. Without this a push feed emits
// duplicates: the same goal, over and over, with a fresh Kafka sequence number
// each time, which a downstream consumer cannot distinguish from a real replay.
type renderer func() (payload any, modelOverride string, ok bool)

// factory builds a sim and the renderer for one endpoint.
type factory func(rnd *rand.Rand) (sim, renderer)

type registration struct {
	ep Endpoint
	// newFeed builds a Feed for this endpoint from a seed. Simulated endpoints
	// wrap their sim and renderer; captured endpoints return a replayer.
	newFeed func(seed int64) Feed
}

// The registry is a slice per sport rather than a map keyed by kind, because a
// sport can expose several endpoints of the same kind — a season schedule and a
// season schedule and a driver directory, and both are reference documents.
var (
	registryMu sync.RWMutex
	registry   = map[Sport][]registration{}
)

// lookup finds a sport's registration by kind, optionally narrowed by name.
// With no name it returns the first endpoint of that kind, which is the
// sport's default for the kind.
func lookup(sport Sport, kind FeedKind, name string) (registration, bool) {
	for _, reg := range registry[sport] {
		if reg.ep.Kind != kind {
			continue
		}
		if name == "" || reg.ep.Name == name {
			return reg, true
		}
	}
	return registration{}, false
}

// register wires one (sport, kind) endpoint. Called from each sport's init.
func register(ep Endpoint, f factory) {
	// Default the provider so only the odd ones out have to declare it.
	if ep.Provider == "" {
		if ep.Provenance == ProvenanceModeled {
			ep.Provider = ProviderNone
		} else {
			ep.Provider = ProviderLiveGolf
		}
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	for _, existing := range registry[ep.Sport] {
		if existing.ep.Kind == ep.Kind && existing.ep.Name == ep.Name {
			panic(fmt.Sprintf("generators: duplicate registration for %s/%s", ep.Sport, ep.Ref()))
		}
	}
	registry[ep.Sport] = append(registry[ep.Sport], registration{
		ep: ep,
		newFeed: func(seed int64) Feed {
			s, r := f(rand.New(rand.NewSource(seed)))
			return &feed{ep: ep, sim: s, render: r}
		},
	})
}

// feed is the generic Feed built over a sim and a renderer.
type feed struct {
	ep     Endpoint
	sim    sim
	render renderer
	seq    int64
}

func (f *feed) Endpoint() Endpoint { return f.ep }
func (f *feed) FixtureID() string  { return f.sim.fixtureID() }
func (f *feed) Done() bool         { return f.sim.done() }

func (f *feed) Reset() {
	f.sim.reset()
	f.seq = 0
}

// maxTicksPerMessage bounds the search for a renderable tick on an
// event-driven feed. A soccer match produces an incident every few simulated
// minutes; this ceiling is far above that and exists only so a renderer that
// never returns ok cannot hang the poller.
const maxTicksPerMessage = 512

func (f *feed) Next() Message {
	var (
		payload  any
		override string
		ok       bool
	)
	for tick := 0; tick < maxTicksPerMessage; tick++ {
		// A finished fixture rolls straight into the next one so a
		// long-running load test keeps producing rather than going quiet.
		if f.sim.done() {
			f.Reset()
		}
		f.sim.advance()
		if payload, override, ok = f.render(); ok {
			break
		}
	}
	f.seq++

	model := f.ep.Model
	if override != "" {
		model = override
	}
	return Message{
		Sport:      f.ep.Sport,
		Kind:       f.ep.Kind,
		Endpoint:   f.ep.Path,
		Projection: f.ep.Projection,
		Model:      model,
		FixtureID:  f.sim.fixtureID(),
		Sequence:   f.seq,
		Emitted:    now().UTC(),
		Payload:    payload,
	}
}

// New builds the feed for one (sport, kind) with a deterministic seed. Two
// calls with the same arguments produce byte-identical streams.
func New(sport Sport, kind FeedKind, seed int64) (Feed, error) {
	registryMu.RLock()
	reg, ok := lookup(sport, kind, "")
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("generators: %s has no %s feed", sport, kind)
	}
	return reg.newFeed(seed), nil
}

// NewNamed builds a specific endpoint when a sport exposes more than one of a
// kind, such as a season schedule alongside a driver directory.
func NewNamed(sport Sport, kind FeedKind, name string, seed int64) (Feed, error) {
	registryMu.RLock()
	reg, ok := lookup(sport, kind, name)
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("generators: %s has no %s/%s feed", sport, kind, name)
	}
	return reg.newFeed(seed), nil
}

// NewAll builds one feed per (sport, kind) combination that exists, each with
// its own derived seed. Empty sports or kinds mean "everything".
//
// Combinations that do not exist are skipped rather than erroring: asking for
// play-by-play across all sports should give you the eight that have it, not a
// failure because golf does not.
func NewAll(sports []Sport, kinds []FeedKind, seed int64) ([]Feed, error) {
	if len(sports) == 0 {
		sports = AllSports
	}
	if len(kinds) == 0 {
		kinds = AllKinds
	}
	want := make(map[FeedKind]bool, len(kinds))
	for _, k := range kinds {
		if !k.Valid() {
			return nil, fmt.Errorf("generators: unknown feed kind %q", k)
		}
		want[k] = true
	}

	var out []Feed
	i := 0
	for _, sp := range sports {
		if !sp.Valid() {
			return nil, fmt.Errorf("generators: unknown sport %q", sp)
		}
		// Iterate the registrations rather than the kinds, so a sport that
		// exposes several endpoints of one kind — a schedule and a driver
		// directory are both reference documents — contributes all of them.
		for _, ep := range EndpointsFor(sp) {
			if !want[ep.Kind] {
				continue
			}
			f, err := NewNamed(ep.Sport, ep.Kind, ep.Name, seed+int64(i)*104729)
			if err != nil {
				continue
			}
			out = append(out, f)
			i++
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("generators: no feeds match the requested sports and kinds")
	}
	return out, nil
}

// Endpoints lists every registered endpoint, sorted.
func Endpoints() []Endpoint {
	registryMu.RLock()
	defer registryMu.RUnlock()
	var out []Endpoint
	for _, regs := range registry {
		for _, reg := range regs {
			out = append(out, reg.ep)
		}
	}
	sortEndpoints(out)
	return out
}

// EndpointsFor lists the endpoints registered for one sport.
func EndpointsFor(sport Sport) []Endpoint {
	registryMu.RLock()
	defer registryMu.RUnlock()
	var out []Endpoint
	for _, reg := range registry[sport] {
		out = append(out, reg.ep)
	}
	sortEndpoints(out)
	return out
}

// KindsFor lists the feed kinds a sport offers.
func KindsFor(sport Sport) []FeedKind {
	var out []FeedKind
	for _, ep := range EndpointsFor(sport) {
		out = append(out, ep.Kind)
	}
	return out
}

// now is swapped out in tests to make timestamps deterministic.
var now = time.Now
