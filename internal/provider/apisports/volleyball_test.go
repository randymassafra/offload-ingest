package apisports

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	ingestapi "github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
)

// captured is the live volleyball capture, so these tests run against the
// document the provider actually returns rather than one written to pass.
func captured(t *testing.T) string {
	t.Helper()
	for _, p := range []string{
		"../../../fixtures/apisports/volleyball.json",
		"fixtures/apisports/volleyball.json",
	} {
		if b, err := os.ReadFile(filepath.Clean(p)); err == nil {
			return string(b)
		}
	}
	t.Skip("no volleyball capture on disk; run `make capture`")
	return ""
}

func serve(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-apisports-key"); got != "test-key" {
			t.Errorf("key header = %q", got)
		}
		// Volleyball is swept by date; live=all does not exist on this host.
		if r.URL.Query().Get("live") != "" {
			t.Errorf("live=all was sent to a vertical that rejects it: %s", r.URL.RawQuery)
		}
		if r.URL.Query().Get("date") == "" {
			t.Errorf("no date parameter: %s", r.URL.RawQuery)
		}
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)

	api, err := ingestapi.New(ingestapi.Config{APIKey: "test-key", BaseURLOverride: srv.URL})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return New("test-key").Configure(WithClient(api))
}

// TestDecodesTheLiveCapture is the contract that matters: the models must bind
// to the real document, not to an idealised one.
func TestDecodesTheLiveCapture(t *testing.T) {
	body := captured(t)
	var env struct {
		Results int `json:"results"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatalf("capture is not readable: %v", err)
	}

	games, err := serve(t, body).Games(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("Games: %v", err)
	}
	if len(games) != env.Results {
		t.Fatalf("decoded %d games, the capture has %d", len(games), env.Results)
	}
	g := games[0]
	if g.ID == 0 {
		t.Error("game id did not decode")
	}
	if g.Teams.Home.Name == "" || g.Teams.Away.Name == "" {
		t.Errorf("teams did not decode: %+v", g.Teams)
	}
	if g.League.Name == "" {
		t.Error("league did not decode")
	}
	if g.Status.Short == "" {
		t.Error("status did not decode")
	}
	if g.Timestamp == 0 || g.Kickoff().IsZero() {
		t.Error("timestamp did not decode")
	}
}

// TestScoresAreSetCounts. Volleyball reports plain set totals where basketball
// reports per-quarter columns and baseball a per-inning map — the families
// differ per vertical and are reproduced rather than normalised into one shape.
func TestScoresAreSetCounts(t *testing.T) {
	games, err := serve(t, captured(t)).Games(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("Games: %v", err)
	}
	var scored bool
	for _, g := range games {
		if g.Scores.Home+g.Scores.Away > 0 {
			scored = true
			if g.Scores.Home > 5 || g.Scores.Away > 5 {
				t.Errorf("%s scored %d-%d; sets should not exceed 5",
					g.Fixture(), g.Scores.Home, g.Scores.Away)
			}
		}
	}
	if !scored {
		t.Log("note: the capture contains no played matches, so scores went unchecked")
	}
}

// TestInPlayUsesSetStatuses. Volleyball's live codes are S1..S5, not the halves
// and quarters the other verticals use.
func TestInPlayUsesSetStatuses(t *testing.T) {
	for _, tc := range []struct {
		short string
		live  bool
		done  bool
	}{
		{"S1", true, false}, {"S3", true, false}, {"S5", true, false},
		{"LIVE", true, false}, {"FT", false, true}, {"NS", false, false},
		{"POST", false, true}, {"", false, false},
		// An unrecognised status must not read as live: that is the safe
		// direction on a metered request budget.
		{"WHAT", false, false},
	} {
		g := Game{Status: Status{Short: tc.short}}
		if g.InPlay() != tc.live {
			t.Errorf("InPlay(%q) = %v, want %v", tc.short, g.InPlay(), tc.live)
		}
		if g.Finished() != tc.done {
			t.Errorf("Finished(%q) = %v, want %v", tc.short, g.Finished(), tc.done)
		}
	}
}

func TestLiveFiltersTheCard(t *testing.T) {
	body := `{"get":"games","errors":[],"results":3,"response":[
	  {"id":1,"status":{"short":"S2"},"teams":{"home":{"name":"A"},"away":{"name":"B"}}},
	  {"id":2,"status":{"short":"FT"},"teams":{"home":{"name":"C"},"away":{"name":"D"}}},
	  {"id":3,"status":{"short":"NS"},"teams":{"home":{"name":"E"},"away":{"name":"F"}}}]}`
	live, err := serve(t, body).Live(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("Live: %v", err)
	}
	if len(live) != 1 || live[0].ID != 1 {
		t.Errorf("got %d live games, want just the one in set 2", len(live))
	}
}

// TestErrorsInsideA200AreSurfaced inherits the underlying client's handling of
// the trap where a rejected request returns HTTP 200 with the reason in the
// body. This is the behaviour a duplicate client would have had to reimplement.
func TestErrorsInsideA200AreSurfaced(t *testing.T) {
	body := `{"get":"games","errors":{"date":"The Date field is invalid."},"results":0,"response":[]}`
	_, err := serve(t, body).Games(context.Background(), time.Now())
	if err == nil {
		t.Fatal("a 200 carrying an errors block should be an error")
	}
}

func TestEmptyCardIsNotAnError(t *testing.T) {
	body := `{"get":"games","errors":[],"results":0,"response":[]}`
	games, err := serve(t, body).Games(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("an out-of-season card is valid: %v", err)
	}
	if len(games) != 0 {
		t.Errorf("got %d games, want none", len(games))
	}
}

// TestMissingCredentialIsReported. The constructor cannot return an error, so
// the failure has to surface on first use rather than as a panic.
func TestMissingCredentialIsReported(t *testing.T) {
	if _, err := New("").Games(context.Background(), time.Now()); err == nil {
		t.Error("want an error when the client has no credential")
	}
}

// TestUsesTheSharedVertical guards against this package drifting into a second
// definition of where volleyball lives.
func TestUsesTheSharedVertical(t *testing.T) {
	if Vertical != ingestapi.VerticalVolleyball {
		t.Errorf("vertical = %s, want the shared constant", Vertical)
	}
	spec, ok := ingestapi.SpecFor(Vertical)
	if !ok {
		t.Fatal("no spec for volleyball in the shared catalog")
	}
	if spec.Host != "v1.volleyball.api-sports.io" {
		t.Errorf("host = %s", spec.Host)
	}
	if spec.Mode != ingestapi.BulkDate {
		t.Error("volleyball must be swept by date; live=all does not exist on this host")
	}
}
