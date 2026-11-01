package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"yyb_go/internal/protocol"
	"yyb_go/internal/store"
)

func testBreaker(t *testing.T) (*circuitBreaker, *time.Time) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0)
	b := newCircuitBreaker(defaultCircuitConfig())
	b.now = func() time.Time { return now }
	return b, &now
}

func TestTargetBreakerHoldsOffThenRecovers(t *testing.T) {
	b, now := testBreaker(t)
	// A single rejection is not enough (default threshold is 2).
	if b.record(1, "wxA", circuitAppReject) != "" || b.check(1, "wxA") != nil {
		t.Fatal("target opened too early")
	}
	if scope := b.record(1, "wxA", circuitAppReject); scope != "target" {
		t.Fatalf("second -14 did not open the target: %q", scope)
	}
	var open *circuitOpenError
	if err := b.check(1, "wxA"); !errors.As(err, &open) || open.Scope != "target" {
		t.Fatalf("held-off target not reported: %v", err)
	}
	// A different target on the same account is unaffected.
	if err := b.check(1, "wxB"); err != nil {
		t.Fatalf("unrelated target held off: %v", err)
	}
	// After the cooldown the target is allowed again.
	*now = now.Add(defaultCircuitConfig().TargetCooldown + time.Second)
	if err := b.check(1, "wxA"); err != nil {
		t.Fatalf("target not released after cooldown: %v", err)
	}
	// A success clears accumulated state.
	b.record(1, "wxA", circuitAppReject)
	b.record(1, "wxA", circuitOK)
	if b.record(1, "wxA", circuitAppReject) != "" {
		t.Fatal("success did not reset the target failure count")
	}
}

func TestSessionBreakerOpensOnDistinctRejectedTargets(t *testing.T) {
	// Repeated -14 on one AppID must NOT open the session (that is the target
	// layer); only distinct AppIDs count toward a wholesale lockout.
	single, _ := testBreaker(t)
	for range 6 {
		single.record(2, "wxSame", circuitAppReject)
	}
	if err := single.check(2, "wxOther"); err != nil {
		t.Fatalf("session opened from a single AppID: %v", err)
	}

	// Three distinct rejected AppIDs open the session on the third.
	b, _ := testBreaker(t)
	if b.record(7, "wx1", circuitAppReject) == "session" || b.record(7, "wx2", circuitAppReject) == "session" {
		t.Fatal("session opened before the distinct-target threshold")
	}
	if scope := b.record(7, "wx3", circuitAppReject); scope != "session" {
		t.Fatalf("third distinct -14 did not open the session: %q", scope)
	}
	var open *circuitOpenError
	if err := b.check(7, "wxBrandNew"); !errors.As(err, &open) || open.Scope != "session" {
		t.Fatalf("session hold-off does not cover every target: %v", err)
	}

	// Empty codes never open the session.
	empty, _ := testBreaker(t)
	for _, id := range []string{"a", "b", "c", "d"} {
		empty.record(9, id, circuitEmptyCode)
	}
	if err := empty.check(9, "e"); err != nil {
		t.Fatalf("empty codes opened the session: %v", err)
	}
}

func TestSessionWindowExpiresDistinctTargets(t *testing.T) {
	b, now := testBreaker(t)
	b.record(3, "wx1", circuitAppReject)
	*now = now.Add(defaultCircuitConfig().SessionWindow + time.Second)
	b.record(3, "wx2", circuitAppReject)
	b.record(3, "wx3", circuitAppReject)
	// wx1 has aged out of the window, so only two distinct targets remain.
	if err := b.check(3, "wx9"); err != nil {
		t.Fatalf("stale target still counted toward the session: %v", err)
	}
}

func TestBreakerForgetAndDisabled(t *testing.T) {
	b, _ := testBreaker(t)
	b.record(4, "wxA", circuitAppReject)
	b.record(4, "wxA", circuitAppReject)
	b.forget(4)
	if err := b.check(4, "wxA"); err != nil {
		t.Fatalf("forget did not clear the target: %v", err)
	}
	off := newCircuitBreaker(circuitConfigFor(true))
	for range 10 {
		off.record(5, "wxA", circuitAppReject)
		off.record(5, "wxB", circuitAppReject)
		off.record(5, "wxC", circuitAppReject)
	}
	if err := off.check(5, "wxA"); err != nil {
		t.Fatalf("disabled breaker held off a request: %v", err)
	}
}

// A held-off request must be a local 429 that never reaches the pool and never
// changes account status or credentials.
func TestInvokeWXAppShortCircuitsWithoutUpstream(t *testing.T) {
	app := newKeepAliveTestApp(t)
	defer app.Close()
	acc := insertKeepAliveTestAccount(t, app, "openid-circuit", time.Now().Add(time.Hour))
	var calls atomic.Int32
	reject := func(context.Context, *store.WechatAccount, string, map[string]any, string, bool) (map[string]any, error) {
		calls.Add(1)
		return nil, &protocol.ShortlinkRejectedError{Code: -14}
	}
	ctx := context.Background()
	// Two -14 open the target; the third call must be short-circuited locally.
	for range 2 {
		if _, err := app.invokeWXApp(ctx, acc, "wxTarget", nil, reject); !protocol.IsApplicationRequestRejection(err) {
			t.Fatalf("expected -14 passthrough, got %v", err)
		}
	}
	before := calls.Load()
	_, err := app.invokeWXApp(ctx, acc, "wxTarget", nil, reject)
	var open *circuitOpenError
	if !errors.As(err, &open) || open.Scope != "target" {
		t.Fatalf("third call was not short-circuited: %v", err)
	}
	if calls.Load() != before {
		t.Fatal("short-circuited request still reached the pool")
	}
	stored, getErr := app.db.GetAccount(ctx, acc.ID)
	if getErr != nil || accountStatus(stored) != "alive" || stored.Credentials == nil {
		t.Fatal("circuit breaker changed account status or credentials")
	}

	rec := httptest.NewRecorder()
	if !writeIfCircuitOpen(rec, err) || rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("circuit error did not map to 429 with Retry-After: %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"circuit_open":"target"`) {
		t.Fatalf("429 body missing circuit marker: %s", rec.Body.String())
	}

	// A fresh scan clears the breaker.
	if _, err := app.storeFromScan(ctx, acc.LoginBuffer, protocol.CredentialsFromMap(acc.Credentials), nil); err != nil {
		t.Fatal(err)
	}
	if err := app.circuit.check(acc.ID, "wxTarget"); err != nil {
		t.Fatalf("re-scan did not clear the breaker: %v", err)
	}
}

// The production signature: one account whose whole session is refused on many
// AppIDs must stop reaching the upstream instead of being hammered every run.
func TestWholesaleRejectionStopsUpstreamHammering(t *testing.T) {
	app := newKeepAliveTestApp(t)
	defer app.Close()
	acc := insertKeepAliveTestAccount(t, app, "openid-wholesale", time.Now().Add(time.Hour))
	var calls atomic.Int32
	reject := func(context.Context, *store.WechatAccount, string, map[string]any, string, bool) (map[string]any, error) {
		calls.Add(1)
		return nil, &protocol.ShortlinkRejectedError{Code: -14}
	}
	ctx := context.Background()
	targets := []string{"wx1", "wx2", "wx3", "wx4", "wx5", "wx6"}
	for round := range 20 {
		for _, target := range targets {
			_, _ = app.invokeWXApp(ctx, acc, target, nil, reject)
		}
		_ = round
	}
	// Without the breaker this would be 120 upstream calls. The session opens
	// after a few distinct -14 and holds the rest off locally.
	if n := calls.Load(); n >= 40 {
		t.Fatalf("breaker did not curb upstream calls: %d of 120 reached upstream", n)
	}
}
