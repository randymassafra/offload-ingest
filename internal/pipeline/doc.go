// Package pipeline holds end-to-end tests that span the ingest stack.
//
// Every other package tests its own seam: pkg/ingest/apisports proves the
// client decodes an envelope, pkg/ingest/scope proves the validator refuses an
// unlicensed league, pkg/metrics proves a counter counts. What none of them can
// prove is that those pieces survive each other when the provider misbehaves,
// because each one stubs the layer below it and a stub is, by construction,
// well behaved.
//
// This package assembles the real thing — a real HTTP client against a real
// (if hostile) server, a real limiter, a real production streamer, a real scope
// validator, a real publisher — and then breaks the provider on purpose. It
// exists because the failures worth catching in an appliance shipped to a venue
// are not "does this function return the right value" but "does the process
// still be running in an hour, and is it publishing again once the provider
// recovers".
//
// It carries no non-test code. The package is a home for tests that cannot
// honestly live inside any single component.
package pipeline
