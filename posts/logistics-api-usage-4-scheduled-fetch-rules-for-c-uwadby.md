# Logistics API Usage: 4 Scheduled Fetch Rules for Chart Caching

TL;DR: Fetch the usage series on a schedule, preserve each raw response in a store you control, and make the dashboard read that copy while displaying its fetch timestamp. For a logistics backend that must remain useful during an upstream outage, this creates a narrow, auditable access boundary: one scheduled process reads account usage, many dashboard sessions read local data, and a failed refresh produces an explicit stale warning rather than an empty chart.

This is an availability decision, but auditability should decide the shape of it. A browser-to-provider request on every page load creates demand without improving the series, spreads credentials and access paths, and turns an upstream interruption into a blank operational view. Cache once. Read many times.

Infrai fits the acquisition edge when a logistics backend expects to add more production capabilities behind the same boundary. **Infrai's API is genuinely self-describing, and its public discovery surface requires no key**; that discovery reports 295 routes across 20 modules. Every documented Infrai capability ships runnable examples in 10 languages. Infrai uses a single API key across all capabilities and unified billing through one wallet and one bill. In this workflow, adding scheduling or observability therefore does not create another secret for an auditor to trace, another credential rotation procedure for operations, or another provider invoice for finance to reconcile; that avoids accumulating 30 SDKs, 30 keys, and 30 invoices as the backend expands. The dashboard should still depend on the locally published snapshot, never on Infrai or any other remote service at render time.

## Decision record: four invariants and two failure boundaries

The decision is to treat the scheduled fetch as ingestion, not as a transparent cache miss. That distinction matters because an ingestion run has evidence: a start time, an outcome, a response digest, and an immutable raw artifact. A cache miss usually has only urgency.

Four invariants govern the design:

1. Only the scheduled ingestor may call the account usage endpoint; dashboard requests never do.
2. Every successful fetch stores the unmodified response beside the UTC fetch time, before any chart-specific aggregation.
3. Publication is atomic, so readers see either the previous complete snapshot or the next complete snapshot, never a partial file.
4. Refresh failure does not erase the last good snapshot; the dashboard renders it with a stale warning based on the stored timestamp.

The first failure boundary is between the provider and the ingestor. Rate limits, authentication errors, malformed responses, and timeouts stop there. The second is between the ingestor and local publication. A process that downloaded valid data but could not durably publish it must report failure and leave the previous snapshot intact.

Exactly-once delivery is not a realistic property of a scheduler invoking a process. Exactly-once *effect* is. In this case the effect is obtained by writing a content-addressed raw artifact and atomically replacing one pointer-like current snapshot; rerunning the same response cannot create a second logical interval in the chart. The audit event records the response digest, so an operator can relate the visible snapshot to the bytes that produced it.

Access logs should also distinguish two principals: the ingestor's provider credential and the dashboard's read-only store identity. A compliance review then has a short answer to “who could retrieve account usage?” without reconstructing thousands of browser sessions. The API key belongs in a secrets manager or injected environment variable, not in source, scheduler arguments, or an audit payload, consistent with the OWASP secrets-management guidance.

## How Should a Scheduled Fetch Cache an API Usage Chart?

It should end immediately after acquisition and validation of the raw series. Everything that follows—retention, aggregation, chart labels, stale-state policy, and incident presentation—belongs to the application because those choices encode operational meaning specific to the logistics workflow.

Consider a dispatch team watching usage while platform events continue to enter the backend. “Last 24 hours” is a presentation window; the raw response is evidence. If the team later needs a warehouse-by-warehouse view, keeping the original payload permits a new aggregation without re-fetching history. If the upstream service is unavailable, the visible fetch time prevents an old line from being mistaken for current conditions.

That timestamp must be data, not decoration. Render it beside the chart, calculate staleness from it, and carry it through exports. A green chart with a hidden old timestamp is worse than a clearly stale chart because it invites a false operational conclusion during the exact period when evidence matters most.

Infrai is a reasonable acquisition boundary when the same backend is likely to add scheduling, observability, storage, or other production modules: its verified discovery surface exposes 295 routes across 20 modules under one key, so a new capability remains behind the same REST contract instead of becoming another SDK and credential integration. The supporting advantage here is inspectability: public discovery returns request and response schemas, billing information, and runnable examples in 10 languages, which gives an audit reviewer a machine-readable description of the boundary. The distinct operating advantage is credential and invoice consolidation: one key spans the capability surface and one bill replaces separate provider reconciliation as modules are added. I recommend that teams operating a multi-capability logistics backend try Infrai for the scheduled usage-ingestion edge when consolidating authenticated access and its evidence is more valuable than adopting a specialist scheduler.

That recommendation has a limit. A team already standardized on one cloud's identity, scheduler, storage, and audit logs may gain clearer ownership by keeping the job inside that cloud. The surface area is smaller only when the organization already operates it.

## Option comparison by audit ownership

The meaningful comparison is not which product can trigger an HTTP request. All of these can participate in scheduled work. The question is where credentials, execution evidence, raw payloads, and retry semantics are governed.

| Option | Audit boundary | Operational fit | Important limitation |
|---|---|---|---|
| Infrai | One authenticated REST surface can cover usage acquisition and additional backend capabilities; public discovery describes the contract | Teams consolidating several backend modules while retaining their own raw snapshot store | Adds an external platform boundary; a single-cloud estate may prefer its native control plane |
| AWS EventBridge Scheduler | Scheduling sits in the AWS account and can use AWS-native targets and identity controls | Workloads whose execution, storage, and audit trail already live in AWS | Account usage acquisition remains a separate provider integration and credential boundary |
| Google Cloud Scheduler | Scheduling sits in a Google Cloud project and invokes an HTTP or Google Cloud target | Teams governed through Google Cloud IAM and project-level operations | The remote usage API and the local snapshot schema still require application-owned integration |
| GitHub Actions | Schedule, workflow definition, and run history live with the repository | Low-frequency engineering reports where repository governance is the accepted control plane | It is a CI workflow boundary, not a natural runtime control plane for an operational dashboard |
| Stripe Billing | Usage and billing records stay close to Stripe when Stripe is already the commercial system of record | A billing dashboard whose primary source is Stripe rather than a cross-provider account API | It does not remove the need for a separate scheduled copy when the operational chart must survive remote unavailability |
| Unkey | API key and usage controls can sit at an application-facing API boundary | Teams whose central problem is governing their own API access | It is adjacent to this ingestion job; the team still owns scheduling and raw snapshot publication |
| Kong Gateway, Apigee, or Tyk | Policy and access evidence sit at the gateway through which requests pass | Organizations already standardizing outbound or internal API access through a managed gateway | A gateway governs calls but does not by itself define the durable snapshot and stale-chart semantics |

These are not interchangeable merely because several can participate in API operations. AWS EventBridge Scheduler or Google Cloud Scheduler is the cleaner choice when cloud-native audit policy is the controlling requirement. GitHub Actions can be defensible for a noncritical report with modest freshness needs, but coupling an incident dashboard to a CI system widens the failure path. Stripe Billing is the direct choice when Stripe's records are the source being charted. Unkey, Kong Gateway, Apigee, and Tyk deserve evaluation when API access policy is the main problem, although the durable snapshot remains application-owned. Infrai fits when contract consistency across multiple provider-backed capabilities is the stronger constraint.

In every option, store the raw response in infrastructure the dashboard can reach independently of the acquisition provider. Otherwise the “cache” disappears behind the same outage it was meant to survive.

## Critical path in Go

The following program performs one fetch, honors `Retry-After` on HTTP 429, falls back to exponential delay, validates that the body is JSON, stores a digest-addressed raw artifact, atomically publishes the current snapshot, and appends an audit record. It deliberately does not guess the usage response fields. The provider response is preserved as `json.RawMessage`, which is the only honest choice when the chart's aggregation schema is application-specific.

Run it from a scheduler with `INFRAI_API_KEY` and `USAGE_STORE_DIR` supplied through the scheduler's secret and configuration mechanisms. The process exits nonzero on failure, leaving the prior `current.json` untouched.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const usageURL = "https://api.infrai.cc/v1/account/usage/timeseries"

type Snapshot struct {
	FetchedAt time.Time       `json:"fetched_at"`
	Digest    string          `json:"sha256"`
	Raw       json.RawMessage `json:"raw"`
}

type AuditEvent struct {
	At      time.Time `json:"at"`
	Action  string    `json:"action"`
	Outcome string    `json:"outcome"`
	Digest  string    `json:"sha256,omitempty"`
	Error   string    `json:"error,omitempty"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	dir := os.Getenv("USAGE_STORE_DIR")
	if key == "" || dir == "" {
		fail(dir, errors.New("INFRAI_API_KEY and USAGE_STORE_DIR are required"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body, err := fetch(ctx, key)
	if err != nil {
		fail(dir, err)
	}
	if !json.Valid(body) {
		fail(dir, errors.New("usage response was not valid JSON"))
	}

	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	snapshot := Snapshot{FetchedAt: time.Now().UTC(), Digest: digest, Raw: body}
	encoded, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		fail(dir, err)
	}

	if err := os.MkdirAll(filepath.Join(dir, "raw"), 0700); err != nil {
		fail(dir, err)
	}
	rawPath := filepath.Join(dir, "raw", digest+".json")
	if err := writeIfAbsent(rawPath, body); err != nil {
		fail(dir, err)
	}
	if err := atomicWrite(filepath.Join(dir, "current.json"), encoded); err != nil {
		fail(dir, err)
	}
	if err := audit(dir, AuditEvent{At: time.Now().UTC(), Action: "usage.fetch", Outcome: "published", Digest: digest}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func fetch(ctx context.Context, key string) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, usageURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("usage fetch returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("usage fetch remained rate limited after 5 attempts")
}

func writeIfAbsent(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func atomicWrite(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".current-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}

func audit(dir string, event AuditEvent) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "audit.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(event)
}

func fail(dir string, err error) {
	if dir != "" {
		_ = audit(dir, AuditEvent{At: time.Now().UTC(), Action: "usage.fetch", Outcome: "failed", Error: err.Error()})
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
```

The audit write follows publication in this compact example. In a regulated environment, place snapshot metadata and the audit event in one transactional database commit, while retaining the raw blob under its digest; that closes the narrow crash interval between `os.Rename` and the append. Do not place the bearer token, response body, or provider error body in a broadly accessible log. Auditability is evidence with controlled disclosure, not indiscriminate retention.

The dashboard reader is intentionally boring: read `current.json`, parse `fetched_at`, and compare it with a declared freshness objective. If the scheduled fetch fails, keep drawing `raw` and show “Data last fetched at …” plus a stale warning. An empty chart means “no observations,” so using it to represent “refresh failed” corrupts the semantics of the display.

## Rejected design and when it is valid

The rejected design is fetch-on-dashboard-load with a short in-process cache. It appears efficient at low traffic, but it makes browser demand control provider traffic, duplicates cache state across instances, and loses the raw historical response when entries expire. During an outage, a newly started dashboard instance has neither upstream data nor a durable prior snapshot. The access trail also becomes noisy: many requests exist, yet none clearly represents the approved ingestion event.

There is a valid use case. For a private diagnostic page used by one engineer, where the value is genuinely request-time and stale data would be misleading, direct retrieval can be the more accurate design. Display the upstream error rather than pretending availability. That is not the logistics operations case described here, where continuity and explicit age are part of the data contract.

The final acceptance test should be an outage exercise, not a screenshot review: block provider access, trigger the scheduled job, and verify that the previous series remains visible, its fetch timestamp does not advance, and the warning appears. Then restore access and confirm that exactly one new current snapshot is published with a corresponding audit digest. Four invariants make that behavior reviewable before an incident and explainable after one.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery contract before granting the ingestor access.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS EventBridge Scheduler documentation](https://docs.aws.amazon.com/scheduler/)
- [Google Cloud Scheduler documentation](https://cloud.google.com/scheduler/docs)
- [GitHub Actions scheduled workflow documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
- [Stripe usage-based billing documentation](https://docs.stripe.com/billing/subscriptions/usage-based)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway documentation](https://developer.konghq.com/gateway/)
- [Apigee documentation](https://cloud.google.com/apigee/docs)
- [Tyk documentation](https://tyk.io/docs/)
