# Small SaaS Uptime Monitoring: Auditable Node Health Endpoint During Rollbacks

For a small education SaaS running a Node service, the best simple uptime monitoring design is the one that preserves a falsifiable account of notification delivery through a rollback. **Use an external health endpoint probe for reachability, a deadline witness for scheduled work, and an idempotent receipt for each terminal delivery attempt.** A green response alone cannot prove that a lesson-reminder cron job ran or reached a terminal outcome; conversely, one failed reminder does not prove that the public service was unreachable.

TL;DR: define the evidence before choosing the tool. Probe from the EU and US only when those viewpoints correspond to the service's actual reachability requirement, treat cron silence as a missed deadline rather than an HTTP failure, and keep delivery receipts independent of application releases. Select a monitor by replaying and rolling back those signals in a test environment. Price can constrain retention and probe frequency, but it should not decide what counts as delivery truth.

## Can small SaaS uptime monitoring trust a Node health endpoint?

Begin with one invariant: for every notification that became eligible, operators must be able to distinguish no attempt, an unfinished attempt, and a terminal outcome. The distinction must survive a release reversal. This is stricter than asking whether a process is alive, yet narrower than asking an observability platform to reconstruct every business event from logs.

An edtech notification service has at least 3 independent observations. An external probe records whether a public surface answered at a given time and place. A scheduler witness records whether an expected run reported completion before its deadline. A delivery receipt records the terminal state of one attempt. Their timestamps may be close, but their meanings are not interchangeable.

Keep the health response shallow and free of student data. It should report the service's ability to accept traffic under the team's documented contract, not execute a synthetic lesson reminder. Deep checks can couple availability to a downstream dependency that the request path does not need, producing a red status that says little about the user-visible operation under investigation.

Silence is evidence too.

For a scheduled job, however, silence becomes actionable only after the expected time plus an explicit grace period. There is no universal correct grace period in the supplied standards; it is a service decision that should reflect the notification deadline and scheduler behavior. Record the schedule or policy version alongside the run identifier so that a later timetable change does not reinterpret an old absence.

## Model delivery as a reconciled state machine

The useful analogy is ledger reconciliation. An expected notification is one side of the record, while attempts and their terminal receipts are the other. Reconciliation detects zero terminal receipts after the deadline and rejects more than one accepted terminal receipt for the same attempt. This exactly-once mindset does not claim exactly-once transport, which a monitor cannot guarantee; it makes duplicate evidence detectable and replay harmless.

A compact contract is enough. Stable identifiers belong in the durable event store, while metrics should use bounded labels. The cited metric-naming guidance says a metric should represent one logical quantity, use base units, and carry a suffix where applicable. A terminal-outcome counter may therefore label a controlled outcome and channel, but a student identifier, email address, attempt identifier, or error message should not become a metric label.

```go
package evidence

import (
	"context"
	"errors"
	"time"
)

type Outcome string

const (
	Delivered Outcome = "delivered"
	Failed    Outcome = "failed"
	Expired   Outcome = "expired"
)

type Receipt struct {
	NotificationID string
	AttemptID      string
	RunID          string
	Outcome        Outcome
	ObservedAt     time.Time
	PolicyVersion string
}

type Store interface {
	InsertReceipt(ctx context.Context, receipt Receipt) (inserted bool, err error)
}

func RecordTerminal(ctx context.Context, store Store, receipt Receipt) error {
	if receipt.AttemptID == "" || receipt.NotificationID == "" || receipt.ObservedAt.IsZero() {
		return errors.New("incomplete terminal receipt")
	}
	_, err := store.InsertReceipt(ctx, receipt)
	return err
}
```

`InsertReceipt` needs a uniqueness rule on the attempt identifier. A retry may submit the same receipt again, and that replay must neither create another accepted outcome nor increment a derived counter twice. The return value remains useful for metrics: emit an increment only after a new durable insertion, or derive aggregates from committed receipts. The latter increases query work; the former requires careful recovery between the write and metric emission. Document which record is authoritative.

Replays happen.

Do not use a release identifier as the idempotency key. Releases change during rollback; the business attempt does not. Store the release identifier only as diagnostic context, and evolve the receipt schema additively so that the previous reader can tolerate records written by the newer deployment.

## Break the witnesses separately

The acceptance test should attack one observation at a time. Stop the scheduler while leaving the API available: the deadline alert should fire, while the reachability probe remains green. Cause a delivery attempt to reach a controlled terminal failure: reconciliation should preserve one failed receipt without declaring the whole public surface unavailable. Replay that receipt. Nothing should be counted twice.

Then reverse a deployment while an attempt is in flight. The older release must read or safely ignore newly added optional fields, the durable receipt store must remain available, and the monitoring system must not erase or close an alert merely because a deploy event occurred. This is the practical meaning of rollback safety here: evidence continues across the release boundary.

Regional probes deserve the same discipline. An EU observation and a US observation can distinguish a regional reachability condition from a broad one, but neither observation establishes notification delivery. Before paying for more locations, decide which claim each location is expected to support and how responders will interpret disagreement.

Alert severity also requires a local policy. RFC 5424 defines 8 severity values from Emergency through Debug, while warning that numerical severity values are not an exact operational definition across applications. Map evidence to response explicitly: a missed run containing time-sensitive mandatory notices may require immediate action, whereas one failed optional reminder may be retained for reconciliation or aggregated under a rate policy. Preserve both the raw outcome and the policy version. Changing a rule later must not rewrite history.

## Evaluate monitors with an evidence worksheet

Once the state model passes fault injection, compare monitoring options against the same worksheet. This keeps the decision vendor-neutral and prevents a polished setup flow from substituting for coverage. A search for cheap Healthchecks, StatusCake, or Better Stack plans can produce procurement candidates, but their names establish none of the required evidence; each candidate still has to pass the same rollback exercise. A small hosted service, a self-operated stack, or several narrow components may satisfy the contract, and none is categorically better because the ownership burden differs.

| Required claim | Minimum observation | Reversal test | Disqualifying gap |
|---|---|---|---|
| The public service was reachable | Timestamped external result from each required region | Results remain visible across deploy and rollback | Checks run only inside the failing service |
| A scheduled notification run completed | Run identity, expected deadline, completion time | Rollback does not reset the last accepted run | Process liveness stands in for completion |
| A delivery attempt terminated | Idempotent receipt with controlled outcome | Replaying a receipt produces no second accepted outcome | Only free-form logs remain |
| Responders were notified | Alert, route, acknowledgement, and policy version | Deploy events cannot silently close the alert | Alert history is unavailable for review |

The selection questions follow directly. Can the system express both active HTTP checks and deadline-based absence, or must a second component own one of them? Can observations and alert history be exported in a form that retains timestamps and stable identifiers? Are retention, access control, and regional processing compatible with the organization's educational-record and audit obligations? The relevant compliance limits must come from the responsible legal and security owners; an uptime dashboard cannot infer them. Operational simplicity also includes failure ownership. Combining all signals in one system reduces integrations but enlarges the effect of one configuration error. Separating probes, deadline witnesses, and the receipt store narrows each component's authority but creates more credentials, routes, and reconciliation work. **Choose the smallest arrangement whose independent failures are visible and whose evidence can be recovered.** The limitation of a probe-only arrangement is decisive for this workload: it is not suitable when missed cron runs or delivery outcomes must be audited. A heartbeat-only arrangement has the opposite limitation because it cannot establish user-visible reachability. The trade-off is additional integration work in exchange for claims that remain narrow and testable. Cost still belongs on the worksheet, as a constraint rather than the conclusion. Estimate probe volume from interval and location count, metric volume from bounded label combinations, receipt volume from actual attempts, and retention from the investigation period. Avoid volatile price comparisons in the architecture record. The durable decision is which evidence may be sampled, aggregated, or discarded first: high-cardinality diagnostic context should yield before terminal failure receipts and missed-run records needed for reconciliation.

## Migrate without changing the verdict

Roll out the contract in reversible increments. First, add the shallow probe and observe EU/US disagreement without connecting it to paging. Next, write terminal receipts alongside the current delivery records and compare reconciliation results. Add deadline witnesses in shadow mode after the run identity and schedule policy are stable. Route alerts only after intentionally missing a run, replaying a receipt, and reversing a release all produce the expected verdict.

Keep the old evidence path readable until the new path has covered the organization's required investigation window; that duration is policy-specific, so it should not be invented by the monitoring team. At the end, one acceptance question decides the migration: after rollback, can an operator still determine which scheduled notification run was absent and which delivery attempt reached one terminal outcome, without treating a green health response as proof? If yes, the monitoring arrangement is simple in the way that matters.

## Sources

- Prometheus, "Metric and label naming": https://prometheus.io/docs/practices/naming/
- IETF, "RFC 5424: The Syslog Protocol": https://datatracker.ietf.org/doc/html/rfc5424
