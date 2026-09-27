# Reconcile API Usage Counters Against Invoices (With Credential-Scoped Dispute Evidence)

TL;DR: To reconcile API usage counters during an invoice dispute, treat the platform time series as a claim to verify, not as the source from which your internal ledger is reconstructed. For every billable attempt, persist one immutable event with a stable operation key, the credential scope, the provider's request identifier when available, and an explicit UTC billing window. Aggregate those events independently, compare them with the provider's series in fixed half-open buckets, and quarantine discrepancies before an unattended top-up changes a property's prepaid balance. The decisive architecture constraint is the blast radius of one credential: a credential shared across properties can leave the arithmetic correct while making attribution impossible.

This is an architecture decision record for a multi-tenant property-management backend. Its job is narrow but financially sensitive: prevent a prepaid service balance from running out unattended without letting retry traffic, clock boundaries, or shared credentials turn a top-up into an unauditable guess.

## How should API usage counters reconcile during an invoice dispute?

The first invariant is that a logical operation has one stable idempotency key, even if transport failures cause several attempts. The second is that attempts remain visible; deduplication must not erase evidence. The third is that money movement is based on finalized usage windows, never the still-changing edge of a provider chart. The fourth is tenant attribution: every locally recorded attempt belongs to exactly one property account and exactly one credential scope as they existed at request time.

These statements separate three quantities that are easy to collapse into one counter:

- `logical_operations`: unique business operations after deduplication;
- `attempts`: every outbound request, including retries;
- `provider_billable_units`: the units reported under the provider's published billing semantics.

A dispute begins with identifying which quantity the invoice represents. If a provider bills attempts, comparing its series with `logical_operations` will produce a plausible but false discrepancy whenever retries occur. If it bills successful operations or another defined unit, attempt count alone is equally insufficient. The billing contract or published metering definition resolves that question; intuition does not.

Consider a deliberately hypothetical bucket: 100 logical operations produced 103 attempts because three responses timed out, while two of those operations were later retried. A local counter that increments before every send reports 103. A ledger protected by the operation key reports 100 logical effects. The provider series might legitimately show either figure, or a different unit altogether, depending on its documented charging rule. The useful evidence is not the apparent gap of three; it is the set of operation keys, attempt numbers, request identifiers, outcomes, and timestamps that lets a reviewer classify each row without reverse-engineering a mutable total. This example supplies arithmetic, not a claim about any provider.

Totals can lie.

Time is another boundary, not a formatting detail. Store event instants in UTC, preserve the provider's documented bucket interval, and compare half-open intervals `[start, end)` so an event at exactly midnight belongs to one day rather than two. RFC 3339 defines an Internet timestamp representation, while the actual billing timezone and bucket closure policy still come from the applicable contract. A local dashboard should label both explicitly.

The audit trail is append-only. Corrections become adjustment records linked to the original event, never in-place edits, because reconciliation must explain what the system knew before and after a dispute. This is the same reason a ledger entry and a raw usage event have different responsibilities: the event records evidence; the ledger records a financial consequence.

## Decision: isolate evidence at the credential boundary

Use a separate production credential for the smallest tenant group whose balance and invoice must be independently reconciled, subject to the provider's credential limits and the team's rotation capacity. Record only a non-secret credential identifier or fingerprint on usage events. Never put the secret itself into logs, traces, queue payloads, or reconciliation exports. OWASP recommends limiting access to secrets, automating rotation where possible, and maintaining auditing around secret use; those controls support both containment and attribution.

The right scope is not automatically “one key per request” or even “one key per property.” It is the narrowest scope operations can reliably provision, rotate, revoke, monitor, and reconcile. A portfolio with 4,000 buildings may choose one credential per management account plus a property dimension in its immutable event log. A smaller portfolio with unusually strict financial separation may choose one per property. In either case, a compromised or misconfigured credential has an explicit maximum blast radius.

| Option | Attribution during a dispute | Credential blast radius | Operational burden | Decision |
|---|---|---|---|---|
| One credential for every tenant | Depends entirely on local metadata | Entire portfolio | Lowest rotation load | Reject for unattended balance control |
| One credential per management account | Direct account partition, then property evidence | One managed portfolio | Moderate | Default when provider limits and operations support it |
| One credential per property | Direct property partition | One property | Highest provisioning and rotation load | Use for strict isolation where supportable |

This table does not decide the provider's billing unit. It decides whether evidence can be partitioned when totals disagree. That distinction matters: credential isolation reduces ambiguity and containment scope, but it cannot repair a missing idempotency key or redefine what a vendor considers billable.

There is a real trade-off. Finer credential scopes increase provisioning, rotation, recovery, and access-review work, and they may be unsuitable when a platform imposes a credential limit below the tenant count. I choose account-level isolation as the default in this record because it bounds a financial dispute without pretending that thousands of independently rotated secrets are free to operate; property-level isolation remains the stricter option when the compliance boundary requires it.

Keep least privilege in view as well. A metering worker that only reads usage should not hold a credential capable of changing billing configuration, and a request worker should not gain access to reconciliation exports merely because both processes concern the same account. Separation makes revocation less disruptive and narrows the audit question from “what could this platform key do?” to a documented capability set.

## Critical path: record intent, attempts, and settlement separately

The critical path writes local evidence before sending the request, updates the attempt with the remote identifier and outcome, and applies an idempotent ledger effect only after the business operation reaches the state that policy defines as chargeable. A durable database transaction or transactional outbox should connect state transitions; a process-local mutex cannot survive a crash.

The following Go sketch focuses on the contract between components. `InsertAttempt` is append-only. `ApplyUsageOnce` enforces a unique constraint on the operation key inside the same database transaction as the prepaid balance mutation. The secret resolver is deliberately separate from persisted event data.

```go
package metering

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

type Attempt struct {
	OperationKey string
	AttemptNo    int
	PropertyID   string
	CredentialID string // Non-secret fingerprint used for attribution.
	StartedAt    time.Time
	ProviderID   string
	Outcome      string
}

type Store interface {
	InsertAttempt(context.Context, Attempt) error
	FinishAttempt(context.Context, string, int, string, string) error
	ApplyUsageOnce(context.Context, string, string, int64, time.Time) error
}

type Client interface {
	Do(context.Context, string, string) (providerRequestID string, units int64, err error)
}

func credentialID(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

func Execute(ctx context.Context, db Store, api Client, propertyID string,
	operationKey string, attemptNo int, secret []byte, now time.Time) error {

	a := Attempt{
		OperationKey: operationKey,
		AttemptNo: attemptNo,
		PropertyID: propertyID,
		CredentialID: credentialID(secret),
		StartedAt: now.UTC(),
		Outcome: "started",
	}
	if err := db.InsertAttempt(ctx, a); err != nil {
		return err
	}

	requestID, units, err := api.Do(ctx, operationKey, string(secret))
	if err != nil {
		_ = db.FinishAttempt(ctx, operationKey, attemptNo, requestID, "unknown")
		return err
	}
	if err := db.FinishAttempt(ctx, operationKey, attemptNo, requestID, "accepted"); err != nil {
		return err
	}

	// A database uniqueness constraint makes repeated settlement a no-op.
	return db.ApplyUsageOnce(ctx, operationKey, propertyID, units, now.UTC())
}
```

The `unknown` outcome is intentional. A timeout does not prove that the provider rejected the request. The retry reuses `operationKey`, increments `attemptNo`, and creates another attempt record. If the provider supports an idempotency mechanism, the same stable key should cross that boundary according to its documented rules; local deduplication remains necessary because provider retention periods and billing semantics may differ from the application's ledger policy.

No system can promise literal exactly-once delivery across an arbitrary network boundary. The useful exactly-once mindset is narrower: preserve every attempt, give each logical effect a stable identity, and make repeated settlement idempotent. Then a retry can be both visible for invoice analysis and harmless to the prepaid ledger.

Retries stay visible.

## Reconciliation and safe automatic top-ups

Run reconciliation after the provider's documented reporting delay and only for closed windows. For each credential scope and bucket, compute local logical operations, attempts, accepted attempts, unknown outcomes, and locally settled units. Import the remote series as a versioned observation with retrieval time and source-window metadata. Do not overwrite yesterday's import when the remote series changes; record a new observation so reviewers can see late adjustments.

Comparison should proceed from identity to totals. Match provider request identifiers first, where they exist. Next compare stable operation keys if the external export exposes them. Only then compare aggregate buckets. Aggregate equality is useful, but weak: two offsetting attribution errors can produce the right total for the wrong properties.

The top-up controller consumes a reconciliation state, not a raw dashboard counter. A practical state machine is `open`, `provisional`, `reconciled`, or `disputed`. Only `reconciled` windows contribute to forecasted burn. Fresh local events can still support a conservative reserve calculation, but a disputed window should pause an unusually large automatic adjustment and alert an operator under a documented threshold policy. The threshold is a business and compliance decision; no universal percentage is defensible.

This design also has limitations. It is not suitable for workloads where the provider exposes neither stable request identifiers nor sufficiently granular usage exports and the contract supplies no dependable billing-unit definition; local evidence can prove what the application sent, but it cannot prove how an opaque counter was derived. In that case, automatic top-ups should use a conservative cap and the commercial dispute process must resolve the uncertainty. Credential partitioning cannot manufacture missing external evidence.

Observe the pipeline with counts of unknown attempts, duplicate settlement conflicts, import age, unreconciled windows, and variance partitioned by credential scope. Avoid property identifiers and secrets in metric labels because high-cardinality labels degrade operations and sensitive identifiers expand data exposure. Put detailed evidence in access-controlled records, then link alerts to a case identifier.

Deployment deserves the same caution. Introduce event capture before allowing it to influence top-ups, shadow the reconciliation calculation over several closed billing windows, and test crashes at three boundaries: after intent persistence, after remote acceptance, and before local settlement. Also test an event exactly at a bucket boundary, a retry with the same operation key, a delayed remote observation, credential rotation midway through a window, and two workers racing to settle one operation. The acceptance criterion is not merely equal totals. It is a reproducible explanation for every difference.

## Rejected option and where it still fits

The rejected design is a single portfolio-wide credential paired with one mutable local counter. It has appealingly low provisioning overhead, but it combines the broadest credential blast radius with the weakest dispute evidence. Once the counter has been incremented, a reviewer cannot determine which attempts were retries, which property caused them, what was known at the time, or whether a later correction rewrote history.

There is a valid use case: a non-production environment containing no tenant financial state, where usage is capped, the credential has minimal permissions, and coarse aggregate monitoring is sufficient. It can also serve a short-lived integration test. It should not drive unattended replenishment of production prepaid balances.

The final decision rule is therefore compact. Scope credentials to the smallest operable reconciliation boundary; preserve immutable per-attempt evidence; settle each logical operation once; and allow automated top-ups to trust only closed, reconciled windows. Arithmetic catches variance. Identity explains it.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://www.rfc-editor.org/rfc/rfc3339
- https://www.rfc-editor.org/rfc/rfc9110
- https://opentelemetry.io/docs/specs/semconv/general/metrics/
