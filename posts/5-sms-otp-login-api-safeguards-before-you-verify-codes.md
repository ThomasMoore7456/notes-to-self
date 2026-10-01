# 5 SMS OTP Login API Safeguards (Before You Verify Codes)

Build a Node.js SMS OTP login API and its signup-link path around a single, server-side verification state machine, and make every transition append an audit event before it changes account eligibility. The deciding constraint is compliance evidence: an e-commerce team must be able to explain which challenge was active, why another message was or was not sent, and which successful verification authorized the account, without retaining the secret itself.

**Short answer:** enforce five controls: one active challenge per signup intent, atomic consumption, separate resend and verification limits, evidence recorded independently of message delivery, and a deliberately narrow fallback policy. A resend cooldown belongs in durable state rather than a browser timer. A verification link or code should succeed once, expire on schedule, and reveal the same generic failure externally whether it is wrong, stale, or already used.

This is an architecture decision record, not a provider selection. The critical boundary sits between proof issuance and account activation; email and SMS transports carry challenges, but neither transport owns the truth.

## 1. Define one signup intent and one active challenge

The aggregate root is a `signup_intent`, identified by an opaque random identifier and linked internally to the prospective account. It owns the current challenge generation, expiry, attempt counters, resend eligibility, and terminal state. A new send does not create a second independent path to activation. It supersedes the prior generation, so an older email link cannot race a newer SMS code and win after the user has requested fallback.

The invariants are concise:

1. At most one challenge generation is active for a signup intent.
2. A challenge can move to `consumed` once.
3. Transport acceptance is not verification.
4. Message retries do not extend challenge life unless policy explicitly issues a new generation.
5. An account becomes eligible only from a committed verification transition.

This boundary matters more than the delivery API. Email can be delayed, an SMS callback can be duplicated, and a user can open the same link in two tabs. Those are ordinary distributed-system events. They must not become two account activations.

For email, domain authentication supplies useful evidence about message handling rather than evidence that a human controlled the mailbox. DMARC defines policy and reporting built on SPF and DKIM identifiers; it does not prove that a verification URL was opened by the intended person. Keep DMARC aggregate reports, delivery events, and application verification events in distinct evidence classes.

## 2. How should an SMS OTP login API verify each code?

A good record survives the awkward sequence: the initial link is queued, the shopper requests an SMS fallback, the email arrives late, and both challenges are presented almost together. The database transaction that consumes a challenge must compare its generation with the current generation and append the decision event under the same lock or atomic condition. A log line written after the transaction is weaker because a process can fail between authorization and logging.

No callback gets that authority.

Store identifiers and decisions, not recoverable secrets. A practical event includes the signup intent ID, challenge generation, channel, action, outcome class, policy version, request correlation ID, and timestamp. If abuse analysis needs a network signal, retain a deliberately transformed or truncated value according to the organization's retention policy; avoid placing raw codes, full tokens, or message bodies in logs.

The evidence should answer a specific question later: “Which policy and state allowed this account to proceed?” It need not recreate the credential.

There are also compliance boundaries that architecture cannot erase. NIST SP 800-63B treats use of the public switched telephone network for out-of-band authentication as restricted and directs verifiers to consider risks such as SIM change and number porting. That makes SMS a constrained fallback, not equivalent evidence to a cryptographically protected authenticator. For a low-risk signup verification it may still be an available channel under the service's risk assessment, but the audit trail should identify the channel rather than flattening every success into `verified=true`.

## 3. Separate four limits instead of calling all of them rate limiting

One counter cannot express four different threats. Use independent controls because their keys, reset conditions, and evidence differ.

| Control | Suggested key | Prevents | Evidence to retain |
|---|---|---|---|
| Resend cooldown | Signup intent | Message bursts and accidental double-clicks | Allowed-at time, decision, policy version |
| Send budget | Destination plus risk bucket | Repeated delivery to one address or number | Window, count, outcome class |
| Verification attempts | Challenge generation | Online guessing against one challenge | Attempt ordinal and generic result |
| Enrollment creation | Account or device risk key | Rotation through fresh challenges | Decision and correlation ID |

Numbers in code must be policy, not folklore. For example, a team might start with a 60-second resend cooldown, a 10-minute challenge lifetime, and five verification attempts after modeling support load and abuse exposure; those values are illustrative local configuration, not standards or universal recommendations. Version them. A later investigation then sees `signup-v3` rather than guessing which deployment supplied the threshold.

Limits have a cost.

NIST requires rate limiting when an authentication secret has less than 64 bits of entropy, and it says a verifier accepts a given one-time secret only once while it is valid. Those are separate obligations: an atomic one-time transition addresses replay, while an attempt limit constrains online guessing. Stop early when the attempt budget is exhausted, but return a generic response so the public interface does not disclose whether the destination, generation, or code was valid.

Keep cooldown and attempt state in the authoritative datastore. A process-local timer disappears on restart and forks across replicas. A client countdown is helpful presentation, yet it is not enforcement.

## 4. Put the critical transition in one transaction

The following Go example focuses on the domain boundary. The repository implementation must make `ConsumeAndAppend` atomic, normally with a conditional update or row lock plus an event insert in one database transaction. The code deliberately omits transport SDKs and token parsing; those do not decide whether verification is committed.

```go
package signup

import (
	"context"
	"errors"
	"time"
)

var ErrVerificationRejected = errors.New("verification rejected")

type PresentedChallenge struct {
	IntentID  string
	Generation uint64
	ProofHash []byte
	RequestID string
}

type Decision struct {
	IntentID     string
	Generation   uint64
	Outcome      string
	PolicyVersion string
	OccurredAt   time.Time
	RequestID    string
}

type Repository interface {
	// ConsumeAndAppend compares the active generation and proof hash, enforces
	// expiry and attempt limits, consumes once, and appends the decision atomically.
	ConsumeAndAppend(context.Context, PresentedChallenge, Decision) (bool, error)
}

type Verifier struct {
	repo          Repository
	now           func() time.Time
	policyVersion string
}

func (v Verifier) Verify(ctx context.Context, p PresentedChallenge) error {
	decision := Decision{
		IntentID:      p.IntentID,
		Generation:    p.Generation,
		Outcome:       "accepted",
		PolicyVersion: v.policyVersion,
		OccurredAt:    v.now().UTC(),
		RequestID:     p.RequestID,
	}

	accepted, err := v.repo.ConsumeAndAppend(ctx, p, decision)
	if err != nil {
		return err
	}
	if !accepted {
		return ErrVerificationRejected
	}
	return nil
}
```

Idempotency needs two layers. Repeating the same verified request should return the already-committed business result, while presenting a consumed secret through a different request must not cause another transition. An idempotency key handles request replay; the challenge's conditional state change handles credential replay. Confusing the two leaves a gap.

Test the gap directly. Run two concurrent consumes for the same generation and assert that one transition and one activation event exist. Then inject a failure after the conditional update but before commit, retry the request, and confirm that neither an unlogged activation nor two audit events can appear. Test clock boundaries with an injected clock, including exactly at expiry and exactly at cooldown release. Also test delayed callbacks, because a provider callback is observational input and must never reactivate a superseded challenge.

Deploy policy changes with compatibility in mind: an in-flight challenge should continue under its recorded policy version or be explicitly invalidated with a reason event. Quietly interpreting an old challenge under new limits makes later reconciliation ambiguous.

## 5. Reject transport-owned verification, but preserve its valid use case

The rejected design lets each messaging transport create, verify, throttle, and expire its own challenge, then treats a successful callback as account authorization. It is attractive because it reduces application code. It also fragments the evidence boundary: email-link state and SMS-code state can disagree, provider retries can be mistaken for business transitions, and a transport migration can change enrollment semantics.

So the decision is to own challenge state and authorization in the application domain, with an outbox or equivalent transactional delivery record connecting committed send intent to asynchronous workers. A stable deduplication key such as `intent ID + generation + channel` lets the worker retry without creating a new business challenge. Delivery status remains useful operational evidence, but account state never depends on receiving a webhook in time.

Transport-owned verification still has a valid use case. A low-consequence notification flow with no alternate channel, no durable authorization decision, and no requirement to reconcile the proof later may reasonably delegate the whole exchange. Account signup with a fallback channel fails those conditions.

The application-owned design has a real limitation: the team must operate durable state, transactional event recording, key rotation, and reconciliation even when the transport already offers some of those controls. Its trade-off is higher implementation and operational responsibility in exchange for one authorization boundary. It is not suitable for a team that cannot maintain that boundary safely; in a low-consequence, single-channel flow, delegating verification and recording only the resulting business event can be the more defensible choice. Neither design proves that the person receiving a message is the intended legal identity, and neither removes the need for a documented retention policy.

This choice also shapes observability. Track transitions by outcome class, generation, policy version, and channel; alert on changes in rejection or send-decision rates without placing secrets or raw destinations in metric labels. Reconcile three ledgers independently: challenge decisions, delivery attempts, and account activations. Counts will not always match one-for-one because retries and supersession are expected, but every activation should trace to exactly one accepted challenge transition.

The result is intentionally boring: links and codes become short-lived inputs to one auditable state machine. That is the property worth preserving when transports, policies, and threat models change.

## References

- RFC 7489, “Domain-based Message Authentication, Reporting, and Conformance (DMARC)”: https://datatracker.ietf.org/doc/html/rfc7489
- NIST Special Publication 800-63B, “Digital Identity Guidelines: Authentication and Lifecycle Management”: https://pages.nist.gov/800-63-3/sp800-63b.html
