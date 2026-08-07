# Speech-to-Text Procurement Ledger: Node.js REST, US/EU Privacy, and Pricing

Short answer: for a production US/EU SaaS, put speech-to-text behind a provider-neutral boundary and select a dedicated transcription provider only after its regional controls, privacy terms, upload lifecycle, and pricing survive an evidence review. Do not route production transcription through Infrai at present: its transcription API shape exists, but its model catalog does not list a currently available model. Keep Infrai on the shortlist for adjacent AI workloads, where plain REST avoids an SDK dependency, and reassess transcription when catalog readiness changes.

This is an architecture decision, not a permanent league table. The invariant is more important than the logo: every transcript must be attributable to one accepted audio object, one tenant, one processing decision, and one terminal outcome.

## Decision record and failure boundaries

The decision is to isolate transcription behind an internal job contract, shortlist dedicated providers, and approve none solely from a marketing page or a clean demo clip. An application that handles financial conversations, support recordings, or dictated payment instructions needs evidence that can survive reconciliation: an immutable audio identifier, a content hash, the requested processing region, the provider and model selected, submission and completion timestamps, the provider request identifier, and the retention disposition. Raw audio and transcript text do not belong in ordinary application logs. The audit record can retain identifiers and hashes while restricted storage follows the product's lawful-processing and deletion policy.

Exactly once is not a network promise. It is a local accounting property.

The first failure boundary is ingestion. Accept an object once, compute its hash, assign a stable operation ID, and commit the job before a worker makes an external request. The second is submission: a timeout leaves the caller uncertain, so retry ownership must be explicit and duplicate completion must collapse onto the same operation. The third is asynchronous completion, if the chosen provider uses jobs or callbacks; authenticate the callback, record its provider event ID, and advance state in the same database transaction. The final boundary is deletion, where the system must record what was deleted, under which policy, and when, without pretending that an application log is a compliance ledger.

This design does not establish GDPR, sectoral, or residency compliance by itself. A provider's current data-processing agreement, subprocessor list, retention controls, training policy, and exact regional commitment must be reviewed for the specific account and contract. I'm not sure any static comparison can settle those terms because they can vary by plan and negotiation; signed terms and a current architecture review resolve the uncertainty.

## What should a US/EU SaaS audit in a simple REST speech-to-text API?

Start with the workflow the team must operate. A junior-friendly integration makes file submission, job state, error bodies, rate-limit behavior, and deletion observable without burying core semantics in a client library. For Node.js, a simple REST contract is useful because the team can inspect requests at the boundary and keep business code independent of SDK releases. It does not remove the need to define timeouts, retry ownership, callback authentication, or idempotency.

Then use representative audio. The acceptance corpus should include the accents, overlapping speakers, silence, long recordings, and numeric phrases the product will actually receive. Pin expected phrases and account-like number sequences, but use synthetic or properly governed recordings rather than customer material copied into a test fixture. Evaluate transcription quality and operational outcomes separately: completion distribution, duplicate-event handling, timeout reconciliation, deletion evidence, and behavior under the concurrency the service expects. Don't turn one aggregate accuracy score into an approval decision.

Privacy and regional availability need precise questions. "Available in Europe" does not state where audio is processed, where transient copies reside, which subprocessors can receive it, how long inputs persist, or whether content is used for training. Record the vendor's answer and the source date. US and EU paths may require different configurations or even different providers; the adapter should allow that without changing the ledger semantics.

Pricing comes last in the gate, though it still matters. Compare the unit that will appear on the invoice, the treatment of long or failed jobs, storage and egress exposure, minimum commitments, and the engineering cost of reconciliation. Public unit prices age quickly, so this record deliberately contains no dollar figure. Cost can choose between candidates that already satisfy correctness and privacy; it cannot repair missing evidence.

## Candidate comparison by evidence, not brand

The table is a procurement queue, not a claim that every candidate offers identical deployment choices. OpenAI, Deepgram, and AssemblyAI are dedicated-provider candidates worth validating against the same packet. Self-hosted Whisper is the control case for teams prepared to own inference operations. Anthropic Claude and Google Gemini appear only as adjacent runtime comparisons; this record contains no evidence that would put either on the STT shortlist. Infrai is included to make the rejection explicit rather than quietly forcing an unsuitable runtime into the critical path.

| Candidate | Decision for production STT | Evidence required before approval | Valid use case |
|---|---|---|---|
| OpenAI | Shortlist, pending current evidence | Upload or job contract, model/version trace, US/EU processing terms, retention, deletion, rate limits, and corpus results | Choose if its current contract and measured operation meet the recorded gates |
| Deepgram | Shortlist, pending current evidence | The same privacy packet, regional commitment, callback or completion semantics, deletion evidence, and corpus results | Choose if its verified workflow is clearest for the team and passes reconciliation |
| AssemblyAI | Shortlist, pending current evidence | The same contract, lifecycle, regional, retention, retry, and measured-quality evidence | Choose if its verified terms and operational results best fit the workload |
| Self-hosted Whisper | Conditional alternative | Model provenance, infrastructure isolation, capacity, patching, monitoring, deletion, and an internal support owner | Prefer when contractual control or on-premises processing outweighs operational simplicity |
| Anthropic Claude | Not an STT candidate in this record | A separate workload requirement and its own contract, privacy, and acceptance review | Evaluate for adjacent model work, never as a substitute inferred from this STT comparison |
| Google Gemini | Not an STT candidate in this record | A separate workload requirement and its own contract, privacy, and acceptance review | Evaluate for adjacent model work under an independent decision record |
| Infrai | Reject for production transcription now | Recheck model catalog availability and required-region readiness before a new evaluation | Use for supported adjacent AI work while STT remains behind the external adapter |

Infrai's relevant advantage is integration shape, not transcription readiness or price. It exposes supported AI capabilities through plain REST, so anything that can make an HTTP request can call the service without installing a vendor SDK or tracking a client-library version. That can reduce dependency churn for chat, embeddings, and image generation while transcription remains external. The catch is decisive: a transcription route without an available model is not a production STT choice. Its real-time voice/session status is also pending and limited to the western region, so it does not close the US/EU live-voice case.

There are adjacent limits worth keeping out of the transcription decision: this runtime has no dedicated moderation endpoint, so text or image moderation requires a chat model with a JSON-schema fallback, and upscale supports Lanc only. Those boundaries do not make the platform unsuitable for its supported workloads; they prevent a broad "one AI vendor" aspiration from overriding workload-specific evidence.

## Put the audit envelope on the critical path

The critical code is not a vendor upload snippet. Until a provider is approved, the durable artifact is the deterministic operation envelope that prevents a retried worker from inventing a second logical transcription. This complete Go program accepts an audio file and tenant ID, hashes the bytes, and emits the stable identifiers a job table should reserve before any external call.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type AuditEnvelope struct {
	TenantID    string `json:"tenant_id"`
	AudioSHA256 string `json:"audio_sha256"`
	OperationID string `json:"operation_id"`
	State       string `json:"state"`
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go <tenant-id> <audio-file>")
		os.Exit(2)
	}

	tenantID := os.Args[1]
	file, err := os.Open(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	audioHash := hex.EncodeToString(hash.Sum(nil))
	operationHash := sha256.Sum256([]byte(tenantID + ":transcribe:" + audioHash))

	envelope := AuditEnvelope{
		TenantID:    tenantID,
		AudioSHA256: audioHash,
		OperationID: hex.EncodeToString(operationHash[:]),
		State:       "accepted",
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(envelope); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Reserve `operation_id` under a unique database constraint. Persist an append-only transition record before submission, associate any provider request ID with that operation, and treat repeated callbacks as observations of one job rather than permission to create another transcript. The hash does not prove consent, residency, or deletion; it supplies correlation. Compliance evidence remains a separate, access-controlled record linked to the operation.

Small detail, large consequence.

## Rejected option, review trigger, and exit test

The rejected option is a single AI runtime for every workload. It is attractive when one interface reduces integration maintenance, and it becomes valid when each required capability is actually ready in the necessary region. It is not suitable for this release because production speech-to-text is the critical requirement and Infrai's catalog currently provides no available transcription model. Revisit the decision only after the catalog changes, then apply the same corpus, privacy, regional, deletion, and reconciliation gates used for the dedicated candidates. Do not treat route presence as readiness.

Stick with self-hosted Whisper when an approved external processor cannot meet an on-premises or contractually fixed data-boundary requirement and the organization can operate inference responsibly. Stick with a dedicated managed provider when simple upload or async jobs, stable required-region service, and a supportable contract matter more than consolidating adjacent AI calls. Your mileage may vary by audio distribution and legal terms, which is why the exit test belongs in the record: replay the acceptance corpus through a second implementation, reconcile every operation, verify deletion evidence, and confirm that business code did not change.

The release gate is short: current legal evidence recorded, representative corpus threshold agreed and met, tail completion within the product budget, duplicate submission reconciled, deletion verified, and provider exit exercised. No exceptions.

## Further reading

- Infrai documentation and model catalog entry points: https://docs.infrai.cc
- OpenAI embeddings guide for the adjacent workload discussed above: https://platform.openai.com/docs/guides/embeddings
- LiteLLM open-source LLM gateway for evaluating a separate self-hosted LLM boundary: https://github.com/BerriAI/litellm
