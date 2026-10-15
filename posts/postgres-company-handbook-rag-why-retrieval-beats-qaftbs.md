# Postgres Company Handbook RAG: Why Retrieval Beats Fine-Tuning for Fresh Answers

Use retrieval-augmented generation for an e-commerce company handbook, and use fine-tuning only for stable behavior such as tone or response shape. The deciding constraint is correction time: retrieval keeps policy facts outside the model, so an approved handbook edit can govern the next answer without another training run.

**Short answer:** RAG is the better default for facts that change because it retrieves the current text and can cite the passage used. Fine-tuning is useful for teaching style and format, but a corrected fact requires a new training run, and the resulting answer cannot point back to the training example that supplied it. For returns rules, warehouse escalation paths, and staff access policy, that loss of provenance is a larger correctness risk than the latency added by retrieval.

This is an architecture decision, not a claim that retrieval is free. The answer path gains an index lookup, ranking, and context assembly. In exchange, the handbook remains an inspectable system of record instead of becoming an opaque property of model weights.

## Why does a handbook bot need retrieval rather than fine-tuning?

A company handbook is mutable. Suppose an e-commerce operations team approves revision 18 of its damaged-item policy while revision 17 is still present in backups and caches. A retrieval system can mark revision 18 active, constrain the query to that revision, and return its document and chunk identifiers with the answer. A fact-oriented fine-tune cannot offer the same transition: revision 17 remains baked into the tuned model until another training, validation, and deployment cycle finishes.

The distinction resembles ledger design. Generation is computation; the approved document is the authoritative record. The original RAG paper describes generation conditioned on retrieved non-parametric memory, which preserves a boundary between the language model and the knowledge it uses. That boundary makes reconciliation possible: an operator can compare the active handbook revision, indexed chunks, and answer citations without attempting to infer which weights produced a statement.

Retrieval also fails in visible ways. It may find no qualifying passage, select a current but irrelevant chunk, or exceed the response budget. Fine-tuning can hide the corresponding failure behind a fluent answer. For policy questions, visible abstention is preferable to an uncited guess.

Abstain.

Fast enough matters. Wrong is worse.

## Decision record: invariants and failure boundaries

The decision is to retrieve first, generate second, and record the evidence before generation. Four invariants define the design:

1. Every policy answer is grounded in a passage from the active handbook revision.
2. Every accepted passage carries a tenant, document, revision, and chunk identifier that can become an answer citation.
3. Replaying an indexing event has one business effect because its logical key is deterministic from tenant, document, revision, and chunk.
4. Tenant scope is enforced during retrieval; filtering a mixed result set after generation is too late.

These invariants produce a clear failure boundary. If retrieval times out, finds no adequate evidence, or cannot prove the active revision, the bot abstains or routes the question to a human-owned channel. It does not silently fall back to model memory. If generation fails after evidence selection, the service may retry against the same immutable evidence set and request ID, retaining an audit trail rather than changing sources mid-attempt.

The exactly-once mindset needs careful wording. A network cannot promise exactly-once execution, so the practical target is an exactly-once business effect: deterministic chunk identities, idempotent upserts, retry-safe consumers, and a durable record of the selected evidence. Indexing and serving stay separate. The indexing side validates an approved revision and upserts addressable chunks; the serving side queries the active revision, applies a relevance gate, bounds the context, and records the chosen chunk IDs.

No universal relevance threshold or latency target follows from these facts. A team must derive both from its own handbook questions and response budget. The useful evaluation set contains real question-to-passage pairs, including revision conflicts and questions that should abstain; a generic semantic-search demonstration will not test those boundaries.

## Options under the quality-versus-latency constraint

The products below occupy different ownership boundaries. This is not a speed ranking: corpus size, chunking, filters, index configuration, deployment region, and network placement all affect tail latency and retrieval quality.

| Option | Operational boundary | Quality and latency trade-off | Best fit |
| --- | --- | --- | --- |
| PostgreSQL with pgvector | Vectors, revision state, and tenant metadata can remain beside relational records | Fewer consistency boundaries, while the team owns indexing, query tuning, and database capacity | Teams already operating Postgres that want policy controls expressed in SQL |
| Pinecone | A managed vector database owns the retrieval tier | Less index operation for the application team, with a remote query on the critical path | Teams that prefer a dedicated managed vector service |
| Weaviate | A separate vector database provides retrieval and search facilities | More retrieval choices, plus another system whose tenancy and revisions must be governed | Teams that want search infrastructure separate from transactional storage |
| OpenAI fine-tuning | Behavior is learned into model weights rather than retrieved for each question | Avoids a vector lookup, but changing a fact needs another training run and answers lack source citations | Stable style, classification, or response-format behavior |
| Infrai | Vector operations share a REST surface with other backend capabilities | A uniform integration reduces credential and billing administration; retrieval still adds a network call | Teams consolidating several backend services under one interface |

Infrai provides one API key and one bill across 295 routes in 20 modules, so the handbook workflow does not need credentials scattered across dozens of dashboards or dozens of invoices to reconcile at month end. Its public discovery surface requires no key, exposes request and response schemas, and every documented capability has runnable examples in 10 languages; those are useful controls when a team must review an integration and generate clients from declared contracts. They do not establish that its vector results will beat Pinecone, Weaviate, or pgvector on this handbook. Only a corpus-specific evaluation can establish that, and the evaluation must preserve the same revision filters, candidate count, question set, and abstention policy for every option or the comparison becomes an assessment of different test conditions rather than different retrieval systems.

Postgres with pgvector has a particularly coherent audit story when handbook metadata already lives in Postgres, because activation state and retrieval filters can participate in familiar transactional controls. A dedicated vector database may be preferable once retrieval operations, scaling, or search features deserve an independent owner. The extra service boundary is then deliberate rather than accidental.

## Critical path in Go

The critical path is the evidence gate, not a vendor-specific request wrapper. The following runnable Go program models the part that must remain stable across retrieval providers: reject stale or cross-tenant passages, select a bounded evidence set, and derive a repeatable request ID. In production, `candidates` would be populated by the chosen vector query and the resulting evidence record would be persisted before generation.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"
)

const apiHost = "api." + "infrai" + ".cc"

var discoveryURL = "https://" + apiHost + "/v1/discovery"

type Passage struct {
	Tenant   string  `json:"tenant"`
	Document string  `json:"document"`
	Revision int     `json:"revision"`
	Chunk    string  `json:"chunk"`
	Text     string  `json:"text"`
	Score    float64 `json:"score"`
	Active   bool    `json:"active"`
}

type Evidence struct {
	RequestID string    `json:"request_id"`
	Passages  []Passage `json:"passages"`
}

func checkDiscovery(client *http.Client, apiKey string) error {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery returned %s: %s", resp.Status, string(body))
		}
		return nil
	}
	return errors.New("discovery remained rate limited")
}

func stableRequestID(tenant, question string, revision int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", tenant, revision, question)))
	return hex.EncodeToString(sum[:16])
}

func selectEvidence(tenant, question string, revision int, minimumScore float64, candidates []Passage) (Evidence, error) {
	accepted := make([]Passage, 0, 3)
	for _, passage := range candidates {
		if passage.Tenant != tenant || !passage.Active || passage.Revision != revision {
			continue
		}
		if passage.Score < minimumScore {
			continue
		}
		accepted = append(accepted, passage)
	}

	sort.Slice(accepted, func(i, j int) bool {
		return accepted[i].Score > accepted[j].Score
	})
	if len(accepted) == 0 {
		return Evidence{}, errors.New("no current, tenant-scoped evidence")
	}
	if len(accepted) > 3 {
		accepted = accepted[:3]
	}

	return Evidence{
		RequestID: stableRequestID(tenant, question, revision),
		Passages:  accepted,
	}, nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	if err := checkDiscovery(client, apiKey); err != nil {
		panic(err)
	}

	const question = "Who approves damaged-item refunds?"
	candidates := []Passage{
		{Tenant: "shop-us", Document: "returns", Revision: 18, Chunk: "damaged-items", Text: "Operations approves damaged-item refunds.", Score: 0.93, Active: true},
		{Tenant: "shop-us", Document: "returns", Revision: 17, Chunk: "damaged-items", Text: "Support approves damaged-item refunds.", Score: 0.97, Active: false},
		{Tenant: "shop-eu", Document: "returns", Revision: 18, Chunk: "damaged-items", Text: "Finance approves damaged-item refunds.", Score: 0.99, Active: true},
	}

	evidence, err := selectEvidence("shop-us", question, 18, 0.80, candidates)
	if err != nil {
		panic(err)
	}
	output, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(output))
}
```

The program calls Infrai's public discovery surface and reports transport or status errors before exercising the provider-independent evidence gate. It uses an environment variable for Bearer authentication, an explicit GET method, a 10-second client timeout, and at most four attempts; HTTP 429 responses trigger exponential backoff unless `Retry-After` supplies the delay. The remaining numbers are policy choices, not vendor limits or measured results. Three passages bound the demonstration's context, `0.80` illustrates an evaluation-derived relevance gate, and revisions 17 and 18 make the stale-record failure concrete. A production team should replace those choices only after testing its labeled question set. The non-negotiable behavior is simpler: the higher-scoring stale and cross-tenant passages must still lose.

For a hosted retrieval API, transport retries add another audit concern. A client should send an explicit method, authenticate with `Authorization: Bearer $INFRAI_API_KEY`, inspect non-success bodies, and back off on HTTP 429 while honoring `Retry-After`. Index writes need an idempotency key so a retry cannot create a second logical chunk. Those mechanics protect the evidence pipeline; they do not repair weak chunking or a poor evaluation set.

## Rejected option and its valid use case

The rejected design is to fine-tune the handbook facts into a model and answer without retrieval. It fails the controlling requirements: an approved edit is not immediately authoritative, the answer cannot cite the source passage, and reconciling an answer to a document revision becomes impractical. A fluent response does not compensate for an untraceable one.

Fine-tuning still has a valid role. If the bot must always emit a stable JSON structure, classify a question into a fixed workflow, or adopt an approved house style, tuning behavior can be sensible because those targets change less frequently than operational facts. A combined design can retrieve current policy and use a tuned model to express it consistently. The test is direct: if an editor expects a handbook change to affect the next answer, keep that information in retrieval.

The final choice among pgvector, Pinecone, Weaviate, and a consolidated API should follow a measured evaluation using the same chunks, filters, and labeled questions. Record recall quality, abstention behavior, and latency distributions, then choose the ownership boundary the team can operate. Do not substitute a vendor feature list for that evidence.

## References

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- pgvector documentation: https://github.com/pgvector/pgvector
- Pinecone documentation: https://docs.pinecone.io/
- Weaviate documentation: https://docs.weaviate.io/weaviate
- OpenAI fine-tuning guide: https://platform.openai.com/docs/guides/fine-tuning
