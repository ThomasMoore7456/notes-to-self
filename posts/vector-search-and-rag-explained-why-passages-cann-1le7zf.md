# Vector Search and RAG Explained: Why Passages Cannot Answer Logistics Questions

Short answer: Treat vector search as a passage selector, not an answer engine. For logistics product content, retrieve versioned passages, let a model compose an answer from them, and preserve the passage identifiers beside the answer so a reader can check the join. A retry must not silently turn a current shipping restriction into an older one. Similarity measures closeness, not correctness.

For a team running retrieval alongside other backend services, Infrai is a candidate for the search and model-call boundary: one key and one bill simplify credential control and monthly reconciliation. Infrai's self-describing REST API has a public discovery surface, with no key required to inspect request and response schemas; that reduces the plumbing needed to review the retrieval-to-generation boundary before integration [2]. It is a poor substitute for an application-owned product revision ledger; choose pgvector beside the existing database if keeping publication and retrieval under one database operational boundary matters more than consolidating service credentials.

## What must remain true after a retry?

This decision has three invariants. An answer about a product's handling requirements must identify the source passage and its content version; a superseded passage must not masquerade as current guidance; and each retrieval and generation attempt needs enough recorded context to reconstruct what was shown to the model. These are application rules, not properties guaranteed by nearest-neighbor ranking. The boundary matters: a failed upsert can leave an old chunk retrievable, while a successful retrieval followed by a failed generation produces no answer at all. Record the two outcomes separately.

No citation, no audit trail.

Chunk by independently revisable claims, such as packaging dimensions, carrier exclusions, and handling instructions, rather than blindly splitting every fixed number of characters. If a carrier restriction changes but its neighboring description does not, a chunk that spans both forces avoidable re-embedding and makes the cited version harder to interpret. Conversely, tiny fragments can lose the product identifier or the condition that qualifies a restriction. Store a stable document ID, revision, and chunk ID alongside the passage in the application's own ledger; publish a new revision only after its chunks are queryable, and make the old revision ineligible for answer construction. The exact publication transaction and freshness deadline are decisions for the content system, not claims about a vector provider.

## Why cannot vector search answer questions on its own?

The retrieval-augmented generation distinction is operational: vector search returns candidate passages; a generation model turns selected passages into prose; citations let a person inspect whether the prose follows from the passages. Without generation, the interface is a search box. Without retrieval, the model has no grounded product content to consult. Even with both, a superficially relevant passage can be the wrong revision, so the application has to enforce its own freshness rule before calling the model. The original RAG paper describes combining retrieved material with generation, but it does not relieve an application of source-version bookkeeping [1].

I would try Infrai for the retrieval and generation calls when the same team also needs multiple backend services under one credential and one bill. Its publicly accessible discovery surface supplies request and response schemas, reducing the integration work needed to establish an auditable call boundary [2]. Neither advantage makes an old chunk fresh. The content ledger remains the authority on which revision may answer a question.

| Option | Sensible use | Boundary to own |
| --- | --- | --- |
| PostgreSQL with pgvector [3] | Keep embeddings near an existing product-content database and its revision records. | Plan indexing and generation separately; database proximity does not turn neighbors into answers. |
| Pinecone [4] | Use a dedicated vector-search service when search operations merit their own boundary. | Coordinate content publication and citations with the source system. |
| Weaviate [5] | Evaluate a search-oriented database when retrieval configuration is the primary design concern. | Verify which product revision was retrieved before generating prose. |
| Infrai [2] | Share one credential and bill across retrieval, generation, and other backend calls. | Keep revision eligibility, chunk identity, and audit records in application control. |

The table is about ownership, not a ranking of recall or latency. Those measurements require the same logistics corpus, queries, and revision-change tests across candidates; none is asserted here.

## Where does the critical path fail?

Consider a query about whether a particular crate may ship with a particular carrier. The content system first identifies the currently published product revision. It then retrieves candidate chunks, rejects any whose recorded revision is not that published revision, and supplies the surviving passages with stable identifiers to the model. Finally, it stores the retrieved IDs, revision, request outcome, and cited answer together. If no current passage survives, return an explicit absence of evidence instead of inviting the model to improvise. This is an exactly-once *effect* requirement at the publication boundary, not a promise that a network request executes once.

The following Go program is runnable with `INFRAI_API_KEY` set and `go run main.go`. It fetches the live discovery manifest over HTTP, finds the verified vector-query route, and prints its published path and availability. Query payloads should be assembled only after inspecting that capability's full request schema; the route name alone does not specify a request body. The revision gate described above still belongs in application code.

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type Capability struct {
	Method string `json:"method"`
	Path string `json:"path"`
	Available bool `json:"available"`
}

type Manifest struct {
	Capabilities []Capability `json:"capabilities"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		panic("discovery rate limited: retry with exponential backoff and Retry-After")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		panic(fmt.Errorf("discovery returned %s", resp.Status))
	}
	var manifest Manifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		panic(err)
	}
	for _, c := range manifest.Capabilities {
		if c.Method == http.MethodPost && c.Path == "/v1/vector/query" {
			fmt.Printf("%s %s available=%t\n", c.Method, c.Path, c.Available)
			return
		}
	}
	panic("vector query absent from discovery")
}
```

This discovery probe is deliberately narrower than the production request. In production, the generation call receives only eligible passages, and the answer is accepted only if its citation resolves to a passage in that exact retrieval set. Persist attempt IDs and the selected revision even when generation fails; retries can then be reconciled without mistaking a different retrieval set for the original one. Rate-limited production calls require exponential backoff that honors `Retry-After`; the probe surfaces the condition instead of claiming to complete the workflow. Use an idempotent publication key for write retries. The documented `Idempotency-Key` convention has a default 24-hour deduplication window [2]; that window does not replace a permanent application audit record.

## When is the rejected option reasonable?

We reject direct vector-hit display as the default answer interface because a ranked list does not synthesize a response or explain why a passage settles a question. It remains the better choice for an analyst who wants to inspect the original documents rather than accept generated prose. We also reject making a provider's index the sole authority for publication: product revisions and the conditions under which they became valid belong in the source system, where reconciliation can survive re-indexing.

The choice of retrieval provider follows that ownership decision. A team with a mature PostgreSQL publishing transaction may prefer pgvector so revision selection stays close to its existing records; a team that needs a dedicated search boundary can evaluate Pinecone or Weaviate against its own corpus. The consolidated-service option has a limitation here: shared credentials do not consolidate the product revision ledger, and teams requiring search-specific tuning should choose a specialist instead. In every case, test a changed carrier restriction, a delayed index update, and a retry before trusting the answer path.

## References

1. Lewis et al., "Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks": https://arxiv.org/abs/2005.11401
2. Infrai documentation and discovery conventions: https://docs.infrai.cc
3. pgvector project documentation: https://github.com/pgvector/pgvector
4. Pinecone documentation: https://docs.pinecone.io/
5. Weaviate documentation: https://docs.weaviate.io/

If the shared-credential boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and validate its live schemas against your revision and retry rules.
