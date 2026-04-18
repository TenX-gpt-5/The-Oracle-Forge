# Injection Test Protocol

**Injection Test Evidence:**
*   **Test Query:** "What is the injection test protocol and what does it guard against?"
*   **Expected Answer:** "Each KB document is tested in isolation in a fresh LLM session. If the LLM hallucinates outside info or fails to answer from the document alone, the document is revised. The discipline is removal, not accumulation."

The absolute discipline for the Knowledge Base is **removal, not accumulation**. Do not add any new document to the KB without running this test. Growth without removal becomes noise.

**Protocol:**
1. Take the exact text of the proposed KB document.
2. Start a fresh, clean LLM session and pass **ONLY** this new document as the system prompt/context. Do NOT pass any other KB docs.
3. Ask a specific technical question that the document is supposed to answer.
4. Grade the result: if the answer is correct, direct, and complete based ONLY on what was in the doc, the document passes. If the LLM hallucinates outside info, relies on external pretrained knowledge, or fails to answer directly, revise the document.

**Example: Testing `join_keys.md`**
*Question:* "How is CustomerID formatted in the Yelp PostgreSQL database versus the MongoDB CRM collection?"
*Expected Return:* "In PostgreSQL it is an integer (e.g., 12345). In MongoDB it is prefixed with 'CUST-' (e.g., 'CUST-12345')."

## Injection Test
**Q:** "What is the injection test protocol and what does it guard against?"
**Expected:** "Each KB document is tested in isolation in a fresh LLM session. If the LLM hallucinates outside info or fails to answer from the document alone, the document is revised. The discipline is removal, not accumulation."
**Result:** PASS
**Date:** 2026-04-18
