"""
RAG Query System
AI Engineer 2 - Week 1 Deliverable (Enhanced)

Retrieval-Augmented Generation query interface for banking policies.
Features:
- Semantic search across policy documents
- Grounded answer generation (hallucination prevention)
- Citation tracking for transparency
- Confidence scoring

Author: AI Engineer 2 (Security & Knowledge Specialist)
"""

from typing import List, Dict, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor
from chromadb_config import initialize_chromadb, ChromaDBConfig

# ----------------------------
# CONFIGURABLE CONSTANTS
# ----------------------------
DEFAULT_TOP_K = 5
RELEVANCE_THRESHOLD = 0.5
HIGH_CONFIDENCE_THRESHOLD = 0.75
MAX_SNIPPET_LENGTH = 200
MAX_ANSWER_WORDS = 500
BATCH_THREAD_COUNT = 4

# ----------------------------
# LOGGING SETUP
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RAGQueryEngine:
    """
    RAG-powered query engine for banking policy questions.
    """

    def __init__(self, client, config: ChromaDBConfig):
        self.client = client
        self.config = config

        # Load collections
        self.policy_collection = config.get_or_create_collection(client, config.COLLECTION_POLICIES)
        self.faq_collection = config.get_or_create_collection(client, config.COLLECTION_FAQS)
        self.all_collection = config.get_or_create_collection(client, config.COLLECTION_ALL)

        logger.info("RAG Query Engine initialized successfully")

    # ----------------------------
    # CORE QUERY FUNCTION
    # ----------------------------
    def query(
        self,
        question: str,
        collection_name: Optional[str] = None,
        top_k: int = DEFAULT_TOP_K,
        include_metadata: bool = True
    ) -> Dict:
        """
        Query the knowledge base using semantic similarity.
        """
        logger.info(f"Query received: {question[:100]}...")

        # Select collection
        collection = self.client.get_collection(collection_name) if collection_name else self.all_collection

        # Perform semantic search safely
        try:
            results = collection.query(
                query_texts=[question],
                n_results=top_k,
                include=['documents', 'metadatas', 'distances']
            )
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return {
                'answer': None,
                'sources': [],
                'confidence': 0.0,
                'grounded': False,
                'message': "An error occurred during query execution."
            }

        # Check if results exist
        if not results['documents'] or not results['documents'][0]:
            logger.warning(f"No relevant documents found for query: {question}")
            return {
                'answer': None,
                'sources': [],
                'confidence': 0.0,
                'grounded': False,
                'message': "No relevant information found in the knowledge base."
            }

        # Extract chunks and compute similarity
        retrieved_chunks = []
        for i in range(len(results['documents'][0])):
            distance = results['distances'][0][i]
            similarity = max(0.0, min(1.0, 1 - distance))  # ensure similarity is 0-1
            chunk = {
                'content': results['documents'][0][i],
                'metadata': results['metadatas'][0][i] if include_metadata else {},
                'distance': distance,
                'similarity': similarity
            }
            retrieved_chunks.append(chunk)

        # Filter by relevance
        relevant_chunks = [c for c in retrieved_chunks if c['similarity'] >= RELEVANCE_THRESHOLD]
        if not relevant_chunks:
            logger.warning(f"No chunks above relevance threshold for: {question}")
            return {
                'answer': None,
                'sources': [],
                'confidence': 0.0,
                'grounded': False,
                'message': "The retrieved information is not relevant enough."
            }

        # Calculate confidence
        avg_similarity = sum(c['similarity'] for c in relevant_chunks) / len(relevant_chunks)
        confidence = min(avg_similarity, 1.0)

        # Synthesize answer
        answer = self._synthesize_answer(question, relevant_chunks)

        # Prepare citations
        sources = self._prepare_citations(relevant_chunks)

        result = {
            'answer': answer,
            'sources': sources,
            'confidence': round(confidence, 3),
            'grounded': True,
            'retrieved_chunks': len(relevant_chunks),
            'question': question
        }

        logger.info(f"Query answered with confidence: {confidence:.3f}")
        return result

    # ----------------------------
    # ANSWER SYNTHESIS
    # ----------------------------
    def _synthesize_answer(self, question: str, chunks: List[Dict]) -> str:
        """
        Extractive summarization of retrieved chunks.
        Uses relevance-weighted selection of best paragraph from multiple sources.
        """
        question_words = set(question.lower().split())
        answer_parts = []

        for chunk in chunks:
            paragraphs = [p.strip() for p in chunk['content'].split('\n\n') if len(p.strip()) > 50]
            best_paragraph = max(
                paragraphs,
                key=lambda p: len(question_words & set(p.lower().split())),
                default=None
            )
            if best_paragraph and best_paragraph not in answer_parts:
                answer_parts.append(best_paragraph)

        # Truncate answer for readability
        answer = "\n\n".join(answer_parts)
        words = answer.split()
        if len(words) > MAX_ANSWER_WORDS:
            answer = ' '.join(words[:MAX_ANSWER_WORDS]) + "... [Additional details in sources]"

        return answer

    # ----------------------------
    # CITATION HANDLING
    # ----------------------------
    def _prepare_citations(self, chunks: List[Dict]) -> List[Dict]:
        citations = []
        for rank, chunk in enumerate(chunks, 1):
            metadata = chunk['metadata']
            snippet = chunk['content'][:MAX_SNIPPET_LENGTH]
            if len(chunk['content']) > MAX_SNIPPET_LENGTH:
                snippet += "..."
            citations.append({
                'rank': rank,
                'source_document': metadata.get('source_document', 'Unknown'),
                'section': metadata.get('section_title', 'N/A'),
                'document_type': metadata.get('document_type', 'Unknown'),
                'similarity_score': round(chunk['similarity'], 3),
                'snippet': snippet
            })
        return citations

    # ----------------------------
    # CONTEXTUAL QUERY
    # ----------------------------
    def query_with_context(self, question: str, context: Optional[str] = None, top_k: int = DEFAULT_TOP_K) -> Dict:
        enhanced_query = f"{context}\n\nCurrent question: {question}" if context else question
        return self.query(enhanced_query, top_k=top_k)

    # ----------------------------
    # BATCH QUERY
    # ----------------------------
    def multi_query(self, questions: List[str], top_k: int = 3) -> List[Dict]:
        results = []
        with ThreadPoolExecutor(max_workers=BATCH_THREAD_COUNT) as executor:
            futures = [executor.submit(self.query, q, top_k=top_k) for q in questions]
            for future in futures:
                results.append(future.result())
        logger.info(f"Processed {len(questions)} queries in batch")
        return results

    # ----------------------------
    # GROUNDING CHECK
    # ----------------------------
    def check_grounding(self, statement: str, top_k: int = 3) -> Dict:
        result = self.query(statement, top_k=top_k)
        return {
            'statement': statement,
            'is_grounded': result['grounded'],
            'confidence': result['confidence'],
            'supporting_evidence': result['sources'] if result['grounded'] else [],
            'verdict': (
                'SUPPORTED' if result['confidence'] > HIGH_CONFIDENCE_THRESHOLD else
                'PARTIALLY_SUPPORTED' if result['grounded'] else
                'NOT_SUPPORTED'
            )
        }

    # ----------------------------
    # COLLECTION INFO
    # ----------------------------
    def get_collection_info(self) -> Dict:
        return {
            'policies': {'count': self.policy_collection.count(), 'name': self.config.COLLECTION_POLICIES},
            'faqs': {'count': self.faq_collection.count(), 'name': self.config.COLLECTION_FAQS},
            'all_documents': {'count': self.all_collection.count(), 'name': self.config.COLLECTION_ALL}
        }


# ----------------------------
# DEMO FUNCTIONS
# ----------------------------
def interactive_query_demo():
    print("\n" + "="*70)
    print(" "*15 + "RAG QUERY SYSTEM - INTERACTIVE DEMO")
    print("="*70 + "\n")

    client, config = initialize_chromadb()
    engine = RAGQueryEngine(client, config)
    print("✓ Query engine ready\n")

    info = engine.get_collection_info()
    print("Knowledge Base Statistics:")
    for name, stats in info.items():
        print(f"  {name}: {stats['count']} chunks")
    print()

    while True:
        try:
            question = input("\nYour question: ").strip()
            if not question:
                continue
            if question.lower() in ['quit', 'exit', 'q']:
                print("\nGoodbye!")
                break

            print("\nSearching knowledge base...")
            result = engine.query(question)

            print("\n" + "="*70)
            print(f"CONFIDENCE: {result['confidence']:.1%}")
            print(f"GROUNDED: {'✓ Yes' if result['grounded'] else '✗ No'}")
            print("="*70)

            if result['answer']:
                print("\nANSWER:\n", result['answer'])
                print("\n" + "-"*70)
                print(f"SOURCES ({len(result['sources'])} documents):")
                for src in result['sources']:
                    print(f"\n  [{src['rank']}] {src['source_document']}")
                    print(f"      Section: {src['section']}")
                    print(f"      Relevance: {src['similarity_score']:.1%}")
                    print(f"      Snippet: {src['snippet']}")
            else:
                print("\nNO ANSWER FOUND")
                print(result.get('message', 'Information not available'))

            print("-"*70)

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {e}")
            logger.error(f"Query error: {e}")


def run_test_queries():
    client, config = initialize_chromadb()
    engine = RAGQueryEngine(client, config)

    test_questions = [
        "What is the SLA for transaction disputes?",
        "How are fraud cases handled?",
        "What department handles card retention issues?",
        "What is the daily limit for Tier 2 accounts?",
        "How long does it take to reverse a failed NIP transfer?",
        "What should I do if my ATM card is swallowed?",
        "Can you reverse a transfer sent to the wrong account?",
        "What are the red flags for fraudulent transactions?",
        "How do I enable international transactions on my card?",
        "What is the process for escalating a complaint?"
    ]

    print(f"\nRunning {len(test_questions)} test queries...\n")
    results = engine.multi_query(test_questions, top_k=3)

    answered = sum(1 for r in results if r['answer'])
    high_confidence = sum(1 for r in results if r['confidence'] > 0.75)
    avg_confidence = sum(r['confidence'] for r in results) / len(results)

    print("\nTEST RESULTS SUMMARY")
    print("="*70)
    print(f"Questions answered: {answered}/{len(test_questions)} ({answered/len(test_questions):.1%})")
    print(f"High confidence answers (>75%): {high_confidence}/{answered}")
    print(f"Average confidence: {avg_confidence:.1%}")
    print("="*70)


# ----------------------------
# MAIN EXECUTION
# ----------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        run_test_queries()
    else:
        interactive_query_demo()
