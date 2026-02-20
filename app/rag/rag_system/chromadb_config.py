"""
ChromaDB Configuration & Setup
AI Engineer 2 - Week 1 Deliverable

Configures the vector database for banking policy documents.
This module handles:
- ChromaDB client initialization
- Collection creation and management
- Embedding model configuration
- Persistence settings

CHANGELOG v2:
  - Added DOCUMENT_REGISTRY: maps all 6 document IDs to category,
    agent_target, version, title, and doc_type_flag — consumed by
    ingest_documents.py to tag every chunk with routing metadata,
    enabling per-agent filtered retrieval in rag_query.py
  - Added reset_all_collections(): wipes all three collections in one
    call, used by ingest_documents.py before a full re-ingestion run
  - Added document_count to default collection metadata

Author: AI Engineer 2 (Security & Knowledge Specialist)
Date: February 2026
"""

import chromadb
from chromadb.utils import embedding_functions
from pathlib import Path
from typing import Optional, Dict         # ← added Dict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# DOCUMENT REGISTRY  ← NEW
# =============================================================================
# Maps every document ID to its pipeline metadata.
# Keys MUST match:
#   - document_id values from policy_generator._package_for_rag()
#   - .txt filenames produced by BankingPolicyGenerator.save_all_policies()
#
# Consumed by ingest_documents.py so every ChromaDB chunk is tagged with:
#   category, agent_target, version, title, doc_type_flag
#
# doc_type_flag drives collection routing in the ingester:
#   "policy"         → COLLECTION_POLICIES  (+ COLLECTION_ALL)
#   "knowledge_base" → COLLECTION_FAQS      (+ COLLECTION_ALL)
#
# agent_target enables per-agent filtered retrieval in rag_query.py, e.g.:
#   collection.query(..., where={"agent_target": "Sentinel"})

DOCUMENT_REGISTRY: Dict[str, Dict] = {
    "POL-CCH-001": {
        "title":         "Customer Complaint Handling Policy",
        "category":      "policy",
        "version":       "2.1",
        "agent_target":  "Dispatcher",
        "doc_type_flag": "policy",
        "description":   "Department routing rules, SLA hours, escalation matrix",
    },
    "FRM-001": {
        "title":         "Fraud Detection & Prevention Guidelines",
        "category":      "security",
        "version":       "4.0",
        "agent_target":  "Sentinel",
        "doc_type_flag": "policy",
        "description":   "Red flags, risk scoring 0–100, push-to-app authorization",
    },
    "TSU-POL-002": {
        "title":         "Transaction Processing Policies",
        "category":      "operations",
        "version":       "4.0",
        "agent_target":  "All",
        "doc_type_flag": "policy",
        "description":   "KYC tier limits, fee schedule, reversal policies",
    },
    "FAQ-001": {
        "title":         "Customer Service Frequently Asked Questions",
        "category":      "knowledge_base",
        "version":       "2.0",
        "agent_target":  "Customer",
        "doc_type_flag": "knowledge_base",
        "description":   "Customer-facing self-service answers",
    },
    "FRM-002": {
        "title":         "Merchant Risk Profiles",
        "category":      "security",
        "version":       "1.0",
        "agent_target":  "Sentinel",
        "doc_type_flag": "policy",
        "description":   "Per-category merchant risk weights and velocity rules",
    },
    "PRS-001": {
        "title":         "Product Recommendation Policy",
        "category":      "policy",
        "version":       "1.0",
        "agent_target":  "Trajectory",
        "doc_type_flag": "policy",
        "description":   "Car Loan / Personal Loan / Investment Plan eligibility",
    },
}


class ChromaDBConfig:
    """
    Centralized configuration for ChromaDB vector database.
    
    This class manages:
    - Database persistence location
    - Embedding model selection and configuration
    - Collection naming conventions
    - Client settings and optimization
    """
    
    # Collection names (organized by document type)
    COLLECTION_POLICIES = "bank_policies"
    COLLECTION_FAQS = "customer_faqs"
    COLLECTION_ALL = "all_documents"  # Combined collection for cross-document search
    
    # Embedding model configuration
    EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # Sentence-transformers model
    # Alternative models:
    # - "all-mpnet-base-v2" (higher quality, slower)
    # - "paraphrase-multilingual-MiniLM-L12-v2" (multilingual support)
    
    # Distance metrics for similarity search
    DISTANCE_METRIC = "cosine"  # Options: cosine, l2, ip (inner product)
    
    def __init__(self, persist_directory: Optional[Path] = None):
        """
        Initialize ChromaDB configuration.
        
        Args:
            persist_directory: Where to store the vector database
                              (defaults to ./chroma_db/)
        """
        if persist_directory is None:
            # Default to chroma_db folder in project root
            persist_directory = Path(__file__).parent.parent / "chroma_db"
        
        self.persist_directory = Path(persist_directory)
        self.persist_directory.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"ChromaDB persistence directory: {self.persist_directory}")
    
    def get_embedding_function(self):
        """
        Get configured embedding function for text-to-vector conversion.
        
        Returns:
            Embedding function compatible with ChromaDB
        """
        # Using sentence-transformers for high-quality embeddings
        embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=self.EMBEDDING_MODEL
        )
        
        logger.info(f"Loaded embedding model: {self.EMBEDDING_MODEL}")
        return embedding_function
    
    def create_client(self) -> chromadb.Client:
        """
        Create and configure ChromaDB persistent client.
        
        Returns:
            Configured ChromaDB client
        """
        # Ensure the persistence directory exists
        self.persist_directory.mkdir(parents=True, exist_ok=True)

        # Create persistent client (data survives Python process restarts)
        client = chromadb.PersistentClient(
            path=str(self.persist_directory)
        )

        logger.info(f"ChromaDB persistent client created at: {self.persist_directory}")

        return client

    def get_or_create_collection(self, 
                                 client: chromadb.Client, 
                                 collection_name: str,
                                 metadata: Optional[dict] = None):
        """
        Get existing collection or create new one with proper configuration.
        
        Args:
            client: ChromaDB client instance
            collection_name: Name of the collection
            metadata: Optional metadata about the collection
            
        Returns:
            ChromaDB collection object
        """
        # Get embedding function
        embedding_function = self.get_embedding_function()
        
        # Default metadata
        if metadata is None:
            metadata = {
                "description":     f"Banking documents collection: {collection_name}",
                "embedding_model": self.EMBEDDING_MODEL,
                "distance_metric": self.DISTANCE_METRIC,
                "document_count":  str(len(DOCUMENT_REGISTRY)),  # ← NEW
            }
        
        try:
            # Try to get existing collection
            collection = client.get_collection(
                name=collection_name,
                embedding_function=embedding_function
            )
            logger.info(f"Retrieved existing collection: {collection_name}")
            
        except Exception:
            # Collection doesn't exist, create it
            collection = client.create_collection(
                name=collection_name,
                embedding_function=embedding_function,
                metadata=metadata
            )
            logger.info(f"Created new collection: {collection_name}")
        
        return collection
    
    def reset_collection(self, client: chromadb.Client, collection_name: str):
        """
        Delete and recreate a collection (useful for fresh ingestion).
        
        Args:
            client: ChromaDB client instance
            collection_name: Name of collection to reset
        """
        try:
            client.delete_collection(name=collection_name)
            logger.info(f"Deleted collection: {collection_name}")
        except Exception as e:
            logger.warning(f"Collection {collection_name} didn't exist: {e}")
        
        # Recreate the collection
        self.get_or_create_collection(client, collection_name)
        logger.info(f"Reset collection: {collection_name}")

    def reset_all_collections(self, client: chromadb.Client):  # ← NEW
        """
        Reset all three collections in one call.

        Call this before a full re-ingestion run to prevent duplicate chunks
        accumulating across multiple ingest_documents.py executions.

        Args:
            client: ChromaDB client instance
        """
        for name in [self.COLLECTION_POLICIES,
                     self.COLLECTION_FAQS,
                     self.COLLECTION_ALL]:
            self.reset_collection(client, name)
        logger.info("All collections reset and ready for fresh ingestion")


def initialize_chromadb() -> tuple:
    """
    Helper function to quickly initialize ChromaDB with default settings.
    
    Returns:
        Tuple of (client, config) for use in other modules
        
    Example:
        >>> client, config = initialize_chromadb()
        >>> collection = config.get_or_create_collection(
        ...     client, 
        ...     config.COLLECTION_POLICIES
        ... )
    """
    config = ChromaDBConfig()
    client = config.create_client()
    
    return client, config


def get_collection_stats(collection) -> dict:
    """
    Get statistics about a ChromaDB collection.
    
    Args:
        collection: ChromaDB collection object
        
    Returns:
        Dictionary with collection statistics
    """
    try:
        count = collection.count()
        
        # Get sample metadata if collection not empty
        if count > 0:
            sample = collection.peek(limit=1)
            sample_metadata = sample['metadatas'][0] if sample['metadatas'] else {}
        else:
            sample_metadata = {}
        
        stats = {
            "name":            collection.name,
            "total_documents": count,
            "metadata":        collection.metadata,
            "sample_metadata": sample_metadata
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")
        return {"error": str(e)}


if __name__ == "__main__":
    """Test ChromaDB configuration and connectivity"""
    
    print("\n" + "="*70)
    print(" "*20 + "CHROMADB CONFIGURATION TEST")
    print("="*70 + "\n")
    
    # Initialize ChromaDB
    print("Initializing ChromaDB...")
    client, config = initialize_chromadb()
    print(f" Client created with persistence at: {config.persist_directory}\n")

    # Display document registry  ← NEW
    print(f"Document Registry ({len(DOCUMENT_REGISTRY)} documents):")
    for doc_id, meta in DOCUMENT_REGISTRY.items():
        print(f"  {doc_id:<14}  category={meta['category']:<14}  "
              f"agent={meta['agent_target']:<12}  v{meta['version']}")
    print()

    # Test collection creation
    print("Testing collection creation...")
    test_collection = config.get_or_create_collection(
        client,
        "test_collection",
        metadata={"test": "This is a test collection"}
    )
    print(f" Collection '{test_collection.name}' ready\n")
    
    # Test embedding function
    print("Testing embedding function...")
    embedding_fn = config.get_embedding_function()
    test_text = "This is a test sentence for embedding"
    embedding = embedding_fn([test_text])[0]
    print(f" Embedding generated (dimension: {len(embedding)})\n")
    
    # Display collection stats
    print("Collection Statistics:")
    stats = get_collection_stats(test_collection)
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Cleanup test collection
    print("\nCleaning up test collection...")
    client.delete_collection("test_collection")
    print(" Test collection deleted")
    
    print("\n" + "="*70)
    print(" "*15 + "CHROMADB CONFIGURATION TEST COMPLETE!")
    print("="*70)
    print("\nChromaDB is ready for document ingestion.")
    print("Next step: Run ingest_documents.py to load all 6 banking policies")
    print("="*70 + "\n")