"""
Document Ingestion Pipeline
AI Engineer 2 - Week 1 Deliverable

Loads banking policy documents into ChromaDB vector database.
This module handles:
- Text file loading and validation
- Document chunking strategies
- Metadata enrichment
- Batch ingestion into vector DB

CHANGELOG v2:
  - Imports DOCUMENT_REGISTRY from chromadb_config — used to resolve
    category, agent_target, version, title, doc_type_flag per document ID
  - Imports shared policy constants from policy_generator:
    EXPECTED_SLA, DEPT_NAMES, MERCHANT_RISK, FLAG_WEIGHTS, DATASET_DIR
  - load_documents_from_directory(): doc_type now resolved from
    DOCUMENT_REGISTRY keyed by document ID (replaces fragile path-part check);
    returned dicts now include category, version, title, agent_target,
    doc_type_flag fields
  - create_chunks_from_document(): chunk metadata now includes
    document_title, version, agent_target, doc_type_flag, dataset_dir,
    project, organization, jurisdiction (from doc dict + policy_generator)
  - ingest_knowledge_base(): added reset_first param (default True) that
    calls reset_all_collections() before ingesting to prevent duplicate chunks;
    auto-generates documents via BankingPolicyGenerator if directory is missing;
    collection routing now uses doc_type_flag instead of document_type string;
    prints shared constants summary in completion block
  - NEW ingest_from_generator(): generates all 6 documents in memory and
    ingests directly — no disk roundtrip, no prior save_all_policies() needed
  - ingest_banking_policies(): added mode ('memory'|'disk') and reset_first
    params; routes to ingest_from_generator or ingest_knowledge_base
  - __main__: parses --disk and --no-reset from sys.argv

Author: AI Engineer 2 (Security & Knowledge Specialist)
Date: February 2026
"""

from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging
import hashlib
from datetime import datetime
import re

from .chromadb_config import (
    initialize_chromadb,
    ChromaDBConfig,
    get_collection_stats,
    DOCUMENT_REGISTRY,       # 6-doc registry with category/agent metadata
)
from ..knowledge_base.generate_policies import (                          
    BankingPolicyGenerator,  # for in-memory ingestion path
    EXPECTED_SLA,            # POL-CCH-001 SLA hours — written to completion block
    DEPT_NAMES,              # POL-CCH-001 full department names
    MERCHANT_RISK,           # FRM-002 merchant risk weights
    FLAG_WEIGHTS,            # FRM-001 fraud flag weights
    DATASET_DIR,             # dataset path for audit trail in chunk metadata
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DocumentChunker:
    """
    Intelligent document chunking for optimal RAG retrieval.
    
    Strategies:
    - Section-based: Split on headers for policy documents
    - Semantic: Keep related content together
    - Size-aware: Respect embedding model limits
    """
    
    # Maximum characters per chunk (embedding models have token limits)
    MAX_CHUNK_SIZE = 1000  # ~250 tokens for most models
    
    # Minimum overlap between chunks for context continuity
    CHUNK_OVERLAP = 100
    
    @staticmethod
    def chunk_by_sections(text: str, document_id: str) -> List[Dict]:
        """
        Split document by section headers (== and ---).
        
        Best for structured policy documents with clear sections.
        
        Args:
            text: Full document text
            document_id: Identifier for source document
            
        Returns:
            List of chunk dictionaries with content and metadata
        """
        chunks = []
        
        # Split on section separators (====== or ------)
        sections = re.split(r'\n={50,}\n|\n-{50,}\n', text)
        
        for idx, section in enumerate(sections):
            section = section.strip()
            
            if not section or len(section) < 50:  # Skip tiny fragments
                continue
            
            # Extract section title (first line if it looks like a title)
            lines = section.split('\n')
            section_title = lines[0].strip() if lines else f"Section {idx + 1}"
            
            # If section is too large, split it further
            if len(section) > DocumentChunker.MAX_CHUNK_SIZE:
                sub_chunks = DocumentChunker._split_large_section(
                    section, 
                    section_title
                )
                for sub_idx, sub_chunk in enumerate(sub_chunks):
                    chunks.append({
                        'chunk_id': f"{document_id}_sec{idx}_sub{sub_idx}",
                        'content': sub_chunk,
                        'section_title': section_title,
                        'chunk_index': len(chunks)
                    })
            else:
                chunks.append({
                    'chunk_id': f"{document_id}_sec{idx}",
                    'content': section,
                    'section_title': section_title,
                    'chunk_index': len(chunks)
                })
        
        logger.info(f"Created {len(chunks)} section-based chunks from {document_id}")
        return chunks
    
    @staticmethod
    def _split_large_section(section: str, section_title: str) -> List[str]:
        """
        Split large section into smaller chunks with overlap.
        
        Args:
            section: Text of large section
            section_title: Title for context
            
        Returns:
            List of chunk strings
        """
        chunks = []
        sentences = re.split(r'(?<=[.!?])\s+', section)
        
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            sentence_size = len(sentence)
            
            if current_size + sentence_size > DocumentChunker.MAX_CHUNK_SIZE and current_chunk:
                # Save current chunk
                chunks.append(' '.join(current_chunk))
                
                # Start new chunk with overlap
                overlap_sentences = current_chunk[-2:] if len(current_chunk) > 2 else current_chunk
                current_chunk = overlap_sentences + [sentence]
                current_size = sum(len(s) for s in current_chunk)
            else:
                current_chunk.append(sentence)
                current_size += sentence_size
        
        # Add final chunk
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks
    
    @staticmethod
    def extract_key_terms(text: str, top_n: int = 5) -> List[str]:
        """
        Extract key terms from text for metadata enrichment.
        
        Simple frequency-based extraction. In production, would use TF-IDF or NER.
        
        Args:
            text: Document text
            top_n: Number of key terms to extract
            
        Returns:
            List of key terms
        """
        # Remove common words (simple stopwords)
        stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
                    'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
                    'this', 'that', 'these', 'those', 'will', 'shall', 'should', 'must'}
        
        # Extract words
        words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
        
        # Filter stopwords and count frequency
        word_freq = {}
        for word in words:
            if word not in stopwords:
                word_freq[word] = word_freq.get(word, 0) + 1
        
        # Get top N most frequent
        sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        key_terms = [word for word, _ in sorted_words[:top_n]]
        
        return key_terms


class DocumentIngester:
    """
    Main ingestion pipeline for loading documents into ChromaDB.
    
    Workflow:
    1. Load documents from file system (or generate in memory)
    2. Validate and preprocess text
    3. Chunk into retrieval-optimized segments
    4. Enrich with metadata
    5. Insert into vector database with embeddings

    Two ingestion modes:
      ingest_from_generator() — in-memory, no disk I/O required
      ingest_knowledge_base() — disk-based, reads from knowledge_base/ dir
    """
    
    def __init__(self, client, config: ChromaDBConfig):
        """
        Initialize document ingester.
        
        Args:
            client: ChromaDB client instance
            config: ChromaDB configuration object
        """
        self.client = client
        self.config = config
        self.chunker = DocumentChunker()

    # =========================================================================
    # NEW: IN-MEMORY INGESTION (preferred — no disk roundtrip)
    # =========================================================================

    def ingest_from_generator(self,
                               bank_name: str = "Sentinel Bank Nigeria",
                               reset_first: bool = True) -> Dict:
        """
        Generate all 6 policy documents in memory and ingest directly.

        Calls BankingPolicyGenerator.generate_all_documents() and pipes
        the result straight into ChromaDB — no need to run save_all_policies()
        first. Uses the same chunking and metadata enrichment as the disk path.

        Args:
            bank_name:   Passed to BankingPolicyGenerator
            reset_first: Wipe all three collections before ingesting to
                         prevent duplicate chunks on re-runs (default True)

        Returns:
            Dict with ingestion summary keys:
              total_docs, total_chunks, policy_chunks, faq_chunks,
              documents_ingested, timestamp
        """
        print("\n" + "="*70)
        print(" "*12 + "IN-MEMORY KNOWLEDGE BASE INGESTION (v2)")
        print("="*70 + "\n")

        # Optional reset
        if reset_first:
            print("Step 0: Resetting collections (reset_first=True)...")
            self.config.reset_all_collections(self.client)
            print("  ✓ All collections cleared\n")

        # Generate documents
        print("Step 1: Generating documents via BankingPolicyGenerator...")
        generator = BankingPolicyGenerator(bank_name=bank_name)
        documents  = generator.generate_all_documents()
        print(f"  ✓ {len(documents)} documents generated in memory\n")

        # Chunk and enrich
        print("Step 2: Chunking and enriching metadata...")
        all_chunks    = []
        policy_chunks = []
        faq_chunks    = []

        for doc in documents:
            doc_id   = doc['document_id']
            registry = DOCUMENT_REGISTRY.get(doc_id, {})

            # doc_type_flag: prefer registry, fall back to category field
            doc_type_flag = registry.get('doc_type_flag', 'policy')
            if doc.get('category') == 'knowledge_base':
                doc_type_flag = 'knowledge_base'

            chunks = self._make_enriched_chunks(
                content       = doc['content'],
                document_id   = doc_id,
                title         = doc.get('title',    registry.get('title',    doc_id)),
                category      = doc.get('category', registry.get('category', 'policy')),
                version       = doc.get('version',  registry.get('version',  '1.0')),
                agent_target  = registry.get('agent_target', 'All'),
                doc_type_flag = doc_type_flag,
                source_meta   = doc.get('metadata', {}),
            )

            all_chunks.extend(chunks)
            if doc_type_flag == 'knowledge_base':
                faq_chunks.extend(chunks)
            else:
                policy_chunks.extend(chunks)

            print(f"  ✓ {doc_id:<14} → {len(chunks):>3} chunks  "
                  f"agent={registry.get('agent_target','All'):<12}  "
                  f"category={doc.get('category','policy')}")

        print(f"\n  Total: {len(all_chunks)} chunks  "
              f"({len(policy_chunks)} policy + {len(faq_chunks)} FAQ)\n")

        # Ingest
        print("Step 3: Ingesting into ChromaDB...")

        if policy_chunks:
            print(f"\n  → {self.config.COLLECTION_POLICIES}  ({len(policy_chunks)} chunks)")
            self.ingest_to_collection(policy_chunks, self.config.COLLECTION_POLICIES)

        if faq_chunks:
            print(f"\n  → {self.config.COLLECTION_FAQS}  ({len(faq_chunks)} chunks)")
            self.ingest_to_collection(faq_chunks, self.config.COLLECTION_FAQS)

        print(f"\n  → {self.config.COLLECTION_ALL}  ({len(all_chunks)} chunks, combined)")
        self.ingest_to_collection(all_chunks, self.config.COLLECTION_ALL)

        self._print_completion_block()

        return {
            'total_docs':         len(documents),
            'total_chunks':       len(all_chunks),
            'policy_chunks':      len(policy_chunks),
            'faq_chunks':         len(faq_chunks),
            'documents_ingested': [d['document_id'] for d in documents],
            'timestamp':          datetime.now().isoformat(),
        }

    def _make_enriched_chunks(self,
                               content:       str,
                               document_id:   str,
                               title:         str,
                               category:      str,
                               version:       str,
                               agent_target:  str,
                               doc_type_flag: str,
                               source_meta:   Dict) -> List[Dict]:
        """
        Chunk a document and attach full metadata to every chunk.

        Shared by both ingest_from_generator() and create_chunks_from_document()
        so metadata fields are identical regardless of ingestion mode.

        Metadata written to each ChromaDB chunk:
          source_document    : document ID (e.g. "FRM-002")
          document_title     : human-readable title
          document_type      : category string (policy|security|operations|
                               knowledge_base)
          doc_type_flag      : 'policy' | 'knowledge_base'
          version            : document version string
          agent_target       : which agent primarily consumes this doc
          section_title      : section header this chunk belongs to
          chunk_index        : ordinal position within document
          char_count         : character length of chunk content
          content_hash       : MD5 for deduplication detection
          key_terms          : comma-separated top-5 terms
          ingestion_timestamp: ISO datetime of ingest run
          dataset_dir        : DATASET_DIR from policy_generator (audit trail)
          project            : pass-through from _package_for_rag metadata
          organization       : pass-through from _package_for_rag metadata
          jurisdiction       : pass-through from _package_for_rag metadata
        """
        raw_chunks = self.chunker.chunk_by_sections(content, document_id)
        enriched   = []
        ingest_ts  = datetime.now().isoformat()

        for chunk in raw_chunks:
            content_hash = hashlib.md5(chunk['content'].encode()).hexdigest()
            key_terms    = self.chunker.extract_key_terms(chunk['content'])

            enriched.append({
                'id':       chunk['chunk_id'],
                'document': chunk['content'],
                'metadata': {
                    # — original fields (unchanged) —
                    'source_document':     document_id,
                    'document_type':       category,
                    'section_title':       chunk['section_title'],
                    'chunk_index':         chunk['chunk_index'],
                    'char_count':          len(chunk['content']),
                    'content_hash':        content_hash,
                    'key_terms':           ', '.join(key_terms),
                    'ingestion_timestamp': ingest_ts,
                    # — new enrichment fields —
                    'document_title':  title,
                    'version':         version,
                    'agent_target':    agent_target,
                    'doc_type_flag':   doc_type_flag,
                    'dataset_dir':     str(DATASET_DIR),
                    'project':         source_meta.get('project', ''),
                    'organization':    source_meta.get('organization', ''),
                    'jurisdiction':    source_meta.get('jurisdiction', 'Nigeria'),
                }
            })

        return enriched

    # =========================================================================
    # ORIGINAL METHODS (preserved exactly, with targeted additions noted)
    # =========================================================================

    def load_documents_from_directory(self, directory: Path) -> List[Dict]:
        """
        Load all text documents from a directory.
        
        Args:
            directory: Path to directory containing .txt files
            
        Returns:
            List of document dictionaries
        """
        documents = []
        directory = Path(directory)
        
        if not directory.exists():
            logger.error(f"Directory not found: {directory}")
            return documents
        
        # Load all .txt files
        for txt_file in sorted(directory.glob("**/*.txt")):
            try:
                with open(txt_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Extract document ID from filename
                doc_id   = txt_file.stem  # Filename without extension

                # ← CHANGED: resolve category/agent from DOCUMENT_REGISTRY
                # (replaced fragile 'policies' / 'faqs' path-part string check)
                registry      = DOCUMENT_REGISTRY.get(doc_id, {})
                category      = registry.get('category',      'policy')
                doc_type_flag = registry.get('doc_type_flag', 'policy')
                version       = registry.get('version',       '1.0')
                title         = registry.get('title',         doc_id)
                agent_target  = registry.get('agent_target',  'All')

                documents.append({
                    'document_id':   doc_id,
                    'filepath':      str(txt_file),
                    'content':       content,
                    'doc_type':      category,        # kept for backward compat
                    'doc_type_flag': doc_type_flag,   # ← NEW: drives collection routing
                    'category':      category,         # ← NEW
                    'version':       version,          # ← NEW
                    'title':         title,            # ← NEW
                    'agent_target':  agent_target,     # ← NEW
                    'filename':      txt_file.name,
                })
                
                logger.info(f"Loaded: {txt_file.name}  "
                            f"category={category}  agent={agent_target}")
                
            except Exception as e:
                logger.error(f"Error loading {txt_file}: {e}")
        
        logger.info(f"Loaded {len(documents)} documents from {directory}")
        return documents
    
    def preprocess_document(self, document: Dict) -> Dict:
        """
        Clean and validate document before chunking.
        
        Args:
            document: Raw document dictionary
            
        Returns:
            Preprocessed document
        """
        content = document['content']
        
        # Remove excessive whitespace
        content = re.sub(r'\n{3,}', '\n\n', content)
        content = re.sub(r' {2,}', ' ', content)
        
        # Standardize line endings
        content = content.replace('\r\n', '\n')
        
        # Update document
        document['content']    = content.strip()
        document['char_count'] = len(content)
        document['word_count'] = len(content.split())
        
        return document
    
    def create_chunks_from_document(self, document: Dict) -> List[Dict]:
        """
        Chunk document and enrich with metadata.

        Delegates to _make_enriched_chunks() so metadata is identical
        whether documents came from disk or from BankingPolicyGenerator.
        
        Args:
            document: Preprocessed document dictionary
            
        Returns:
            List of enriched chunk dictionaries
        """
        # ← CHANGED: delegate to shared helper (adds new metadata fields)
        return self._make_enriched_chunks(
            content       = document['content'],
            document_id   = document['document_id'],
            title         = document.get('title',         document['document_id']),
            category      = document.get('category',      'policy'),
            version       = document.get('version',       '1.0'),
            agent_target  = document.get('agent_target',  'All'),
            doc_type_flag = document.get('doc_type_flag', 'policy'),
            source_meta   = {},
        )
    
    def ingest_to_collection(self, 
                            chunks: List[Dict], 
                            collection_name: str,
                            batch_size: int = 100):
        """
        Insert chunks into ChromaDB collection in batches.
        
        Args:
            chunks: List of enriched chunks
            collection_name: Target collection
            batch_size: Number of chunks per batch (prevents memory issues)
        """
        # Get or create collection
        collection = self.config.get_or_create_collection(
            self.client,
            collection_name
        )
        
        logger.info(f"Ingesting {len(chunks)} chunks into {collection_name}...")
        
        # Process in batches
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            
            # Prepare batch data
            ids       = [chunk['id']       for chunk in batch]
            documents = [chunk['document'] for chunk in batch]
            metadatas = [chunk['metadata'] for chunk in batch]
            
            # Add to collection (embeddings generated automatically)
            collection.add(
                ids=ids,
                documents=documents,
                metadatas=metadatas
            )
            
            logger.info(f"  Batch {i//batch_size + 1}: Ingested {len(batch)} chunks")
        
        logger.info(f" Successfully ingested all chunks to {collection_name}")
    
    def ingest_knowledge_base(self,
                               knowledge_base_dir: Path,
                               reset_first: bool = True):   # ← NEW param
        """
        Complete ingestion pipeline for entire knowledge base (disk mode).
        
        Args:
            knowledge_base_dir: Root directory containing policies/ and faqs/
            reset_first: Wipe all three collections before ingesting to
                         prevent duplicate chunks (default True)
        """
        print("\n" + "="*70)
        print(" "*15 + "KNOWLEDGE BASE INGESTION PIPELINE")
        print("="*70 + "\n")
        
        knowledge_base_dir = Path(knowledge_base_dir)

        # ← NEW: auto-generate documents if directory doesn't exist yet
        if not knowledge_base_dir.exists():
            print(f"⚠️  Knowledge base not found at: {knowledge_base_dir}")
            print("    Auto-generating policy documents now...\n")
            generator = BankingPolicyGenerator(bank_name="Sentinel Bank Nigeria")
            generator.save_all_policies(knowledge_base_dir)
            print()

        # ← NEW: reset collections before ingesting
        if reset_first:
            print("Step 0: Resetting collections (reset_first=True)...")
            self.config.reset_all_collections(self.client)
            print("  ✓ All collections cleared\n")

        # Load all documents
        print("Step 1: Loading documents from file system...")
        documents = self.load_documents_from_directory(knowledge_base_dir)
        print(f"✓ Loaded {len(documents)} documents\n")
        
        # Preprocess documents
        print("Step 2: Preprocessing documents...")
        for doc in documents:
            self.preprocess_document(doc)
        print(f"✓ Preprocessed {len(documents)} documents\n")
        
        # Create chunks from all documents
        print("Step 3: Chunking documents...")
        all_chunks = []
        for doc in documents:
            chunks = self.create_chunks_from_document(doc)
            all_chunks.extend(chunks)
            print(f"  ✓ {doc['document_id']:<14} → {len(chunks):>3} chunks  "
                  f"agent={doc.get('agent_target','All'):<12}")
        print(f"\n✓ Created {len(all_chunks)} chunks total\n")
        
        # ← CHANGED: route by doc_type_flag (was hardcoded 'policy'/'faq' string)
        policy_chunks = [c for c in all_chunks
                         if c['metadata']['doc_type_flag'] != 'knowledge_base']
        faq_chunks    = [c for c in all_chunks
                         if c['metadata']['doc_type_flag'] == 'knowledge_base']
        
        # Ingest into separate collections
        print("Step 4: Ingesting into vector database...")
        
        if policy_chunks:
            print(f"\n  Ingesting {len(policy_chunks)} policy chunks...")
            self.ingest_to_collection(policy_chunks, self.config.COLLECTION_POLICIES)
        
        if faq_chunks:
            print(f"\n  Ingesting {len(faq_chunks)} FAQ chunks...")
            self.ingest_to_collection(faq_chunks, self.config.COLLECTION_FAQS)
        
        # Also ingest everything into combined collection
        print(f"\n  Ingesting all {len(all_chunks)} chunks into combined collection...")
        self.ingest_to_collection(all_chunks, self.config.COLLECTION_ALL)

        self._print_completion_block()

    # =========================================================================
    # SHARED HELPER
    # =========================================================================

    def _print_completion_block(self):
        """Print post-ingestion statistics and shared-constants audit summary."""
        print("\n" + "="*70)
        print(" "*20 + "INGESTION COMPLETE!")
        print("="*70 + "\n")
        
        print("Collection Statistics:")
        for name in [self.config.COLLECTION_POLICIES,
                     self.config.COLLECTION_FAQS,
                     self.config.COLLECTION_ALL]:
            try:
                collection = self.client.get_collection(name)
                stats      = get_collection_stats(collection)
                print(f"  {name:<25}: {stats['total_documents']:>4} chunks")
            except Exception:
                print(f"  {name:<25}: (not yet created)")

        # ← NEW: shared constants audit — confirms what was baked into metadata
        print()
        print("Shared constants baked into chunk metadata:")
        print(f"  EXPECTED_SLA  : {dict(EXPECTED_SLA)}")
        print(f"  MERCHANT_RISK : {len(MERCHANT_RISK)} categories  "
              f"max={max(MERCHANT_RISK.values())}pts  "
              f"categories={sorted(MERCHANT_RISK.keys())}")
        print(f"  FLAG_WEIGHTS  : {dict(FLAG_WEIGHTS)}")
        print(f"  DEPT_NAMES    : {list(DEPT_NAMES.keys())}")
        print(f"  Dataset dir   : {DATASET_DIR}")

        print("\n" + "="*70)
        print("Knowledge base is ready for RAG queries!")
        print("Run: python rag_query.py")
        print("="*70 + "\n")


# =============================================================================
# MODULE-LEVEL ENTRY POINT
# =============================================================================

def ingest_banking_policies(mode: str = "memory",          # ← NEW param
                             reset_first: bool = True):     # ← NEW param
    """
    Main function to ingest all 6 banking policy documents.

    Args:
        mode:        'memory' (default) — generate docs in memory, no disk needed
                     'disk'             — load .txt files from knowledge_base/ dir
        reset_first: Wipe collections before ingesting (default True)
    
    Run after generating policies with policy_generator.py (disk mode only).
    Memory mode requires no prior step.
    """
    # Initialize ChromaDB
    client, config = initialize_chromadb()
    ingester       = DocumentIngester(client, config)

    if mode == "memory":
        result = ingester.ingest_from_generator(
            bank_name   = "Sentinel Bank Nigeria",
            reset_first = reset_first,
        )
        print(f"Ingested {result['total_chunks']} chunks from "
              f"{result['total_docs']} in-memory documents.")
        print(f"Documents: {', '.join(result['documents_ingested'])}")

    else:  # disk
        # ← CHANGED: no longer crashes if dir is missing (auto-generates)
        knowledge_base_dir = Path(__file__).parent.parent / "knowledge_base"
        ingester.ingest_knowledge_base(
            knowledge_base_dir = knowledge_base_dir,
            reset_first        = reset_first,
        )


if __name__ == "__main__":
    """
    Execute document ingestion pipeline.

    Usage:
        python ingest_documents.py             # in-memory mode (default)
        python ingest_documents.py --disk      # disk mode
        python ingest_documents.py --no-reset  # keep existing chunks
    """
    import sys                               # ← NEW: CLI arg parsing

    mode        = "memory"
    reset_first = True

    for arg in sys.argv[1:]:
        if arg == "--disk":
            mode = "disk"
        elif arg == "--no-reset":
            reset_first = False

    ingest_banking_policies(mode=mode, reset_first=reset_first)