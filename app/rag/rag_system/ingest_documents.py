"""
Document Ingestion Pipeline
AI Engineer 2 - Week 1 Deliverable

Loads banking policy documents into ChromaDB vector database.
This module handles:
- Text file loading and validation
- Document chunking strategies
- Metadata enrichment
- Batch ingestion into vector DB

Author: AI Engineer 2 (Security & Knowledge Specialist)
"""

from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging
import hashlib
from datetime import datetime
import re

from chromadb_config import initialize_chromadb, ChromaDBConfig, get_collection_stats

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
    1. Load documents from file system
    2. Validate and preprocess text
    3. Chunk into retrieval-optimized segments
    4. Enrich with metadata
    5. Insert into vector database with embeddings
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
        for txt_file in directory.glob("**/*.txt"):
            try:
                with open(txt_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Extract document ID from filename
                doc_id = txt_file.stem  # Filename without extension
                
                # Determine document type from path
                if 'policies' in txt_file.parts:
                    doc_type = 'policy'
                elif 'faqs' in txt_file.parts:
                    doc_type = 'faq'
                else:
                    doc_type = 'general'
                
                documents.append({
                    'document_id': doc_id,
                    'filepath': str(txt_file),
                    'content': content,
                    'doc_type': doc_type,
                    'filename': txt_file.name
                })
                
                logger.info(f"Loaded document: {txt_file.name}")
                
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
        document['content'] = content.strip()
        document['char_count'] = len(content)
        document['word_count'] = len(content.split())
        
        return document
    
    def create_chunks_from_document(self, document: Dict) -> List[Dict]:
        """
        Chunk document and enrich with metadata.
        
        Args:
            document: Preprocessed document dictionary
            
        Returns:
            List of enriched chunk dictionaries
        """
        # Chunk the document
        chunks = self.chunker.chunk_by_sections(
            document['content'],
            document['document_id']
        )
        
        # Enrich each chunk with metadata
        enriched_chunks = []
        for chunk in chunks:
            # Generate unique hash for deduplication
            content_hash = hashlib.md5(
                chunk['content'].encode()
            ).hexdigest()
            
            # Extract key terms for better retrieval
            key_terms = self.chunker.extract_key_terms(chunk['content'])
            
            enriched_chunk = {
                'id': chunk['chunk_id'],
                'document': chunk['content'],
                'metadata': {
                    'source_document': document['document_id'],
                    'document_type': document['doc_type'],
                    'section_title': chunk['section_title'],
                    'chunk_index': chunk['chunk_index'],
                    'char_count': len(chunk['content']),
                    'content_hash': content_hash,
                    'key_terms': ', '.join(key_terms),
                    'ingestion_timestamp': datetime.now().isoformat()
                }
            }
            
            enriched_chunks.append(enriched_chunk)
        
        return enriched_chunks
    
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
            ids = [chunk['id'] for chunk in batch]
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
    
    def ingest_knowledge_base(self, knowledge_base_dir: Path):
        """
        Complete ingestion pipeline for entire knowledge base.
        
        This is the main entry point for bulk document ingestion.
        
        Args:
            knowledge_base_dir: Root directory containing policies/ and faqs/
        """
        print("\n" + "="*70)
        print(" "*15 + "KNOWLEDGE BASE INGESTION PIPELINE")
        print("="*70 + "\n")
        
        knowledge_base_dir = Path(knowledge_base_dir)
        
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
        print(f"✓ Created {len(all_chunks)} chunks total\n")
        
        # Separate chunks by type
        policy_chunks = [c for c in all_chunks if c['metadata']['document_type'] == 'policy']
        faq_chunks = [c for c in all_chunks if c['metadata']['document_type'] == 'faq']
        
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
        
        # Display statistics
        print("\n" + "="*70)
        print(" "*20 + "INGESTION COMPLETE!")
        print("="*70 + "\n")
        
        print("Collection Statistics:")
        for collection_name in [self.config.COLLECTION_POLICIES, 
                               self.config.COLLECTION_FAQS,
                               self.config.COLLECTION_ALL]:
            collection = self.client.get_collection(collection_name)
            stats = get_collection_stats(collection)
            print(f"\n{collection_name}:")
            print(f"  Total chunks: {stats['total_documents']}")
        
        print("\n" + "="*70)
        print("Knowledge base is ready for RAG queries!")
        print("="*70 + "\n")


def ingest_banking_policies():
    """
    Main function to ingest banking policy documents.
    
    Run this after generating policies with generate_policies.py
    """
    # Initialize ChromaDB
    client, config = initialize_chromadb()
    
    # Create ingester
    ingester = DocumentIngester(client, config)
    
    # Path to knowledge base
    knowledge_base_dir = Path(__file__).parent.parent / "knowledge_base"
    
    if not knowledge_base_dir.exists():
        print(f"ERROR: Knowledge base directory not found: {knowledge_base_dir}")
        print("Please run generate_policies.py first to create policy documents")
        return
    
    # Run ingestion pipeline
    ingester.ingest_knowledge_base(knowledge_base_dir)


if __name__ == "__main__":
    """Execute document ingestion pipeline"""
    ingest_banking_policies()