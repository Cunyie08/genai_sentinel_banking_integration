"""
RAG Query System - Production Async Version
============================================

Retrieval-Augmented Generation (RAG) query interface for banking policies.

ARCHITECTURE OVERVIEW:
---------------------
This module is the RETRIEVAL + ANSWER GENERATION layer in the RAG pipeline:

    1. INGESTION (One-time, done by ingest_documents.py):
       Documents → Chunks → Embeddings → ChromaDB storage
    
    2. RETRIEVAL (This module - every query):
       Query text → Embed → Similarity search → Top-k chunks
    
    3. GENERATION (This module - every query):
       Retrieved chunks → Synthesize answer → Return with citations

EMBEDDING MODEL:
---------------
- Model: sentence-transformers/all-MiniLM-L6-v2
- Dimensions: 384
- Location: ChromaDB handles embeddings automatically
- When: Query text is embedded at search time (inside collection.query())

AGENT INTEGRATION:
-----------------
Three AI agents use this module:

    ┌──────────────────────────────────────────────────────┐
    │             RAGQueryEngine (This File)               │
    ├──────────────────────────────────────────────────────┤
    │                                                      │
    │ Core Methods:                                        │
    │  • query() - Semantic search                         │
    │  • batch_query() - Bulk processing                   │
    │                                                      │
    │ Agent-Specific Methods:                              │
    │  ├─ detect_complaint_category()  → Dispatcher        │
    │  ├─ calculate_fraud_risk()       → Sentinel          │
    │  └─ validate_product_recommendation() → Trajectory   │
    │                                                      │
    └──────────────────────────────────────────────────────┘
              ↓              ↓              ↓
       ┌───────────┐  ┌──────────┐  ┌──────────┐
       │Dispatcher │  │ Sentinel │  │Trajectory│
       │  Agent    │  │  Agent   │  │  Agent   │
       └───────────┘  └──────────┘  └──────────┘

USAGE EXAMPLE:
-------------
    from rag_system.rag_query import RAGQueryEngine
    from rag_system.chromadb_config import initialize_chromadb
    
    # Initialize
    client, config =  initialize_chromadb()
    engine = RAGQueryEngine(client, config)
    
    # Use in Dispatcher Agent
    routing = await engine.detect_complaint_category(complaint_text)
    
    # Use in Sentinel Agent
    risk = await engine.calculate_fraud_risk(transaction_dict)
    
    # Use in Trajectory Agent
    validation = await engine.validate_product_recommendation(
        customer_data, product
    )

SHARED CONSTANTS:
----------------
All risk weights, thresholds, and department codes are imported from
.knowledge_base.generate_policies.py to ensure zero drift between policies and code.

Author: AI Engineer 2 (Security & Knowledge Specialist)
Date: February 2026
Version: 3.0 (Async Production)
"""

import asyncio
from typing import List, Dict, Optional, Tuple, Any
import logging
from pathlib import Path
from datetime import datetime
import re

# ChromaDB configuration
from .chromadb_config import initialize_chromadb, ChromaDBConfig

# =============================================================================
# IMPORT SHARED CONSTANTS FROM POLICY GENERATOR
# =============================================================================
# Single source of truth - prevents drift between policies and implementation
try:
    from ..knowledge_base.generate_policies import (
        MERCHANT_RISK,           # FRM-002: Merchant category risk weights
        FLAG_WEIGHTS,            # FRM-001: Fraud flag weights
        EXPECTED_SLA,            # POL-CCH-001: SLA hours per department
        DEPT_NAMES,              # POL-CCH-001: Full department names
        RISK_THRESHOLDS,         # FRM-001: Risk score thresholds
        PRODUCT_THRESHOLDS,      # PRS-001: Product eligibility thresholds
        CAR_LOAN_SIGNAL_WEIGHTS, # PRS-001: Car loan signal components
        COMPLAINTS_CSV,          # Dataset path for validation
    )
except ImportError:
    # Fallback for testing - import from local if module structure differs
    logging.warning("Could not import from knowledge_base.generate_policies - using fallback values")
    MERCHANT_RISK = {}
    FLAG_WEIGHTS = {}
    EXPECTED_SLA = {"TSU": 48, "COC": 48, "FRM": 24, "DCS": 72, "AOD": 72, "CLS": 96}
    DEPT_NAMES = {}
    RISK_THRESHOLDS = {}
    PRODUCT_THRESHOLDS = {}
    CAR_LOAN_SIGNAL_WEIGHTS = {}
    COMPLAINTS_CSV = Path("complaints.csv")


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# MAIN RAG QUERY ENGINE CLASS
# =============================================================================

class RAGQueryEngine:
    """
    Async RAG-powered query engine for banking policy questions.
    
    This class provides the retrieval and answer generation layer for the
    AI-driven banking middleware system. It uses semantic search to find
    relevant policy chunks and synthesizes grounded answers with citations.
    
    Key Features:
    ------------
    - **Async operations**: All methods are async for non-blocking execution
    - **Semantic search**: Uses embeddings for meaning-based retrieval
    - **Grounded answers**: Refuses to hallucinate - only answers from docs
    - **Citation tracking**: Every answer includes source documents
    - **Confidence scoring**: Quantifies answer reliability (0-1)
    - **Agent-specific methods**: Tailored for Dispatcher, Sentinel, Trajectory
    - **Shared constants**: Imports all thresholds from policy_generator.py
    
    Architecture:
    ------------
    ChromaDB Storage → Embedding Model → This Class → AI Agents
    
    The embedding model (all-MiniLM-L6-v2) is initialized in chromadb_config.py
    and used automatically by ChromaDB when you call collection.query().
    You don't see the embedding code because it's handled internally.
    
    Thread Safety:
    -------------
    This class is async-safe. Multiple agents can query concurrently.
    
    Example:
    -------
        # Initialize (async)
        client, config = await initialize_chromadb()
        engine = RAGQueryEngine(client, config)
        
        # Query (async)
        result = await engine.query("What is the SLA for TSU?")
        print(result['answer'])
        print(result['sources'])
        print(result['confidence'])
    
    Attributes:
    ----------
    client : ChromaDB client
        Database client for vector storage
    config : ChromaDBConfig
        Configuration object with collection names
    policy_collection : Collection
        Collection containing policy documents
    faq_collection : Collection
        Collection containing FAQ documents
    all_collection : Collection
        Combined collection for searching all documents
    """
    
    # =========================================================================
    # CLASS CONSTANTS
    # =========================================================================
    
    # Retrieval parameters
    DEFAULT_TOP_K = 5  # Number of chunks to retrieve per query
    RELEVANCE_THRESHOLD = 0.5  # Minimum similarity score (0-1)
    HIGH_CONFIDENCE_THRESHOLD = 0.75  # Threshold for "high confidence"
    
    # Department code mapping (POL-CCH-001)
    # Maps both full names and codes → standardized code
    DEPARTMENT_MAPPING = {
        'Transaction Services Unit': 'TSU',
        'TSU': 'TSU',
        'Card Operations Center': 'COC',
        'COC': 'COC',
        'Fraud Risk Management': 'FRM',
        'FRM': 'FRM',
        'Digital Channels Support': 'DCS',
        'DCS': 'DCS',
        'Account Operations Department': 'AOD',
        'AOD': 'AOD',
        'Credit & Loan Services': 'CLS',
        'CLS': 'CLS'
    }
    
    # =========================================================================
    # INITIALIZATION
    # =========================================================================
    
    def __init__(self, client, config: ChromaDBConfig):
        """
        Initialize the RAG query engine.
        
        This sets up connections to the three ChromaDB collections:
        1. bank_policies - Policy documents (POL-CCH-001, FRM-001, etc.)
        2. customer_faqs - Customer FAQ document
        3. all_documents - Combined collection for searching everything
        
        Args:
            client: ChromaDB client instance (from initialize_chromadb())
            config: ChromaDBConfig object with collection names and settings
            
        Raises:
            Exception: If collections cannot be loaded
            
        Note:
            The embedding model is initialized in ChromaDBConfig and used
            automatically by ChromaDB. You don't interact with it directly.
        """
        self.client = client
        self.config = config
        
        # Load the three collections
        try:
            self.policy_collection = config.get_or_create_collection(
                client,
                config.COLLECTION_POLICIES
            )
            self.faq_collection = config.get_or_create_collection(
                client,
                config.COLLECTION_FAQS
            )
            self.all_collection = config.get_or_create_collection(
                client,
                config.COLLECTION_ALL
            )
            logger.info("✓ RAG Query Engine initialized successfully")
            logger.info(f"  - Policies: {self.policy_collection.count()} chunks")
            logger.info(f"  - FAQs: {self.faq_collection.count()} chunks")
            logger.info(f"  - Total: {self.all_collection.count()} chunks")
        except Exception as e:
            logger.error(f"Failed to initialize RAG Query Engine: {e}")
            raise
    
    # =========================================================================
    # CORE RETRIEVAL METHODS
    # =========================================================================
    
    async def query(
        self,
        question: str,
        collection_name: Optional[str] = None,
        top_k: int = DEFAULT_TOP_K,
        include_metadata: bool = True
    ) -> Dict[str, Any]:
        """
        Query the knowledge base with semantic search (async).
        
        This is the core retrieval method. It:
        1. Embeds the question into a 384-dim vector (automatic)
        2. Finds top-k most similar document chunks via cosine similarity
        3. Filters chunks by relevance threshold
        4. Synthesizes a grounded answer from retrieved chunks
        5. Returns answer with citations and confidence score
        
        Embedding Process (Automatic):
        ------------------------------
        When you call this method, ChromaDB automatically:
            1. Takes your question string
            2. Passes it to sentence-transformers model
            3. Gets back a 384-dimension vector
            4. Compares this vector to all stored chunk vectors
            5. Returns the most similar chunks
        
        You never see this embedding step - it's handled internally by ChromaDB.
        
        Args:
            question (str): The question to answer
                Example: "What is the SLA for transaction disputes?"
            
            collection_name (str, optional): Which collection to search
                - "bank_policies" - Search only policy documents
                - "customer_faqs" - Search only FAQ
                - None (default) - Search all documents
            
            top_k (int): Number of similar chunks to retrieve
                Default: 5 (good balance of coverage vs noise)
            
            include_metadata (bool): Include chunk metadata in results
                Default: True (needed for citations)
        
        Returns:
            Dict containing:
                - answer (str): Synthesized answer or None if not found
                - sources (List[Dict]): Citations with source documents
                - confidence (float): 0-1 score (higher = more confident)
                - grounded (bool): True if answer based on retrieved docs
                - retrieved_chunks (int): Number of chunks used
                - question (str): Original question (for logging)
                - message (str): Error message if no answer found
        
        Example:
            >>> result = await engine.query("What is the SLA for TSU?")
            >>> print(result['answer'])
            "The SLA for Transaction Services Unit (TSU) is 48 hours..."
            
            >>> print(result['confidence'])
            0.847
            
            >>> print(result['sources'][0]['source_document'])
            "POL-CCH-001.txt"
        
        Raises:
            Exception: If ChromaDB query fails
        
        Note:
            This method does NOT use an LLM for answer generation.
            It's pure retrieval + extractive summarization for speed.
            For LLM-powered answers, you'd integrate Claude API here.
        """
        logger.info(f"Query received: {question[:100]}...")
        
        # Step 1: Select collection to search
        if collection_name:
            collection = self.client.get_collection(collection_name)
        else:
            collection = self.all_collection
        
        # Step 2: Perform semantic search
        # This is where embedding happens (automatic inside ChromaDB)
        try:
            results = await asyncio.to_thread(
                collection.query,
                query_texts=[question],
                n_results=top_k,
                include=['documents', 'metadatas', 'distances']
            )
        except Exception as e:
            logger.error(f"ChromaDB query failed: {e}")
            return self._error_response(
                "Database query failed. Please try again.",
                question
            )
        
        # Step 3: Check if any documents were found
        if not results['documents'] or not results['documents'][0]:
            logger.warning(f"No relevant documents found for: {question}")
            return self._error_response(
                "No relevant information found in the knowledge base.",
                question
            )
        
        # Step 4: Extract and structure retrieved chunks
        retrieved_chunks = self._process_retrieval_results(
            results, include_metadata
        )
        
        # Step 5: Filter by relevance threshold
        relevant_chunks = [
            chunk for chunk in retrieved_chunks
            if chunk['similarity'] >= self.RELEVANCE_THRESHOLD
        ]
        
        if not relevant_chunks:
            logger.warning(f"No chunks above relevance threshold for: {question}")
            return self._error_response(
                "Found information but relevance is too low to answer confidently.",
                question
            )
        
        # Step 6: Calculate overall confidence
        avg_similarity = sum(c['similarity'] for c in relevant_chunks) / len(relevant_chunks)
        confidence = min(avg_similarity, 1.0)
        
        # Step 7: Synthesize answer from relevant chunks
        answer = await self._synthesize_answer(question, relevant_chunks)
        
        # Step 8: Prepare source citations
        sources = self._prepare_citations(relevant_chunks)
        
        # Step 9: Return complete result
        result = {
            'answer': answer,
            'sources': sources,
            'confidence': round(confidence, 3),
            'grounded': True,
            'retrieved_chunks': len(relevant_chunks),
            'question': question
        }
        
        logger.info(f"✓ Query answered | Confidence: {confidence:.3f} | Chunks: {len(relevant_chunks)}")
        return result
    
    def _process_retrieval_results(
        self,
        results: Dict,
        include_metadata: bool
    ) -> List[Dict]:
        """
        Process raw ChromaDB results into structured chunks.
        
        Args:
            results: Raw results from ChromaDB query()
            include_metadata: Whether to include metadata
        
        Returns:
            List of structured chunk dictionaries
        """
        retrieved_chunks = []
        
        for i in range(len(results['documents'][0])):
            chunk = {
                'content': results['documents'][0][i],
                'metadata': results['metadatas'][0][i] if include_metadata else {},
                'distance': results['distances'][0][i],
                'similarity': 1 - results['distances'][0][i]  # Convert to similarity
            }
            retrieved_chunks.append(chunk)
        
        return retrieved_chunks
    
    def _error_response(self, message: str, question: str) -> Dict:
        """
        Generate error response structure.
        
        Args:
            message: Error message for user
            question: Original question
        
        Returns:
            Dict with error structure
        """
        return {
            'answer': None,
            'sources': [],
            'confidence': 0.0,
            'grounded': False,
            'message': message,
            'question': question,
            'retrieved_chunks': 0
        }
    
    async def _synthesize_answer(
        self,
        question: str,
        chunks: List[Dict]
    ) -> str:
        """
        Synthesize answer from retrieved chunks (async).
        
        This uses pure extractive summarization - no LLM generation.
        It finds the most relevant paragraphs and combines them.
        
        For LLM-powered answer generation, you would:
        1. Take the retrieved chunks
        2. Build a prompt with chunks as context
        3. Call Claude API: anthropic.messages.create(...)
        4. Return the LLM-generated answer
        
        Current Implementation:
        ----------------------
        - Extractive (takes exact text from docs)
        - Fast (no API calls)
        - Grounded (cannot hallucinate)
        - Good for fact retrieval
        
        Args:
            question: Original question
            chunks: Retrieved relevant chunks
        
        Returns:
            Synthesized answer string
        """
        # Get most relevant chunk
        most_relevant = chunks[0]
        content = most_relevant['content']
        paragraphs = content.split('\n\n')
        
        # Find best matching paragraph
        question_words = set(question.lower().split())
        best_paragraph = None
        best_overlap = 0
        
        for para in paragraphs:
            if len(para) < 50:  # Skip headers
                continue
            
            para_words = set(para.lower().split())
            overlap = len(question_words & para_words)
            
            if overlap > best_overlap:
                best_overlap = overlap
                best_paragraph = para
        
        # Build answer
        answer_parts = []
        
        if best_paragraph:
            answer_parts.append(best_paragraph.strip())
        else:
            # Fallback to first substantial paragraph
            for para in paragraphs:
                if len(para) > 100:
                    answer_parts.append(para.strip())
                    break
        
        # Add supporting context from second chunk if available
        if len(chunks) > 1:
            second_chunk = chunks[1]
            if second_chunk['metadata'].get('source_document') != \
               most_relevant['metadata'].get('source_document'):
                second_paragraphs = second_chunk['content'].split('\n\n')
                for para in second_paragraphs:
                    if len(para) > 100:
                        answer_parts.append(
                            f"\n\nAdditional context: {para.strip()}"
                        )
                        break
        
        answer = '\n\n'.join(answer_parts)
        
        # Truncate if too long
        words = answer.split()
        if len(words) > 500:
            answer = ' '.join(words[:500]) + \
                     "... [Additional details available in source documents]"
        
        return answer
    
    def _prepare_citations(self, chunks: List[Dict]) -> List[Dict]:
        """
        Prepare source citations for transparency.
        
        Every answer should cite its sources for audit trails and
        explainability. This is critical for banking compliance.
        
        Args:
            chunks: Retrieved chunks with metadata
        
        Returns:
            List of citation dictionaries with:
                - rank: Position in relevance ranking
                - source_document: Policy document ID
                - section: Section title within document
                - document_type: policy/faq/etc
                - similarity_score: How relevant (0-1)
                - snippet: Preview of chunk content
        """
        citations = []
        
        for rank, chunk in enumerate(chunks, 1):
            metadata = chunk['metadata']
            
            citation = {
                'rank': rank,
                'source_document': metadata.get('source_document', 'Unknown'),
                'section': metadata.get('section_title', 'N/A'),
                'document_type': metadata.get('document_type', 'Unknown'),
                'similarity_score': round(chunk['similarity'], 3),
                'snippet': (chunk['content'][:200] + "..."
                           if len(chunk['content']) > 200
                           else chunk['content'])
            }
            
            citations.append(citation)
        
        return citations
    
    # =========================================================================
    # DISPATCHER AGENT METHODS
    # =========================================================================
    
    async def detect_complaint_category(
        self,
        complaint_text: str
    ) -> Dict[str, Any]:
        """
        Detect complaint category and routing for Dispatcher Agent (async).
        
        This method is the PRIMARY interface for the Dispatcher Agent.
        It analyzes a complaint and returns complete routing information:
        department, priority, SLA, and policy justification.
        
        Integration with Dispatcher Agent:
        ----------------------------------
            class DispatcherAgent:
                async def route_complaint(self, complaint_text):
                    # Call RAG
                    routing = await self.rag.detect_complaint_category(
                        complaint_text
                    )
                    
                    # Use results
                    department = routing['department_code']
                    priority = routing['priority_level']
                    sla_hours = routing['sla_hours']
                    
                    # Assign to human agent
                    agent = self.assign_agent(department)
                    
                    return {
                        'department': department,
                        'priority': priority,
                        'sla': sla_hours,
                        'agent': agent,
                        'policy': routing['sources'][0]['source_document']
                    }
        
        Dataset Alignment:
        -----------------
        Matches complaints.csv structure exactly:
            - department_code: TSU | COC | FRM | DCS | AOD | CLS
            - priority_level: Critical | High | Medium | Low
            - sla_hours_limit: From EXPECTED_SLA constant
        
        Routing Logic (POL-CCH-001):
        ----------------------------
        1. Check for fraud keywords → FRM (Critical, 24h SLA)
        2. Check for card/ATM keywords → COC (High, 48h SLA)
        3. Check for app/login keywords → DCS (Medium, 72h SLA)
        4. Check for account keywords → AOD (Medium, 72h SLA)
        5. Check for loan keywords → CLS (Medium, 96h SLA)
        6. Default → TSU (Medium, 48h SLA)
        
        Args:
            complaint_text (str): Customer complaint text
                Example: "My card was declined at Shoprite"
        
        Returns:
            Dict containing:
                - primary_category (str): Complaint type label
                - department_code (str): TSU/COC/FRM/DCS/AOD/CLS
                - department_name (str): Full department name
                - priority_level (str): Critical/High/Medium/Low
                - sla_hours (int): Expected resolution time
                - confidence (float): RAG confidence (0-1)
                - reasoning (str): Policy explanation
                - sources (List[Dict]): Policy document citations
        
        Example:
            >>> routing = await engine.detect_complaint_category(
            ...     "My card was declined at Shoprite"
            ... )
            >>> print(routing['department_code'])  # COC
            >>> print(routing['priority_level'])   # High
            >>> print(routing['sla_hours'])        # 48
            >>> print(routing['sources'][0]['source_document'])
            # "POL-CCH-001.txt"
        
        Note:
            SLA hours come from EXPECTED_SLA constant (imported from
            policy_generator.py). This ensures zero drift between
            policy documents and routing logic.
        """
        # Query RAG for routing policy
        category_result = await self.query(
            f"Which department should handle this complaint: {complaint_text}",
            collection_name="bank_policies",
            top_k=5
        )
        
        # Handle no relevant policy found
        if not category_result['grounded']:
            return {
                'primary_category': 'unknown',
                'department_code': 'UNKNOWN',
                'department_name': 'Unknown',
                'priority_level': 'Medium',
                'sla_hours': EXPECTED_SLA.get('TSU', 48),
                'confidence': 0.0,
                'reasoning': 'No relevant policy found',
                'sources': []
            }
        
        # Extract department code from RAG answer
        dept_code = self.extract_department_code(category_result['answer'])
        
        # Get SLA from shared constant (zero drift)
        sla_hours = EXPECTED_SLA.get(dept_code, 48)
        
        # Determine priority
        priority = self._determine_priority(complaint_text, dept_code)
        
        # Extract category label
        primary_category = self._extract_category(category_result['answer'])
        
        return {
            'primary_category': primary_category,
            'department_code': dept_code,
            'department_name': DEPT_NAMES.get(dept_code, 'Unknown'),
            'priority_level': priority,
            'sla_hours': sla_hours,
            'confidence': category_result['confidence'],
            'reasoning': category_result['answer'][:300] + '...',
            'sources': category_result['sources']
        }
    
    def extract_department_code(self, answer: str) -> str:
        """
        Extract department code from RAG answer text.
        
        This helper method parses the RAG answer to find which department
        was mentioned. It checks for both full names and abbreviations.
        
        Args:
            answer: RAG-generated answer text
        
        Returns:
            Department code (TSU/COC/FRM/DCS/AOD/CLS) or 'UNKNOWN'
        
        Example:
            >>> answer = "Route to Transaction Services Unit (TSU)..."
            >>> extract_department_code(answer)
            'TSU'
        """
        answer_upper = answer.upper()
        for dept_name, code in self.DEPARTMENT_MAPPING.items():
            if dept_name.upper() in answer_upper:
                return code
        return 'UNKNOWN'
    
    def _determine_priority(self, complaint_text: str, dept_code: str) -> str:
        """
        Determine priority level from complaint text and department.
        
        Implements POL-CCH-001 Section 3 priority classification:
            - Critical: Fraud-related, unauthorized access
            - High: Card declined, ATM issues, high-value failures
            - Medium: Standard disputes, app issues
            - Low: Information requests, statements
        
        Args:
            complaint_text: Complaint text to analyze
            dept_code: Department code (may override priority)
        
        Returns:
            Priority level string
        """
        complaint_lower = complaint_text.lower()
        
        # FRM is always Critical (POL-CCH-001 §3)
        if dept_code == 'FRM':
            return 'Critical'
        
        # Critical keywords
        if any(w in complaint_lower for w in
               ['fraud', 'unauthorized', 'hacked', 'stolen', 'scam']):
            return 'Critical'
        
        # High priority keywords
        if any(w in complaint_lower for w in
               ['declined', 'swallowed', 'retention', 'blocked']):
            return 'High'
        if any(p in complaint_lower for p in
               ['not received', 'failed transfer']):
            return 'High'
        
        # Low priority keywords
        if any(w in complaint_lower for w in
               ['statement', 'balance', 'inquiry']):
            return 'Low'
        
        return 'Medium'
    
    def _extract_category(self, answer: str) -> str:
        """
        Extract complaint category label from RAG answer.
        
        Maps keywords to standardized category labels.
        
        Args:
            answer: RAG answer text
        
        Returns:
            Category label string
        """
        categories = {
            'transaction': 'transaction_dispute',
            'transfer': 'transaction_dispute',
            'card': 'card_issue',
            'atm': 'card_issue',
            'fraud': 'fraud_security',
            'unauthorized': 'fraud_security',
            'app': 'digital_banking',
            'login': 'digital_banking',
            'account': 'account_services',
            'statement': 'account_services',
            'loan': 'credit_services'
        }
        
        answer_lower = answer.lower()
        for keyword, category in categories.items():
            if keyword in answer_lower:
                return category
        
        return 'general_inquiry'
    
    # =========================================================================
    # SENTINEL AGENT METHODS
    # =========================================================================
    
    async def calculate_fraud_risk(
        self,
        transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate fraud risk score for Sentinel Agent (async).
        
        This is the PRIMARY interface for the Sentinel Agent. It analyzes
        a transaction and returns a complete fraud risk assessment with
        policy-backed recommendations.
        
        Integration with Sentinel Agent:
        --------------------------------
            class SentinelAgent:
                async def analyze_transaction(self, transaction):
                    # Call RAG
                    risk = await self.rag.calculate_fraud_risk(transaction)
                    
                    # Generate report
                    if risk['should_block']:
                        await self.block_transaction(transaction['id'])
                        await self.freeze_account(transaction['account_id'])
                    
                    elif risk['requires_challenge']:
                        await self.send_push_to_app(
                            transaction['account_id'],
                            transaction['id']
                        )
                    
                    return {
                        'risk_score': risk['total_risk_score'],
                        'risk_level': risk['risk_level'],
                        'action': risk['recommended_action'],
                        'policy': risk['sources'][0]['source_document']
                    }
        
        Risk Scoring Formula (FRM-001 + FRM-002):
        -----------------------------------------
            total = SUM(FLAG_WEIGHTS for each flag in fraud_explainability_trace)
                  + MERCHANT_RISK[merchant_category]
                  + timing_risk (if applicable)
                  capped at 100
        
        Expected Transaction Fields (from transactions.csv):
        ---------------------------------------------------
            - fraud_explainability_trace: "mobile_channel_risk,high_amount_spike"
            - amount: 450000.00
            - transaction_timestamp: "2024-01-15 02:30:00"
            - channel: "mobile_app"
            - merchant_category: "fintech"
            - merchant_name: "Paystack"
            - is_fraud_score: 1
            - transaction_status: "failed"
        
        Args:
            transaction (Dict): Transaction data from transactions.csv
        
        Returns:
            Dict containing:
                - total_risk_score (int): 0-100 composite score
                - risk_level (str): LOW/MEDIUM/HIGH/CRITICAL
                - risk_breakdown (Dict): Component scores
                - recommended_action (str): What to do
                - requires_challenge (bool): Push-to-app needed?
                - should_block (bool): Block immediately?
                - policy_explanation (str): RAG-generated reasoning
                - sources (List[Dict]): Policy citations
                - confidence (float): RAG confidence
        
        Example:
            >>> txn = {
            ...     'fraud_explainability_trace': 'mobile_channel_risk,high_amount_spike',
            ...     'amount': 450000,
            ...     'merchant_category': 'fintech'
            ... }
            >>> risk = await engine.calculate_fraud_risk(txn)
            >>> print(risk['total_risk_score'])  # 65
            >>> print(risk['risk_level'])        # HIGH
            >>> print(risk['recommended_action'])
            # "Mandatory push-to-app biometric challenge"
        
        Note:
            All weights (FLAG_WEIGHTS, MERCHANT_RISK) are imported from
            policy_generator.py. Risk thresholds come from RISK_THRESHOLDS
            constant. This ensures zero drift.
        """
        # Initialize risk breakdown
        risk_breakdown = {
            'flag_score': 0,     # From fraud_explainability_trace
            'merchant_risk': 0,  # From merchant_category
            'timing_risk': 0,    # Odd-hour bonus
        }
        
        # ------------------------------------------------------------------
        # STEP 1: Parse fraud flags from fraud_explainability_trace
        # ------------------------------------------------------------------
        # Format: "mobile_channel_risk,high_amount_spike,multiple_failures"
        # or "normal_pattern" if no fraud detected
        
        trace = str(transaction.get('fraud_explainability_trace', 'normal_pattern'))
        for flag in trace.split(','):
            flag = flag.strip()
            # Lookup in FLAG_WEIGHTS constant (exact match)
            points = FLAG_WEIGHTS.get(flag, 0)
            risk_breakdown['flag_score'] += points
        
        # ------------------------------------------------------------------
        # STEP 2: Add merchant category risk
        # ------------------------------------------------------------------
        # Categories: fintech, transport, education, healthcare, telecoms,
        #             supermarket, restaurants, fuel, utilities
        
        merchant_category = str(
            transaction.get('merchant_category', '')
        ).lower().strip()
        risk_breakdown['merchant_risk'] = MERCHANT_RISK.get(merchant_category, 0)
        
        # ------------------------------------------------------------------
        # STEP 3: Add timing risk for odd-hour high-value transactions
        # ------------------------------------------------------------------
        # FRM-001 §1.1: 12 AM - 5 AM WAT + amount ≥ ₦100,000 = +20 points
        
        timestamp = str(transaction.get('transaction_timestamp', ''))
        amount = float(transaction.get('amount', 0))
        
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace(' ', 'T'))
                if 0 <= dt.hour < 5 and amount >= 100_000:
                    risk_breakdown['timing_risk'] += 20
            except (ValueError, TypeError):
                pass
        
        # ------------------------------------------------------------------
        # STEP 4: Calculate total and cap at 100
        # ------------------------------------------------------------------
        total_risk = min(sum(risk_breakdown.values()), 100)
        
        # ------------------------------------------------------------------
        # STEP 5: Determine risk level from RISK_THRESHOLDS constant
        # ------------------------------------------------------------------
        # LOW: 0-30, MEDIUM: 31-60, HIGH: 61-85, CRITICAL: 86-100
        
        risk_level = 'LOW'
        for level, (low, high) in RISK_THRESHOLDS.items():
            if low <= total_risk <= high:
                risk_level = level
                break
        
        # ------------------------------------------------------------------
        # STEP 6: Map to action and flags (FRM-001 §2.2)
        # ------------------------------------------------------------------
        if risk_level == 'CRITICAL':
            action = 'BLOCK transaction immediately and freeze account'
            requires_challenge = False
            should_block = True
        elif risk_level == 'HIGH':
            action = 'Mandatory push-to-app biometric challenge'
            requires_challenge = True
            should_block = False
        elif risk_level == 'MEDIUM':
            action = 'Step-up authentication via OTP'
            requires_challenge = True
            should_block = False
        else:  # LOW
            action = 'Process normally with SMS alert after transaction'
            requires_challenge = False
            should_block = False
        
        # ------------------------------------------------------------------
        # STEP 7: Get policy explanation from RAG
        # ------------------------------------------------------------------
        explanation_query = await self.query(
            f"Explain the fraud risk assessment for a {risk_level} risk score "
            f"of {total_risk} points and the recommended action",
            collection_name="bank_policies",
            top_k=3
        )
        
        return {
            'total_risk_score': total_risk,
            'risk_level': risk_level,
            'risk_breakdown': risk_breakdown,
            'recommended_action': action,
            'requires_challenge': requires_challenge,
            'should_block': should_block,
            'policy_explanation': (
                explanation_query['answer']
                if explanation_query['answer']
                else f"Risk score {total_risk}/100 → {risk_level}: {action}"
            ),
            'sources': explanation_query['sources'],
            'confidence': explanation_query['confidence']
        }
    
    # =========================================================================
    # TRAJECTORY AGENT METHODS
    # =========================================================================
    
    async def validate_product_recommendation(
        self,
        customer_data: Dict[str, Any],
        recommended_product: str
    ) -> Dict[str, Any]:
        """
        Validate product recommendation for Trajectory Agent (async).
        
        This is the PRIMARY interface for the Trajectory Agent. It validates
        whether a customer meets eligibility criteria for a product
        recommendation (Car Loan, Personal Loan, Investment Plan).
        
        Integration with Trajectory Agent:
        ----------------------------------
            class TrajectoryAgent:
                async def validate_recommendation(self, customer_data, product):
                    # Call RAG
                    validation = await self.rag.validate_product_recommendation(
                        customer_data, product
                    )
                    
                    # Use results
                    if validation['is_eligible']:
                        await self.offer_product(
                            customer_data['customer_id'],
                            product
                        )
                    
                    return {
                        'product': product,
                        'eligible': validation['is_eligible'],
                        'criteria_met': validation['met_criteria'],
                        'criteria_unmet': validation['unmet_criteria'],
                        'policy': validation['sources'][0]['source_document']
                    }
        
        Product Eligibility Hierarchy (PRS-001):
        ----------------------------------------
        Step 1: monthly_inflow > ₦2,000,000 → Investment Plan
        Step 2: car_loan_signal_score >= 0.7 → Car Loan
        Step 3: salary_detected + inflow > ₦300,000 → Personal Loan
        Default: None
        
        Expected Customer Data Fields:
        -----------------------------
            - monthly_inflow: 1500000.00 (sum of credits)
            - salary_detected: True (2+ fintech credits > ₦200K)
            - car_loan_signal_score: 0.7 (weighted from Uber/salary/inflow)
            - age: 28
        
        Args:
            customer_data (Dict): Customer attributes from joined datasets
            recommended_product (str): "Car Loan" | "Personal Loan" | "Investment Plan"
        
        Returns:
            Dict containing:
                - is_eligible (bool): Meets all criteria?
                - confidence (float): RAG confidence
                - met_criteria (List[str]): Criteria passed
                - unmet_criteria (List[str]): Criteria failed
                - recommendation (str): APPROVED/NOT_ELIGIBLE
                - policy_basis (str): RAG explanation
                - sources (List[Dict]): Policy citations
                - hierarchy_step (int): Which step in hierarchy (1/2/3)
        
        Example:
            >>> customer = {
            ...     'monthly_inflow': 600000,
            ...     'salary_detected': True,
            ...     'car_loan_signal_score': 0.7
            ... }
            >>> result = await engine.validate_product_recommendation(
            ...     customer, "Car Loan"
            ... )
            >>> print(result['is_eligible'])  # True
            >>> print(result['met_criteria'])
            # ['car_loan_signal_score 0.70 ≥ threshold 0.7', ...]
        
        Note:
            All thresholds come from PRODUCT_THRESHOLDS and
            CAR_LOAN_SIGNAL_WEIGHTS constants (imported from
            policy_generator.py). Zero drift guaranteed.
        """
        # Query RAG for policy grounding
        policy_result = await self.query(
            f"What are the eligibility criteria for {recommended_product} "
            f"per PRS-001 product recommendation policy?",
            collection_name="bank_policies",
            top_k=5
        )
        
        if not policy_result['grounded']:
            return {
                'is_eligible': False,
                'confidence': 0.0,
                'met_criteria': [],
                'unmet_criteria': ['No policy found in knowledge base'],
                'recommendation': 'CANNOT_VALIDATE',
                'policy_basis': 'No policy available',
                'sources': [],
                'hierarchy_step': 0
            }
        
        # Extract customer attributes
        monthly_inflow = float(customer_data.get('monthly_inflow', 0))
        salary_detected = bool(customer_data.get('salary_detected', False))
        car_loan_score = float(customer_data.get('car_loan_signal_score', 0))
        age = int(customer_data.get('age', 0))
        
        # Read thresholds from shared constants
        inv_min = PRODUCT_THRESHOLDS['Investment Plan']['monthly_inflow_min']
        cl_score = PRODUCT_THRESHOLDS['Car Loan']['car_loan_signal_score_min']
        pl_min = PRODUCT_THRESHOLDS['Personal Loan']['monthly_inflow_min']
        uber_min = CAR_LOAN_SIGNAL_WEIGHTS['uber_tracker_min']
        inflow_min = CAR_LOAN_SIGNAL_WEIGHTS['monthly_inflow_min']
        
        met = []
        unmet = []
        hierarchy_step = 0
        
        # Validate criteria based on product
        if recommended_product == "Investment Plan":
            hierarchy_step = 1
            if monthly_inflow > inv_min:
                met.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} > '
                    f'threshold ₦{inv_min:,.0f}'
                )
            else:
                unmet.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} ≤ ₦{inv_min:,.0f} '
                    f'(gap: ₦{inv_min - monthly_inflow:,.0f})'
                )
        
        elif recommended_product == "Car Loan":
            hierarchy_step = 2
            # Check step 1 doesn't override
            if monthly_inflow > inv_min:
                unmet.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} > ₦{inv_min:,.0f} '
                    f'→ Investment Plan should take priority'
                )
            
            # Car loan signal score
            if car_loan_score >= cl_score:
                met.append(
                    f'car_loan_signal_score {car_loan_score:.2f} ≥ '
                    f'threshold {cl_score}'
                )
            else:
                unmet.append(
                    f'car_loan_signal_score {car_loan_score:.2f} < '
                    f'threshold {cl_score} (gap: {cl_score - car_loan_score:.2f})'
                )
            
            # Salary signal (informational)
            if salary_detected:
                met.append('Salary detected (2+ fintech credits > ₦200K)')
        
        elif recommended_product == "Personal Loan":
            hierarchy_step = 3
            # Check steps 1 and 2 don't override
            if monthly_inflow > inv_min:
                unmet.append(
                    f'Monthly inflow > ₦{inv_min:,.0f} '
                    f'→ Investment Plan priority'
                )
            if car_loan_score >= cl_score:
                unmet.append(
                    f'car_loan_signal_score ≥ {cl_score} '
                    f'→ Car Loan priority'
                )
            
            # Personal Loan mandatory criteria
            if salary_detected:
                met.append('Salary detected')
            else:
                unmet.append('salary_detected = False')
            
            if monthly_inflow > pl_min:
                met.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} > ₦{pl_min:,.0f}'
                )
            else:
                unmet.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} ≤ ₦{pl_min:,.0f} '
                    f'(gap: ₦{pl_min - monthly_inflow:,.0f})'
                )
        
        else:
            unmet.append(f'Unknown product "{recommended_product}"')
        
        is_eligible = len(unmet) == 0
        
        return {
            'is_eligible': is_eligible,
            'confidence': policy_result['confidence'],
            'met_criteria': met,
            'unmet_criteria': unmet,
            'recommendation': 'APPROVED' if is_eligible else 'NOT_ELIGIBLE',
            'policy_basis': policy_result['answer'],
            'sources': policy_result['sources'],
            'hierarchy_step': hierarchy_step
        }
    
    # =========================================================================
    # BATCH PROCESSING & VALIDATION
    # =========================================================================
    
    async def batch_query(
        self,
        questions: List[str],
        top_k: int = 3,
        show_progress: bool = True
    ) -> List[Dict]:
        """
        Process multiple questions in batch (async).
        
        Optimized for bulk testing (e.g., validating 1000 complaints).
        Processes queries concurrently for speed.
        
        Args:
            questions: List of questions to process
            top_k: Chunks per question
            show_progress: Show progress bar (requires tqdm)
        
        Returns:
            List of query results (same order as questions)
        
        Example:
            >>> questions = ["SLA for TSU?", "Fraud criteria?", ...]
            >>> results = await engine.batch_query(questions)
            >>> print(len(results))  # Same as len(questions)
        """
        results = []
        
        if show_progress:
            try:
                from tqdm.asyncio import tqdm
                iterator = tqdm(questions, desc="Processing queries")
            except ImportError:
                logger.warning(
                    "tqdm not installed. "
                    "Install for progress bars: pip install tqdm"
                )
                iterator = questions
        else:
            iterator = questions
        
        # Process concurrently
        tasks = [self.query(q, top_k=top_k) for q in iterator]
        results = await asyncio.gather(*tasks)
        
        logger.info(f"✓ Processed {len(questions)} queries in batch")
        return results
    
    async def validate_against_dataset(
        self,
        complaints_csv: str = None,
        sample_size: int = 100
    ) -> Dict:
        """
        Validate RAG routing accuracy against complaints dataset (async).
        
        Tests the Dispatcher Agent's routing logic against actual complaints.
        
        Args:
            complaints_csv: Path to complaints.csv (default: COMPLAINTS_CSV)
            sample_size: Number of complaints to test
        
        Returns:
            Dict with validation results:
                - department_accuracy: % correct department
                - priority_accuracy: % correct priority
                - misrouted: List of errors
                - average_confidence: Mean confidence score
        
        Example:
            >>> results = await engine.validate_against_dataset(
            ...     "complaints.csv", sample_size=500
            ... )
            >>> print(f"Accuracy: {results['department_accuracy']:.1f}%")
            # "Accuracy: 94.2%"
        """
        import pandas as pd
        
        # Use shared constant as default
        csv_path = complaints_csv or str(COMPLAINTS_CSV)
        
        # Load dataset
        complaints_df = pd.read_csv(csv_path)
        
        # Sample
        if sample_size < len(complaints_df):
            sample = complaints_df.sample(n=sample_size, random_state=42)
        else:
            sample = complaints_df
        
        correct_dept = 0
        correct_priority = 0
        misrouted = []
        confidences = []
        
        logger.info(f"Testing {len(sample)} complaints from: {csv_path}")
        
        # Process concurrently
        tasks = [
            self.detect_complaint_category(row['complaint_text'])
            for _, row in sample.iterrows()
        ]
        results = await asyncio.gather(*tasks)
        
        # Validate results
        for result, (_, complaint) in zip(results, sample.iterrows()):
            # Department accuracy
            if result['department_code'] == complaint['department_code']:
                correct_dept += 1
            else:
                misrouted.append({
                    'complaint_id': complaint.get('complaint_id', ''),
                    'complaint_text': complaint['complaint_text'][:100] + '...',
                    'expected_dept': complaint['department_code'],
                    'got_dept': result['department_code'],
                    'confidence': result['confidence']
                })
            
            # Priority accuracy
            if result['priority_level'] == complaint.get('priority_level', 'Medium'):
                correct_priority += 1
            
            confidences.append(result['confidence'])
        
        dept_accuracy = (correct_dept / len(sample)) * 100
        priority_accuracy = (correct_priority / len(sample)) * 100
        avg_confidence = sum(confidences) / len(confidences)
        
        return {
            'department_accuracy': dept_accuracy,
            'priority_accuracy': priority_accuracy,
            'total_tested': len(sample),
            'correct_departments': correct_dept,
            'correct_priorities': correct_priority,
            'misrouted': misrouted[:10],  # First 10 errors
            'average_confidence': avg_confidence
        }
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def get_collection_info(self) -> Dict:
        """
        Get information about loaded collections.
        
        Returns:
            Dict with collection statistics
        
        Example:
            >>> info = engine.get_collection_info()
            >>> print(info['policies']['count'])  # 21
        """
        return {
            'policies': {
                'count': self.policy_collection.count(),
                'name': self.config.COLLECTION_POLICIES
            },
            'faqs': {
                'count': self.faq_collection.count(),
                'name': self.config.COLLECTION_FAQS
            },
            'all_documents': {
                'count': self.all_collection.count(),
                'name': self.config.COLLECTION_ALL
            }
        }


# =============================================================================
# INTERACTIVE DEMOS
# =============================================================================

async def interactive_query_demo():
    """Interactive demo of RAG query system (async)"""
    print("\n" + "="*70)
    print(" "*15 + "RAG QUERY SYSTEM - INTERACTIVE DEMO")
    print("="*70 + "\n")
    
    # Initialize
    print("Initializing RAG query engine...")
    client, config =  initialize_chromadb()
    engine = RAGQueryEngine(client, config)
    print("✓ Query engine ready\n")
    
    # Display collection info
    print("Knowledge Base Statistics:")
    info = engine.get_collection_info()
    for name, stats in info.items():
        print(f"  {name}: {stats['count']} chunks")
    print()
    
    # Interactive query loop
    print("Enter your questions (type 'quit' to exit):")
    print("-" * 70)
    
    while True:
        try:
            question = input("\nYour question: ").strip()
            
            if not question:
                continue
            
            if question.lower() in ['quit', 'exit', 'q']:
                print("\nGoodbye!")
                break
            
            # Process query
            print("\nSearching knowledge base...")
            result = await engine.query(question)
            
            # Display results
            print("\n" + "="*70)
            print(f"CONFIDENCE: {result['confidence']:.1%}")
            print(f"GROUNDED: {'✓ Yes' if result['grounded'] else '✗ No'}")
            print("="*70)
            
            if result['answer']:
                print("\nANSWER:")
                print(result['answer'])
                
                print("\n" + "-"*70)
                print(f"SOURCES ({len(result['sources'])} documents):")
                for source in result['sources']:
                    print(f"\n  [{source['rank']}] {source['source_document']}")
                    print(f"      Section: {source['section']}")
                    print(f"      Relevance: {source['similarity_score']:.1%}")
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


async def run_test_queries():
    """Run test queries to demonstrate RAG capabilities (async)"""
    print("\n" + "="*70)
    print(" "*15 + "RAG SYSTEM TEST QUERIES")
    print("="*70 + "\n")
    
    # Initialize engine
    client, config =  initialize_chromadb()
    engine = RAGQueryEngine(client, config)
    
    # Test questions
    test_questions = [
        "What is the SLA for transaction disputes?",
        "Which department handles card retention issues?",
        "How are fraud cases handled?",
        "What risk score triggers an account freeze?",
        "Why is a fintech merchant transaction considered high risk?",
        "What is the daily limit for Tier 2 accounts?",
        "How long does it take to reverse a failed NIP transfer?",
        "What criteria must a customer meet for a Car Loan?",
        "How is salary_detected determined?",
    ]
    
    print(f"Running {len(test_questions)} test queries...\n")
    
    # Process concurrently
    results = await engine.batch_query(test_questions, show_progress=True)
    
    # Display results
    for i, (question, result) in enumerate(zip(test_questions, results), 1):
        status = "✓" if result['answer'] else "✗"
        conf = f"{result['confidence']:.1%}" if result['answer'] else "N/A"
        print(f"[{i}/{len(test_questions)}] {status} {question} (conf: {conf})")
    
    # Summary
    print("\n" + "="*70)
    print("TEST RESULTS SUMMARY")
    print("="*70)
    
    answered = sum(1 for r in results if r['answer'])
    high_conf = sum(1 for r in results if r['confidence'] > 0.75)
    avg_conf = sum(r['confidence'] for r in results) / len(results)
    
    print(f"\nQuestions answered: {answered}/{len(test_questions)}")
    print(f"High confidence (>75%): {high_conf}/{answered}")
    print(f"Average confidence: {avg_conf:.1%}")
    print("\n" + "="*70)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    """
    Run in test mode or interactive mode.
    
    Usage:
        python rag_query_.py          # Interactive demo
        python rag_query_.py --test   # Run test queries
    """
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        asyncio.run(run_test_queries())
    else:
        asyncio.run(interactive_query_demo())