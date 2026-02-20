"""
RAG Query System - COMPLETE PRODUCTION VERSION
AI Engineer 2 - Week 1 Deliverable (Enhanced for All Agents)

Retrieval-Augmented Generation query interface for banking policies.
This module provides:
- Semantic search across policy documents
- Grounded answer generation (hallucination prevention)
- Citation tracking for transparency
- Confidence scoring
- Dispatcher Agent support (complaint routing)
- Sentinel Agent support (fraud risk scoring)
- Trajectory Agent support (product recommendations)

CHANGELOG v2:
  - Imports MERCHANT_RISK, FLAG_WEIGHTS, EXPECTED_SLA, DEPT_NAMES,
    RISK_THRESHOLDS, PRODUCT_THRESHOLDS, CAR_LOAN_SIGNAL_WEIGHTS from
    policy_generator.py (single source of truth — eliminates drift)
  - calculate_fraud_risk(): replaced substring-based flag guessing with
    exact-key lookup against FLAG_WEIGHTS dict (matches dataset exactly)
  - calculate_fraud_risk(): replaced unofficial merchant categories
    ('crypto', 'gambling') with exact dataset categories from MERCHANT_RISK
  - validate_product_recommendation(): thresholds now read from
    PRODUCT_THRESHOLDS and CAR_LOAN_SIGNAL_WEIGHTS constants
  - detect_complaint_category(): SLA hours now read from EXPECTED_SLA,
    department names from DEPT_NAMES — no hardcoded duplicates
  - validate_against_dataset(): dataset path defaults to COMPLAINTS_CSV
    from policy_generator module

Author: AI Engineer 2 (Security & Knowledge Specialist)
Date: February 2026
"""

from typing import List, Dict, Optional, Tuple
import logging
from pathlib import Path
import re

from .chromadb_config import initialize_chromadb, ChromaDBConfig

# =============================================================================
# IMPORT SHARED CONSTANTS FROM POLICY_GENERATOR
# =============================================================================
# These are the canonical values used by data_generator.py, policy documents,
# and the dataset validator. Importing here ensures rag_query.py never drifts
# from the policy definitions.
from ..knowledge_base.generate_policies import (
    MERCHANT_RISK,           # FRM-002 Section 1 — merchant category risk weights
    FLAG_WEIGHTS,            # FRM-001 Section 1 — fraud trace flag weights
    EXPECTED_SLA,            # POL-CCH-001 Section 2 — SLA hours per department
    DEPT_NAMES,              # POL-CCH-001 Section 2 — full department names
    RISK_THRESHOLDS,         # FRM-001 Section 2.2 — score bands → risk levels
    PRODUCT_THRESHOLDS,      # PRS-001 Section 1  — product eligibility thresholds
    CAR_LOAN_SIGNAL_WEIGHTS, # PRS-001 Section 2  — signal score component weights
    COMPLAINTS_CSV,          # Dataset path constant for validate_against_dataset
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RAGQueryEngine:
    """
    RAG-powered query engine for banking policy questions.
    
    Key Features:
    - Semantic similarity search (not just keyword matching)
    - Multi-document retrieval for comprehensive answers
    - Source citation for every claim
    - Confidence scoring to detect when answer is uncertain
    - Grounding enforcement (refuses to hallucinate)
    - Agent-specific query methods for Dispatcher, Sentinel, Trajectory

    All fraud weights, SLA values, product thresholds, and department codes
    are imported from policy_generator.py. Do NOT redefine them locally in
    any method — always use the imported constants.
    """
    
    # Retrieval parameters
    DEFAULT_TOP_K = 5          # Number of chunks to retrieve
    RELEVANCE_THRESHOLD = 0.5  # Minimum similarity score (0–1)
    HIGH_CONFIDENCE_THRESHOLD = 0.75  # Score for "high confidence" answers
    
    # Department mapping — canonical codes from POL-CCH-001
    # Keys cover both full names and codes so either can be matched in answer text.
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
    
    def __init__(self, client, config: ChromaDBConfig):
        """
        Initialize RAG query engine.
        
        Args:
            client: ChromaDB client instance
            config: ChromaDB configuration
        """
        self.client = client
        self.config = config
        
        # Load collections
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
        
        logger.info("RAG Query Engine initialized successfully")
    
    def query(self, 
             question: str, 
             collection_name: Optional[str] = None,
             top_k: int = DEFAULT_TOP_K,
             include_metadata: bool = True) -> Dict:
        """
        Query the knowledge base with semantic search.
        
        Args:
            question: User's question
            collection_name: Specific collection to search (None = search all)
            top_k: Number of relevant chunks to retrieve
            include_metadata: Whether to include chunk metadata in results
            
        Returns:
            Dictionary containing:
            - answer: Synthesized answer (or None if no relevant docs)
            - sources: List of source documents with citations
            - confidence: Confidence score (0-1)
            - grounded: Whether answer is based on retrieved docs
        """
        logger.info(f"Query received: {question[:100]}...")
        
        # Select collection to search
        if collection_name:
            collection = self.client.get_collection(collection_name)
        else:
            collection = self.all_collection
        
        # Perform semantic search
        results = collection.query(
            query_texts=[question],
            n_results=top_k,
            include=['documents', 'metadatas', 'distances']
        )
        
        # Process results
        if not results['documents'] or not results['documents'][0]:
            logger.warning(f"No relevant documents found for query: {question}")
            return {
                'answer': None,
                'sources': [],
                'confidence': 0.0,
                'grounded': False,
                'message': "I cannot find relevant information in the knowledge base to answer this question."
            }
        
        # Extract retrieved chunks
        retrieved_chunks = []
        for i in range(len(results['documents'][0])):
            chunk = {
                'content': results['documents'][0][i],
                'metadata': results['metadatas'][0][i] if include_metadata else {},
                'distance': results['distances'][0][i],
                'similarity': 1 - results['distances'][0][i]  # Convert distance to similarity
            }
            retrieved_chunks.append(chunk)
        
        # Filter by relevance threshold
        relevant_chunks = [
            chunk for chunk in retrieved_chunks 
            if chunk['similarity'] >= self.RELEVANCE_THRESHOLD
        ]
        
        if not relevant_chunks:
            logger.warning(f"No chunks above relevance threshold for: {question}")
            return {
                'answer': None,
                'sources': [],
                'confidence': 0.0,
                'grounded': False,
                'message': "The information I found is not relevant enough to answer this question confidently."
            }
        
        # Calculate overall confidence
        avg_similarity = sum(c['similarity'] for c in relevant_chunks) / len(relevant_chunks)
        confidence = min(avg_similarity, 1.0)
        
        # Synthesize answer from chunks
        answer = self._synthesize_answer(question, relevant_chunks)
        
        # Prepare source citations
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
    
    def _synthesize_answer(self, question: str, chunks: List[Dict]) -> str:
        """
        Synthesize answer from retrieved chunks.
        
        Args:
            question: Original question
            chunks: Retrieved relevant chunks
            
        Returns:
            Synthesized answer string
        """
        # Extract most relevant chunk as primary answer
        most_relevant = chunks[0]
        
        # Simple extractive answer (first relevant paragraph)
        answer_parts = []
        
        # Get the most relevant section
        content = most_relevant['content']
        paragraphs = content.split('\n\n')
        
        # Find paragraph that contains key question words
        question_words = set(question.lower().split())
        
        best_paragraph = None
        best_overlap = 0
        
        for para in paragraphs:
            if len(para) < 50:  # Skip headers and tiny fragments
                continue
            
            para_words = set(para.lower().split())
            overlap = len(question_words & para_words)
            
            if overlap > best_overlap:
                best_overlap = overlap
                best_paragraph = para
        
        if best_paragraph:
            answer_parts.append(best_paragraph.strip())
        else:
            # Fallback to first substantial paragraph
            for para in paragraphs:
                if len(para) > 100:
                    answer_parts.append(para.strip())
                    break
        
        # Add supporting information from other chunks
        if len(chunks) > 1:
            # Add relevant snippet from second chunk if different source
            second_chunk = chunks[1]
            if second_chunk['metadata'].get('source_document') != most_relevant['metadata'].get('source_document'):
                second_paragraphs = second_chunk['content'].split('\n\n')
                for para in second_paragraphs:
                    if len(para) > 100:
                        answer_parts.append(f"\n\nAdditional context: {para.strip()}")
                        break
        
        answer = '\n\n'.join(answer_parts)
        
        # Truncate if too long (keep under 500 words for readability)
        words = answer.split()
        if len(words) > 500:
            answer = ' '.join(words[:500]) + "... [Additional details available in source documents]"
        
        return answer
    
    def _prepare_citations(self, chunks: List[Dict]) -> List[Dict]:
        """
        Prepare source citations for transparency.
        
        Args:
            chunks: Retrieved chunks with metadata
            
        Returns:
            List of citation dictionaries
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
                'snippet': chunk['content'][:200] + "..." if len(chunk['content']) > 200 else chunk['content']
            }
            
            citations.append(citation)
        
        return citations
    
    # ========================================================================
    # DISPATCHER AGENT METHODS
    # ========================================================================
    
    def extract_department_code(self, answer: str) -> str:
        """
        Extract department code from RAG answer.
        
        Useful for validating routing decisions against complaints.csv.
        
        Args:
            answer: RAG-generated answer text
            
        Returns:
            Department code (TSU, COC, FRM, DCS, AOD, CLS) or 'UNKNOWN'
            
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
    
    def detect_complaint_category(self, complaint_text: str) -> Dict:
        """
        Detect complaint category from text for Dispatcher routing.
        
        Analyzes complaint text against POL-CCH-001 categories and returns
        detailed routing information with confidence scores.

        SLA hours are read from the EXPECTED_SLA constant imported from
        policy_generator.py — not hardcoded locally.

        Aligned with complaints.csv structure:
        - department_code  : TSU | COC | FRM | DCS | AOD | CLS
        - priority_level   : Critical | High | Medium | Low
        - sla_hours_limit  : per EXPECTED_SLA constant

        Args:
            complaint_text: The customer's complaint (from complaints.csv)
            
        Returns:
            {
                'primary_category': str,
                'department_code': str,
                'department_name': str,
                'priority_level': str,
                'sla_hours': int,
                'confidence': float,
                'reasoning': str,
                'sources': list
            }
        """
        # Query for category and routing
        category_result = self.query(
            f"Which department should handle this complaint: {complaint_text}",
            collection_name="bank_policies",
            top_k=5
        )
        
        if not category_result['grounded']:
            return {
                'primary_category': 'unknown',
                'department_code': 'UNKNOWN',
                'department_name': 'Unknown',
                'priority_level': 'Medium',
                'sla_hours': EXPECTED_SLA.get('TSU', 48),  # safe default
                'confidence': 0.0,
                'reasoning': 'No relevant policy found',
                'sources': []
            }
        
        # Extract department code from RAG answer
        dept_code = self.extract_department_code(category_result['answer'])

        # SLA hours from shared constant — no hardcoding, no drift
        sla_hours = EXPECTED_SLA.get(dept_code, 48)

        # Determine priority from complaint text + department
        priority = self._determine_priority(complaint_text, dept_code)
        
        # Extract complaint category label
        primary_category = self._extract_category(category_result['answer'])
        
        return {
            'primary_category': primary_category,
            'department_code': dept_code,
            'department_name': DEPT_NAMES.get(dept_code, 'Unknown'),  # from shared constant
            'priority_level': priority,
            'sla_hours': sla_hours,
            'confidence': category_result['confidence'],
            'reasoning': category_result['answer'][:300] + '...',
            'sources': category_result['sources']
        }
    
    def _determine_priority(self, complaint_text: str, dept_code: str) -> str:
        """
        Determine priority level from complaint text and department.
        Aligned with POL-CCH-001 Section 3 priority criteria.
        """
        complaint_lower = complaint_text.lower()
        
        # FRM always Critical — fraud is highest priority (POL-CCH-001 §3)
        if dept_code == 'FRM':
            return 'Critical'

        # Critical keywords
        if any(w in complaint_lower for w in ['fraud', 'unauthorized', 'hacked', 'stolen', 'scam']):
            return 'Critical'
        
        # High priority keywords (POL-CCH-001 §3)
        if any(w in complaint_lower for w in ['declined', 'swallowed', 'retention', 'blocked']):
            return 'High'
        if any(p in complaint_lower for p in ['not received', 'failed transfer']):
            return 'High'
        
        # Low priority keywords
        if any(w in complaint_lower for w in ['statement', 'balance', 'inquiry']):
            return 'Low'
        
        return 'Medium'
    
    def _extract_category(self, answer: str) -> str:
        """Extract complaint category label from RAG answer text."""
        categories = {
            'transaction': 'transaction_dispute',
            'transfer':    'transaction_dispute',
            'card':        'card_issue',
            'atm':         'card_issue',
            'fraud':       'fraud_security',
            'unauthorized':'fraud_security',
            'app':         'digital_banking',
            'login':       'digital_banking',
            'account':     'account_services',
            'statement':   'account_services',
            'loan':        'credit_services'
        }
        answer_lower = answer.lower()
        for keyword, category in categories.items():
            if keyword in answer_lower:
                return category
        return 'general_inquiry'

    # ========================================================================
    # SENTINEL AGENT METHODS
    # ========================================================================
    
    def calculate_fraud_risk(self, transaction: Dict) -> Dict:
        """
        Calculate comprehensive fraud risk score from transaction data.

        Uses FLAG_WEIGHTS (FRM-001) and MERCHANT_RISK (FRM-002) constants
        imported from policy_generator.py — exact-key lookup, no substring
        guessing. Merchant categories are the nine exact values in the dataset.

        Expected transaction dict fields (from transactions.csv):
          fraud_explainability_trace : str  e.g. "mobile_channel_risk,high_amount_spike"
          amount                     : float
          transaction_timestamp      : str  ISO format
          channel                    : str  e.g. "mobile_app"
          merchant_category          : str  e.g. "fintech"
          merchant_name              : str  e.g. "Paystack"
          is_fraud_score             : int  0 or 1
          transaction_status         : str  e.g. "failed"

        Risk scoring formula (FRM-001 §2 + FRM-002 §1):
          total = SUM(FLAG_WEIGHTS for each flag in fraud_explainability_trace)
                + MERCHANT_RISK[merchant_category]
                capped at 100

        Returns:
            {
                'total_risk_score'  : int (0-100),
                'risk_level'        : str (LOW | MEDIUM | HIGH | CRITICAL),
                'risk_breakdown'    : dict,
                'recommended_action': str,
                'requires_challenge': bool,
                'should_block'      : bool,
                'policy_explanation': str,
                'sources'           : list,
                'confidence'        : float
            }
        """
        risk_breakdown = {
            'flag_score':     0,   # From fraud_explainability_trace via FLAG_WEIGHTS
            'merchant_risk':  0,   # From merchant_category via MERCHANT_RISK
            'timing_risk':    0,   # Odd-hour bonus (FRM-001 §1.1)
        }

        # ------------------------------------------------------------------
        # 1. Parse fraud trace flags — exact-key lookup against FLAG_WEIGHTS
        #    Keys: "mobile_channel_risk", "high_amount_spike",
        #          "multiple_failures", "normal_pattern"
        # ------------------------------------------------------------------
        trace = str(transaction.get('fraud_explainability_trace', 'normal_pattern'))
        for flag in trace.split(','):
            flag = flag.strip()
            points = FLAG_WEIGHTS.get(flag, 0)   # 0 for unrecognised flags
            risk_breakdown['flag_score'] += points

        # ------------------------------------------------------------------
        # 2. Merchant category risk — exact-key lookup against MERCHANT_RISK
        #    Valid keys: fintech, transport, education, healthcare, telecoms,
        #                supermarket, restaurants, fuel, utilities
        # ------------------------------------------------------------------
        merchant_category = str(transaction.get('merchant_category', '')).lower().strip()
        risk_breakdown['merchant_risk'] = MERCHANT_RISK.get(merchant_category, 0)

        # ------------------------------------------------------------------
        # 3. Timing risk — odd-hour bonus (FRM-001 §1.1: 12 AM–5 AM WAT)
        #    Only applied for high-value transactions (≥ ₦100,000)
        # ------------------------------------------------------------------
        timestamp = str(transaction.get('transaction_timestamp', ''))
        amount = float(transaction.get('amount', 0))
        if timestamp:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(timestamp.replace(' ', 'T'))
                if 0 <= dt.hour < 5 and amount >= 100_000:
                    risk_breakdown['timing_risk'] += 20
            except (ValueError, TypeError):
                pass

        # ------------------------------------------------------------------
        # 4. Compute total, cap at 100
        # ------------------------------------------------------------------
        total_risk = min(sum(risk_breakdown.values()), 100)

        # ------------------------------------------------------------------
        # 5. Map to risk level and action using RISK_THRESHOLDS
        #    Thresholds: LOW 0-30, MEDIUM 31-60, HIGH 61-85, CRITICAL 86-100
        # ------------------------------------------------------------------
        risk_level = 'LOW'
        for level, (low, high) in RISK_THRESHOLDS.items():
            if low <= total_risk <= high:
                risk_level = level
                break

        # Action and flags per FRM-001 §2.2
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
        # 6. Retrieve policy explanation from RAG knowledge base
        # ------------------------------------------------------------------
        explanation_query = self.query(
            f"Explain the fraud risk assessment for a {risk_level} risk score "
            f"of {total_risk} and the recommended action for the Sentinel Agent",
            collection_name="bank_policies",
            top_k=3
        )

        return {
            'total_risk_score':   total_risk,
            'risk_level':         risk_level,
            'risk_breakdown':     risk_breakdown,
            'recommended_action': action,
            'requires_challenge': requires_challenge,
            'should_block':       should_block,
            'policy_explanation': (explanation_query['answer']
                                   if explanation_query['answer']
                                   else f"Risk score {total_risk}/100 → {risk_level}: {action}"),
            'sources':            explanation_query['sources'],
            'confidence':         explanation_query['confidence']
        }
    
    # ========================================================================
    # TRAJECTORY AGENT METHODS
    # ========================================================================
    
    def validate_product_recommendation(self, customer_data: Dict, 
                                        recommended_product: str) -> Dict:
        """
        Validate product recommendation against PRS-001 policy criteria.

        Thresholds are read from PRODUCT_THRESHOLDS and CAR_LOAN_SIGNAL_WEIGHTS
        imported from policy_generator.py. Do NOT hardcode ₦ thresholds here.

        Applies the strict PRS-001 Section 1 hierarchy:
          Step 1: monthly_inflow > ₦2,000,000 → Investment Plan
          Step 2: car_loan_signal_score >= 0.7 → Car Loan
          Step 3: salary_detected AND monthly_inflow > ₦300,000 → Personal Loan
          Default: None

        Expected customer_data fields (from joined datasets):
          monthly_inflow      : float (sum of credit txns for this customer)
          salary_detected     : bool  (salary_tracker >= 2)
          car_loan_signal_score: float (0.0–1.0, from transactions.csv)
          age                 : int   (from customers.csv)

        Args:
            customer_data       : Dict with customer attributes (see above)
            recommended_product : str — "Car Loan" | "Personal Loan" | "Investment Plan"
            
        Returns:
            {
                'is_eligible'      : bool,
                'confidence'       : float,
                'met_criteria'     : list[str],
                'unmet_criteria'   : list[str],
                'recommendation'   : str,
                'policy_basis'     : str,
                'sources'          : list,
                'hierarchy_step'   : int   (1 | 2 | 3 | 0 for None)
            }
        """
        # Query RAG for policy text grounding
        policy_result = self.query(
            f"What are the eligibility criteria and thresholds for {recommended_product} "
            f"per PRS-001 product recommendation policy?",
            collection_name="bank_policies",
            top_k=5
        )
        
        if not policy_result['grounded']:
            return {
                'is_eligible':    False,
                'confidence':     0.0,
                'met_criteria':   [],
                'unmet_criteria': ['No policy found in knowledge base'],
                'recommendation': 'CANNOT_VALIDATE',
                'policy_basis':   'No policy available',
                'sources':        [],
                'hierarchy_step': 0
            }
        
        # ------------------------------------------------------------------
        # Extract customer attributes
        # ------------------------------------------------------------------
        monthly_inflow  = float(customer_data.get('monthly_inflow', 0))
        salary_detected = bool(customer_data.get('salary_detected', False))
        car_loan_score  = float(customer_data.get('car_loan_signal_score', 0))
        age             = int(customer_data.get('age', 0))

        # Read thresholds from shared constants — no local magic numbers
        inv_min    = PRODUCT_THRESHOLDS['Investment Plan']['monthly_inflow_min']
        cl_score   = PRODUCT_THRESHOLDS['Car Loan']['car_loan_signal_score_min']
        pl_min     = PRODUCT_THRESHOLDS['Personal Loan']['monthly_inflow_min']
        uber_min   = CAR_LOAN_SIGNAL_WEIGHTS['uber_tracker_min']
        inflow_min = CAR_LOAN_SIGNAL_WEIGHTS['monthly_inflow_min']

        met   = []
        unmet = []
        hierarchy_step = 0

        # ------------------------------------------------------------------
        # Validate criteria for the specified product
        # ------------------------------------------------------------------
        if recommended_product == "Investment Plan":
            hierarchy_step = 1
            if monthly_inflow > inv_min:
                met.append(f'Monthly inflow ₦{monthly_inflow:,.0f} > threshold ₦{inv_min:,.0f}')
            else:
                unmet.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} ≤ ₦{inv_min:,.0f} '
                    f'(gap: ₦{inv_min - monthly_inflow:,.0f})'
                )

        elif recommended_product == "Car Loan":
            hierarchy_step = 2
            # Step 1 must NOT fire (otherwise Investment Plan takes priority)
            if monthly_inflow > inv_min:
                unmet.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} > ₦{inv_min:,.0f} '
                    f'→ Investment Plan should take priority (PRS-001 Step 1)'
                )
            # Car loan signal score
            if car_loan_score >= cl_score:
                met.append(
                    f'car_loan_signal_score {car_loan_score:.2f} ≥ threshold {cl_score}'
                )
            else:
                unmet.append(
                    f'car_loan_signal_score {car_loan_score:.2f} < threshold {cl_score} '
                    f'(gap: {cl_score - car_loan_score:.2f}). '
                    f'Need: Uber ≥{uber_min} trips (+{CAR_LOAN_SIGNAL_WEIGHTS["uber_tracker_score"]}) '
                    f'OR salary_detected (+{CAR_LOAN_SIGNAL_WEIGHTS["salary_detected_score"]}) '
                    f'OR inflow > ₦{inflow_min:,.0f} (+{CAR_LOAN_SIGNAL_WEIGHTS["monthly_inflow_score"]})'
                )
            # Salary signal (informational — not blocking for Car Loan)
            if salary_detected:
                met.append('Salary detected (2+ fintech credits > ₦200,000)')
            else:
                met.append('Note: salary not detected — inflow/transport signals may still qualify')

        elif recommended_product == "Personal Loan":
            hierarchy_step = 3
            # Steps 1 and 2 must NOT fire
            if monthly_inflow > inv_min:
                unmet.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} > ₦{inv_min:,.0f} '
                    f'→ Investment Plan should take priority (PRS-001 Step 1)'
                )
            if car_loan_score >= cl_score:
                unmet.append(
                    f'car_loan_signal_score {car_loan_score:.2f} ≥ {cl_score} '
                    f'→ Car Loan should take priority (PRS-001 Step 2)'
                )
            # Personal Loan mandatory criteria (PRS-001 §3)
            if salary_detected:
                met.append('Salary detected (salary_tracker ≥ 2)')
            else:
                unmet.append('salary_detected = False (need 2+ fintech credits > ₦200,000)')
            if monthly_inflow > pl_min:
                met.append(f'Monthly inflow ₦{monthly_inflow:,.0f} > ₦{pl_min:,.0f}')
            else:
                unmet.append(
                    f'Monthly inflow ₦{monthly_inflow:,.0f} ≤ ₦{pl_min:,.0f} '
                    f'(gap: ₦{pl_min - monthly_inflow:,.0f})'
                )
        else:
            unmet.append(f'Unknown product "{recommended_product}" — not in PRS-001')

        is_eligible = len(unmet) == 0

        return {
            'is_eligible':    is_eligible,
            'confidence':     policy_result['confidence'],
            'met_criteria':   met,
            'unmet_criteria': unmet,
            'recommendation': 'APPROVED' if is_eligible else 'NOT_ELIGIBLE',
            'policy_basis':   policy_result['answer'],
            'sources':        policy_result['sources'],
            'hierarchy_step': hierarchy_step
        }
    
    # ========================================================================
    # BATCH PROCESSING & VALIDATION
    # ========================================================================
    
    def batch_query(self, questions: List[str], top_k: int = 3, 
                   show_progress: bool = True) -> List[Dict]:
        """
        Process multiple questions in batch with optional progress tracking.
        
        Optimized for testing large datasets (e.g., your complaints.csv).
        
        Args:
            questions: List of questions to process
            top_k: Number of chunks per question
            show_progress: Whether to show progress bar
            
        Returns:
            List of query results (same length as questions)
        """
        results = []
        
        if show_progress:
            try:
                from tqdm import tqdm
                iterator = tqdm(questions, desc="Processing")
            except ImportError:
                logger.warning("tqdm not installed. Install for progress bars: pip install tqdm")
                iterator = questions
        else:
            iterator = questions
        
        for question in iterator:
            result = self.query(question, top_k=top_k)
            results.append(result)
        
        logger.info(f"Processed {len(questions)} queries in batch")
        return results
    
    def validate_against_dataset(self,
                                 complaints_csv: str = None,
                                 sample_size: int = 100) -> Dict:
        """
        Validate RAG routing accuracy against complaints dataset.

        Defaults to COMPLAINTS_CSV path from policy_generator module.
        Pass an explicit path only if testing a different file.

        Tests against complaints.csv structure:
          complaint_text   : str
          department_code  : str (TSU | COC | FRM | DCS | AOD | CLS)
          priority_level   : str (Critical | High | Medium | Low)
          sla_breach_flag  : int (0 | 1)
        
        Args:
            complaints_csv: Path to complaints.csv (default: COMPLAINTS_CSV)
            sample_size: Number of complaints to test
            
        Returns:
            Dictionary with validation results
        """
        import pandas as pd

        # Use shared constant as default — keeps path in sync with rest of system
        csv_path = complaints_csv or str(COMPLAINTS_CSV)
        
        # Load dataset
        complaints_df = pd.read_csv(csv_path)
        
        # Sample
        if sample_size < len(complaints_df):
            sample = complaints_df.sample(n=sample_size, random_state=42)
        else:
            sample = complaints_df
        
        correct_dept     = 0
        correct_priority = 0
        misrouted        = []
        confidences      = []
        
        print(f"Testing {len(sample)} complaints from: {csv_path}")
        
        for idx, complaint in sample.iterrows():
            result = self.detect_complaint_category(complaint['complaint_text'])
            
            # Department accuracy
            if result['department_code'] == complaint['department_code']:
                correct_dept += 1
            else:
                misrouted.append({
                    'complaint_id':  complaint.get('complaint_id', idx),
                    'complaint_text': complaint['complaint_text'][:100] + '...',
                    'expected_dept': complaint['department_code'],
                    'got_dept':      result['department_code'],
                    'confidence':    result['confidence']
                })
            
            # Priority accuracy
            if result['priority_level'] == complaint.get('priority_level', 'Medium'):
                correct_priority += 1
            
            confidences.append(result['confidence'])
        
        dept_accuracy     = (correct_dept / len(sample)) * 100
        priority_accuracy = (correct_priority / len(sample)) * 100
        avg_confidence    = sum(confidences) / len(confidences)
        
        return {
            'department_accuracy':  dept_accuracy,
            'priority_accuracy':    priority_accuracy,
            'total_tested':         len(sample),
            'correct_departments':  correct_dept,
            'correct_priorities':   correct_priority,
            'misrouted':            misrouted[:10],
            'average_confidence':   avg_confidence
        }
    
    def get_collection_info(self) -> Dict:
        """
        Get information about loaded collections.
        
        Returns:
            Dictionary with collection statistics
        """
        return {
            'policies': {
                'count': self.policy_collection.count(),
                'name':  self.config.COLLECTION_POLICIES
            },
            'faqs': {
                'count': self.faq_collection.count(),
                'name':  self.config.COLLECTION_FAQS
            },
            'all_documents': {
                'count': self.all_collection.count(),
                'name':  self.config.COLLECTION_ALL
            }
        }


# =============================================================================
# INTERACTIVE DEMO
# =============================================================================

def interactive_query_demo():
    """Interactive demo of RAG query system"""
    print("\n" + "="*70)
    print(" "*15 + "RAG QUERY SYSTEM - INTERACTIVE DEMO")
    print("="*70 + "\n")
    
    # Initialize
    print("Initializing RAG query engine...")
    client, config = initialize_chromadb()
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
            result = engine.query(question)
            
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


def run_test_queries():
    """Run test queries to demonstrate RAG capabilities"""
    print("\n" + "="*70)
    print(" "*15 + "RAG SYSTEM TEST QUERIES")
    print("="*70 + "\n")
    
    # Initialize engine
    client, config = initialize_chromadb()
    engine = RAGQueryEngine(client, config)
    
    # Test questions covering all six policy documents
    test_questions = [
        # POL-CCH-001 — Dispatcher
        "What is the SLA for transaction disputes?",
        "Which department handles card retention issues?",
        # FRM-001 — Sentinel base flags
        "How are fraud cases handled?",
        "What risk score triggers an account freeze?",
        # FRM-002 — Merchant risk
        "Why is a fintech merchant transaction considered high risk?",
        # TSU-POL-002 — Transaction policies
        "What is the daily limit for Tier 2 accounts?",
        "How long does it take to reverse a failed NIP transfer?",
        # PRS-001 — Trajectory
        "What criteria must a customer meet for a Car Loan recommendation?",
        "How is salary_detected determined from transactions?",
    ]
    
    print(f"Running {len(test_questions)} test queries...\n")
    
    results = []
    for i, question in enumerate(test_questions, 1):
        print(f"[{i}/{len(test_questions)}] {question}")
        result = engine.query(question, top_k=3)
        results.append(result)
        
        if result['answer']:
            print(f"  ✓ Answered — confidence: {result['confidence']:.1%}")
        else:
            print(f"  ✗ No relevant information found")
    
    # Summary statistics
    print("\n" + "="*70)
    print("TEST RESULTS SUMMARY")
    print("="*70)
    
    answered        = sum(1 for r in results if r['answer'])
    high_confidence = sum(1 for r in results if r['confidence'] > 0.75)
    avg_confidence  = sum(r['confidence'] for r in results) / len(results)
    
    print(f"\nQuestions answered    : {answered}/{len(test_questions)} ({answered/len(test_questions):.1%})")
    print(f"High confidence (>75%): {high_confidence}/{answered}")
    print(f"Average confidence    : {avg_confidence:.1%}")
    print("\n" + "="*70)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    """
    Run in test mode or interactive mode.
    
    Usage:
        python rag_query.py          # Interactive demo
        python rag_query.py --test   # Run test queries
    """
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        run_test_queries()
    else:
        interactive_query_demo()