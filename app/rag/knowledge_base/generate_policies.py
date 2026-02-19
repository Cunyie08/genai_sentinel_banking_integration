"""
Project: AI-Driven Banking Middleware (Capstone)
Organization: The Sentinels / AI Fellowship NCC
Role: AI Engineer 2 - Week 1 Deliverable
Document: Full-Scale Grounding Policy Engine (Enhanced Production Version)

This module generates comprehensive, production-grade banking policy documents
for RAG (Retrieval-Augmented Generation) knowledge base ingestion.

Purpose:
- Provide grounding truth for AI agents (zero hallucination)
- Enable precise complaint routing via Dispatcher Agent
- Support fraud detection via Sentinel Agent
- Ensure policy compliance and SLA adherence

Key Features:
- Enterprise-grade policy content with Nigerian banking context
- RAG-optimized structure for semantic search
- Comprehensive metadata tracking for traceability
- Citation-ready format for transparent AI responses

Author: AI Engineer 2 (Security & Knowledge Specialist)
Date: February 2026
"""

import json
import uuid
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path


class BankingPolicyGenerator:
    """
    Enterprise-grade generator for comprehensive banking policies.
    
    This class generates four core policy documents that serve as the
    "ground truth" for the RAG-based AI middleware system:
    
    1. Complaint Handling Policy (POL-CCH-001)
       - Department routing rules
       - SLA requirements
       - Escalation procedures
       
    2. Fraud Detection Guidelines (FRM-001)
       - Risk scoring framework
       - Red flags and indicators
       - Response protocols
       
    3. Transaction Processing Policies (TSU-POL-002)
       - KYC tier limits
       - Fee structures
       - Reversal policies
       
    4. Customer Service FAQ (FAQ-001)
       - Common issues and solutions
       - Self-service guidance
       - Troubleshooting steps
    
    Usage:
        generator = BankingPolicyGenerator(bank_name="Sentinel Bank Nigeria")
        generator.save_all_policies(Path("./knowledge_base"))
    """
    
    def __init__(self, bank_name: str = "Sentinel Bank Nigeria"):
        """
        Initialize the policy generator with institutional identity.
        
        Args:
            bank_name (str): Name of the financial institution.
                           Used throughout policy documents for branding.
        
        Sets up:
            - Bank name for all documents
            - Generation timestamp for version control
            - System metadata for RAG traceability
        """
        self.bank_name = bank_name
        self.generation_time = datetime.now().isoformat()
        self.display_date = datetime.now().strftime('%B %Y')
        
        # System-level metadata for RAG traceability and academic compliance
        # This ensures every document can be tracked back to its source
        self.system_meta = {
            "project": "AI-Driven Banking Middleware",
            "organization": "The Sentinels / AI Fellowship NCC",
            "jurisdiction": "Nigeria",
            "version": "2026.Q1.v1-COMPLETE",
            "data_classification": "Synthetic/Proprietary"
        }

    def _package_for_rag(self, doc_id: str, title: str, category: str, 
                        version: str, text: str) -> Dict[str, Any]:
        """
        Package document with standardized metadata for RAG ingestion.
        
        This helper method ensures every document has consistent structure
        for vector database indexing and retrieval.
        
        Args:
            doc_id (str): Unique document identifier (e.g., "POL-CCH-001")
            title (str): Human-readable document title
            category (str): Document type (policy, security, operations, knowledge_base)
            version (str): Version number (e.g., "2.1", "4.0")
            text (str): Full document content
            
        Returns:
            Dict containing:
                - document_id: For indexing
                - title: For display
                - category: For filtering
                - version: For version control
                - metadata: Rich metadata for RAG context
                - content: The actual policy text
                - last_updated: Timestamp
        
        The structure supports:
            - Semantic search across documents
            - Source citation in AI responses
            - Version tracking for policy updates
            - Category-based filtering
        """
        return {
            "document_id": doc_id,
            "title": title,  
            "category": category,  
            "version": version,  
            "metadata": {
                **self.system_meta,  # Include project-level metadata
                "title": title,
                "category": category,
                "version": version,
                "uuid": str(uuid.uuid4()),  # Unique ID for this generation
                "last_modified": self.generation_time
            },
            "content": text.strip(),
            "last_updated": self.generation_time 
        }

    def generate_complaint_handling_policy(self) -> Dict:
        """
        Generate comprehensive complaint handling and routing policy.
        
        This is THE MOST CRITICAL document for the Dispatcher Agent.
        It defines exactly which department handles which type of complaint,
        ensuring >95% routing accuracy.
        
        Document Structure:
            - Purpose and scope
            - 6 department routing categories with examples
            - SLA requirements per category
            - 4-tier priority classification
            - 4-level escalation matrix
            - Prohibited actions (critical for compliance)
            - Documentation requirements
        
        Target Audience:
            - Dispatcher Agent (primary consumer)
            - Customer service representatives
            - Service desk supervisors
            
        Returns:
            Dict: Packaged document ready for RAG ingestion
            
        RAG Usage Pattern:
            Query: "Which department handles transfer disputes?"
            Retrieved Section: "2.1 Transaction Disputes -> TSU"
            Agent Action: Route to Transaction Services Unit
        """
        policy_content = f"""
=========================================================================
{self.bank_name} - CUSTOMER COMPLAINT HANDLING & ROUTING POLICY
=========================================================================

**Document ID**: POL-CCH-001
**Version**: 2.1  
**Effective Date**: January 2025  
**Classification**: Internal Use Only
**Review Cycle**: Quarterly

=========================================================================
SECTION 1: PURPOSE & SCOPE
=========================================================================

This policy establishes standardized procedures for receiving, classifying,
routing, and resolving customer complaints across all channels.

Key Objectives:
1. Ensure complaints reach the correct department on first contact (>95% accuracy)
2. Maintain consistent SLA compliance across all complaint types
3. Provide transparent escalation paths for unresolved issues
4. Enable AI-driven triage and routing with full auditability

This policy provides the architectural logic for the AI Middleware to solve
the 'misrouting crisis' by ensuring tickets reach the correct subject matter
expert without manual intervention.

Scope:
- All customer-initiated complaints (regardless of channel)
- All complaint types (transactional, operational, technical, service-related)
- All customer segments (retail, SME, corporate, private banking)

Out of Scope:
- Internal operational issues (handled by separate process)
- Regulatory inquiries (escalated to Compliance immediately)
- Media inquiries (escalated to Communications department)


=========================================================================
SECTION 2: COMPLAINT CATEGORIES & DEPARTMENT ROUTING
=========================================================================

This section is CRITICAL for the Dispatcher Agent. Each category maps
complaint types to specific departments with clear examples.


2.1 TRANSACTION DISPUTES
---------------------------------------------------------------------------
**Routing Destination**: Transaction Services Unit (TSU)
**Department Code**: TSU
**Team Size**: 12 specialists + 2 managers

**Complaint Types Handled:**
- Transfer debited from sender but not received by beneficiary
- Duplicate debit transactions (charged twice for same transaction)
- Incorrect transaction amounts (charged ₦50,000 instead of ₦5,000)
- Failed ATM withdrawals with account debited (dispense errors)
- Unauthorized debits from account (excluding fraud - see FRM)
- Missing credit transactions (expected credit not reflected)
- Incorrect transaction reference or narration

**Service Level Agreement (SLA):**
- Initial acknowledgement: Within 2 hours of complaint receipt
- Resolution target: Within 48 hours (business hours)
- Reversal processing: Within 24 hours for confirmed errors
- Complex inter-bank disputes: Up to 14 days (requires NIBSS coordination)

**Escalation Triggers:**
- Unresolved after 72 hours
- Amount exceeds ₦500,000
- Customer threatens regulatory complaint

**Key Decision Criteria:**
If complaint mentions: "transfer", "debit", "not received", "wrong amount",
"reversal", "failed transaction" → Route to TSU

**Examples:**
✓ "I transferred ₦100,000 to GTBank but it didn't reach"
✓ "My account was debited twice for the same payment"
✓ "ATM didn't give me cash but my account shows debit"
✗ "My card was declined at POS" → Route to COC instead


2.2 CARD SERVICES ISSUES
---------------------------------------------------------------------------
**Routing Destination**: Card Operations Center (COC)
**Department Code**: COC
**Team Size**: 8 specialists + 1 manager

**Complaint Types Handled:**
- Card declined at POS or ATM (domestic or international)
- Card retention by ATM machine (card "swallowed")
- Unauthorized card transactions (card fraud)
- Lost or stolen card reporting
- PIN reset requests and PIN-related issues
- Card activation failures (new card won't activate)
- Chip malfunction or magnetic stripe issues
- Contactless payment problems
- Card replacement requests

**Service Level Agreement (SLA):**
- Emergency card blocking: Immediate (within 5 minutes)
- Card replacement: 3-5 business days
- Dispute investigation: 7-14 days (depends on merchant response)
- PIN reset: Same day (within 4 hours)
- Card retrieval from ATM: 24-48 hours

**Escalation Triggers:**
- Card fraud amount exceeds ₦100,000
- Multiple unauthorized transactions
- International card issues (higher complexity)

**Key Decision Criteria:**
If complaint mentions: "card", "PIN", "ATM swallowed", "declined", 
"POS", "chip", "contactless" → Route to COC

**Examples:**
✓ "My card was declined at Shoprite even though I have money"
✓ "ATM machine swallowed my card at GT Bank branch"
✓ "I see card transactions I didn't make"
✗ "My transfer was declined" → Route to TSU instead


2.3 FRAUD & SECURITY CONCERNS
---------------------------------------------------------------------------
**Routing Destination**: Fraud Risk Management (FRM)
**Department Code**: FRM
**Team Size**: 6 specialists + 1 senior manager
**Operating Hours**: 24/7 (including public holidays)

**Complaint Types Handled:**
- Suspected unauthorized account access
- Phishing or fraud attempt reporting
- Account takeover suspicions (profile changes not made by customer)
- Unusual transaction pattern alerts
- Lost/stolen device with banking app access
- SIM swap fraud concerns
- Social engineering scam reporting
- Suspicious beneficiary additions
- Multiple failed login attempts

**Service Level Agreement (SLA):**
- Account freeze capability: Immediate (within 2 minutes)
- Investigation initiation: Within 1 hour of report
- Customer notification: Within 4 hours
- Preliminary assessment: Within 24 hours
- Full resolution: 14-21 days (depends on investigation complexity)
- Police report filing: Within 48 hours (for amounts >₦1M)

**Escalation Triggers:**
- Fraud amount exceeds ₦1,000,000
- Suspected insider involvement
- Multiple customers affected (pattern fraud)
- Media attention or viral social media posts

**Key Decision Criteria:**
If complaint mentions: "fraud", "hacked", "unauthorized access", "scam",
"phishing", "suspicious", "someone used my account" → Route to FRM

**CRITICAL NOTE**: 
Fraud complaints are HIGHEST PRIORITY. Never route fraud cases to IT 
department or delay for documentation. Immediate action can prevent losses.

**Examples:**
✓ "Someone is making transactions in my account that I didn't do"
✓ "I got a call asking for my PIN and now money is missing"
✓ "My account shows login from location I've never been to"
✗ "I was charged twice" → Route to TSU (not fraud, just error)


2.4 DIGITAL BANKING & APP ISSUES
---------------------------------------------------------------------------
**Routing Destination**: Digital Channels Support (DCS)
**Department Code**: DCS
**Team Size**: 10 specialists + 2 managers

**Complaint Types Handled:**
- Mobile app login failures (password/biometric issues)
- Internet banking access problems
- USSD service errors (*737# not working)
- App crashes, freezes, or hangs
- Feature malfunction in digital channels
- Biometric authentication failures (fingerprint/face ID)
- App update issues
- Slow app performance
- Error messages in app
- QR code payment issues

**Service Level Agreement (SLA):**
- Level 1 troubleshooting: Immediate (via phone/chat)
- Technical escalation: Within 4 hours
- Resolution: 24-72 hours (depending on issue complexity)
- Critical app outages: Escalated to IT immediately

**Escalation Triggers:**
- Issue affects multiple customers (systemic problem)
- Security vulnerability suspected
- App completely unusable for customer

**Key Decision Criteria:**
If complaint mentions: "app", "login", "password", "internet banking",
"USSD", "crash", "freeze", "error message", "biometric" → Route to DCS

**Examples:**
✓ "I can't login to the mobile app, it says wrong password"
✓ "The app keeps crashing when I try to transfer money"
✓ "*737# USSD code is not responding"
✗ "I can't complete a transfer because of insufficient funds" → TSU


2.5 ACCOUNT SERVICES
---------------------------------------------------------------------------
**Routing Destination**: Account Operations Department (AOD)
**Department Code**: AOD
**Team Size**: 15 specialists + 3 managers

**Complaint Types Handled:**
- Statement of account requests
- Account balance inquiries (discrepancies)
- Account closure requests
- Service charge disputes
- Account upgrade/downgrade requests
- Name/address update issues
- BVN update or linking issues
- Dormant account reactivation
- Account freezes (non-fraud related)
- Cheque-related issues

**Service Level Agreement (SLA):**
- Statement generation: Within 24 hours
- Balance inquiries: Immediate to 4 hours
- Account modifications: 2-3 business days
- Account closure: 5-7 business days
- BVN updates: 24-48 hours

**Escalation Triggers:**
- Regulatory compliance issues
- High-value customer (private banking)
- Legal or court-mandated actions

**Key Decision Criteria:**
If complaint mentions: "statement", "balance", "close account", "charges",
"upgrade", "update details", "BVN", "dormant" → Route to AOD

**Examples:**
✓ "I need my account statement for last 6 months"
✓ "Why am I being charged ₦50 monthly maintenance fee?"
✓ "I want to close my account"
✗ "I want to check if my transfer reached" → TSU


2.6 LOAN & CREDIT SERVICES
---------------------------------------------------------------------------
**Routing Destination**: Credit & Loan Services (CLS)
**Department Code**: CLS
**Team Size**: 6 specialists + 1 manager

**Complaint Types Handled:**
- Loan disbursement delays
- Interest calculation queries or disputes
- Repayment schedule issues
- Credit limit increase requests
- Loan restructuring requests
- Early repayment/settlement questions
- Loan documentation problems
- Overdraft facility issues

**Service Level Agreement (SLA):**
- Query response: Within 48 hours
- Disbursement issue resolution: 3-5 business days
- Credit review completion: 7-14 days
- Restructuring decisions: 14-21 days

**Escalation Triggers:**
- Loan amount exceeds ₦5,000,000
- Customer threatens default
- Regulatory compliance issues

**Key Decision Criteria:**
If complaint mentions: "loan", "credit", "disbursement", "interest",
"repayment", "overdraft" → Route to CLS

**Examples:**
✓ "My loan was approved but money not yet in my account"
✓ "The interest on my loan seems too high"
✓ "Can I restructure my loan repayment?"
✗ "My salary credit is missing" → TSU


=========================================================================
SECTION 3: COMPLAINT PRIORITY CLASSIFICATION
=========================================================================

Every complaint must be assigned a priority level. This determines
response time and assignment to specialists.


CRITICAL PRIORITY
---------------------------------------------------------------------------
**Response Time**: IMMEDIATE (within 5 minutes)
**Assignment**: Senior specialist + Manager notification + Real-time monitoring

**Criteria:**
- Fraud involving unauthorized transactions (any amount)
- Account access completely blocked (customer cannot transact at all)
- Large sum transfers (>₦500,000) not received by beneficiary
- Security breach suspicions (account takeover)
- SIM swap with simultaneous high-value transaction
- Multiple customer reports of same systemic issue

**Actions Required:**
1. Immediate account review and security assessment
2. Temporary security hold if fraud suspected
3. Senior specialist assignment within 5 minutes
4. Manager notification (via SMS/Slack)
5. Hourly status updates to customer


HIGH PRIORITY
---------------------------------------------------------------------------
**Response Time**: Within 2 hours
**Assignment**: Experienced specialist (minimum 2 years experience)

**Criteria:**
- Card retention by ATM machine
- Failed high-value transactions (₦100,000 - ₦500,000)
- Repeated service failures (same issue occurred 3+ times)
- Business/corporate account issues
- International transaction problems
- Regulatory inquiry imminent

**Actions Required:**
1. Priority queue placement (top 20% of queue)
2. Experienced agent assignment
3. Supervisor notification if SLA at risk
4. 4-hour status updates to customer


MEDIUM PRIORITY
---------------------------------------------------------------------------
**Response Time**: Within 4 hours
**Assignment**: Standard queue, any available specialist

**Criteria:**
- Standard transaction disputes (below ₦100,000)
- App technical issues (non-critical)
- Service charge queries
- Statement requests
- Account information updates
- General card issues (non-fraud)

**Actions Required:**
1. Normal queue processing (FIFO)
2. Standard SLA monitoring
3. 24-hour status updates to customer


LOW PRIORITY
---------------------------------------------------------------------------
**Response Time**: Within 24 hours
**Assignment**: First available agent (can be handled by junior staff)

**Criteria:**
- General inquiries (no issue, just questions)
- Product information requests
- Non-urgent statement requests
- Educational queries ("How do I...?")
- Suggestions or feedback

**Actions Required:**
1. General queue
2. Self-service options offered first (FAQ, knowledge base)
3. Resolution within 48 hours acceptable


=========================================================================
SECTION 4: ESCALATION MATRIX
=========================================================================

Clear escalation paths ensure no complaint falls through the cracks.


LEVEL 1: First Contact Resolution (FCR)
---------------------------------------------------------------------------
**Handler**: Customer Service Representative (CSR) or AI Assistant
**Timeframe**: Within standard SLA for that complaint type
**Success Rate Target**: 70% of complaints resolved at this level

**Characteristics:**
- Straightforward issues with clear resolution path
- No special approvals needed
- Within agent's authority level

**Actions:**
- Attempt immediate resolution
- Document all details in ticketing system
- Set clear expectations with customer
- Provide ticket reference number

**Escalation Trigger**: 
Unable to resolve within SLA OR requires specialist knowledge


LEVEL 2: Specialist Escalation
---------------------------------------------------------------------------
**Handler**: Departmental specialist (expert in specific complaint type)
**Timeframe**: Escalated if Level 1 cannot resolve
**Success Rate Target**: 25% of complaints escalate to this level

**Characteristics:**
- Complex cases requiring deep expertise
- May need coordination with other departments
- Requires investigation or research

**Actions:**
- Deep dive investigation with access to backend systems
- Coordinate with other departments if needed
- Manager informed of escalation (FYI only)
- Regular customer updates (every 24 hours)
- Root cause analysis to prevent recurrence

**Escalation Trigger**:
Unresolved after 48 hours OR customer requests manager OR high-value customer


LEVEL 3: Management Escalation
---------------------------------------------------------------------------
**Handler**: Department manager or team lead
**Timeframe**: Escalated after 7 days unresolved OR high stakes
**Success Rate Target**: 4% of complaints escalate to this level

**Characteristics:**
- Unresolved after reasonable specialist effort
- High-value customer complaints (private banking clients)
- Potential regulatory issues
- Customer threatening legal action
- Media attention or viral social media

**Actions:**
- Management review of entire case history
- Potential policy exception consideration
- Executive summary prepared
- Customer relationship protection measures
- Complaint trend analysis (is this systemic?)

**Escalation Trigger**:
Unresolved after 14 days OR legal implications OR C-suite customer


LEVEL 4: Executive Escalation
---------------------------------------------------------------------------
**Handler**: Executive committee (C-level)
**Timeframe**: Rare escalation for critical issues
**Success Rate Target**: <1% of complaints escalate to this level

**Characteristics:**
- Regulatory body involvement (CBN Consumer Protection)
- Major brand risk or reputational damage
- Legal action filed by customer
- Systemic issue affecting many customers
- Potential financial crime or insider fraud

**Actions:**
- C-level executive review (CEO, COO, CRO)
- Legal department involvement mandatory
- Regulatory compliance check
- Crisis management protocols activated
- Board notification (if material impact)


=========================================================================
SECTION 5: PROHIBITED ACTIONS
=========================================================================

The following actions are STRICTLY FORBIDDEN and can result in
disciplinary action:


1. NEVER ROUTE FRAUD CASES TO IT DEPARTMENT
   → Fraud = FRM department always, no exceptions
   → IT handles technical issues, not security incidents
   

2. NEVER DELAY CRITICAL SECURITY ISSUES FOR DOCUMENTATION
   → Security first, paperwork second
   → Freeze account immediately if fraud suspected, document later
   

3. NEVER PROMISE SPECIFIC RESOLUTION TIMES BEYOND PUBLISHED SLA
   → Under-promise, over-deliver
   → Say "we target 48 hours" not "I'll fix this in 2 hours"
   

4. NEVER SHARE CUSTOMER DETAILS ACROSS UNAUTHORIZED CHANNELS
   → PII protection is mandatory (NDPR compliance)
   → No customer details via WhatsApp, personal email, SMS
   

5. NEVER CLOSE TICKET WITHOUT CUSTOMER CONFIRMATION
   → Customer satisfaction is the success metric
   → Always confirm resolution with customer before closing
   

6. NEVER ROUTE BASED ON CHANNEL INSTEAD OF ISSUE TYPE
   → Issue type determines department, not where it came from
   → Branch complaint about fraud = FRM (not Branch Operations)


=========================================================================
SECTION 6: DOCUMENTATION REQUIREMENTS
=========================================================================

All complaints MUST be logged with complete information:


MANDATORY FIELDS:
---------------------------------------------------------------------------
1. Customer Identification
   - Account number OR BVN (Bank Verification Number)
   - Customer name (as in bank records)
   - Phone number (for callbacks)
   - Email (for written communication)

2. Complaint Details
   - Complaint category (from Section 2)
   - Detailed description (in customer's own words when possible)
   - Transaction reference (if applicable)
   - Amount involved (if applicable)
   - Date/time of incident

3. Channel Information
   - How complaint was received (Call, Email, App, Branch, Social Media)
   - Channel reference ID (call recording ID, email thread ID, etc.)

4. Priority Classification
   - Priority level (Critical, High, Medium, Low)
   - Justification for priority assignment

5. Assignment Details
   - Assigned department code
   - Assigned agent name/ID
   - Assignment timestamp

6. Timeline Tracking
   - Complaint received timestamp
   - Acknowledgement sent timestamp
   - Resolution completed timestamp
   - Customer confirmation timestamp


OPTIONAL BUT RECOMMENDED:
---------------------------------------------------------------------------
- Customer sentiment score (positive, neutral, negative, angry)
- Related transaction IDs (if multiple transactions involved)
- Supporting documents/screenshots (if customer provided)
- Previous related complaints (check history)
- Root cause category (for trend analysis)


=========================================================================
SECTION 7: POLICY OWNERSHIP & GOVERNANCE
=========================================================================

**Policy Owner**: Head of Customer Experience
**Approver**: Chief Operations Officer (COO)
**Review Cycle**: Quarterly (every 3 months)
**Last Updated**: {self.display_date}
**Next Review**: May 2025

**Amendment Process**:
- Proposed changes submitted to Policy Owner
- Impact assessment conducted
- Stakeholder consultation (department heads)
- COO approval required
- All staff training on changes
- Updated policy published within 5 business days

**Compliance Monitoring**:
- Monthly routing accuracy reports
- SLA compliance dashboards
- Customer satisfaction scores (CSAT)
- Escalation rate analysis

**Contact for Questions**:
- Email: customer.experience@sentinelbank.ng
- Phone: +234-1-SENTINEL (Internal: Extension 5000)
- Teams: CustomerExperience channel


=========================================================================
END OF DOCUMENT POL-CCH-001
=========================================================================
"""
        return self._package_for_rag(
            "POL-CCH-001", 
            "Customer Complaint Handling Policy", 
            "policy", 
            "2.1", 
            policy_content
        )
        
    def generate_fraud_detection_guidelines(self) -> Dict:
        """
        Generate exhaustive fraud detection and prevention guidelines.
        
        This document is CRITICAL for the Sentinel Fraud Engine (Week 2).
        It provides the logic for risk scoring, red flag detection, and
        response protocols.
        
        Document Structure:
            - Red flags and fraud indicators (with risk weights)
            - Dynamic risk scoring framework (0-100 scale)
            - Push-to-app authorization procedures
            - Common Nigerian fraud scenarios
            - Response protocols and timelines
        
        Target Audience:
            - Sentinel Agent (primary consumer for Week 2)
            - Fraud Risk Management team
            - Security operations personnel
            
        Returns:
            Dict: Packaged document ready for RAG ingestion
            
        RAG Usage Pattern:
            Query: "What risk score should trigger account freeze?"
            Retrieved Section: "2. Dynamic Risk Scoring -> 86-100 = CRITICAL"
            Agent Action: Block transaction and freeze account
        """
        
        guidelines = f"""
=========================================================================
{self.bank_name} - FRAUD DETECTION & PREVENTION GUIDELINES
=========================================================================

**Document Code**: FRM-001
**Classification**: CONFIDENTIAL
**Version**: 4.0
**Effective Date**: January 2026
**Review Frequency**: Monthly (agile policy refinement due to evolving threats)

**Document Owner**: Chief Risk Officer
**Emergency Contact**: fraud-desk@sentinelbank.ng (Active 24/7)


=========================================================================
SECTION 1: COMPREHENSIVE RED FLAGS FOR FRAUDULENT TRANSACTIONS
=========================================================================

To mitigate fraud before transaction completion, the middleware system
monitors every transaction for the following high-density red flags.

Each red flag has an assigned risk weight (points). Total points determine
risk score (0-100) and subsequent action.


1.1 UNUSUAL TRANSACTIONAL PATTERNS (Behavioral Biometrics)
---------------------------------------------------------------------------

First-time Merchant Relationship
→ Risk Weight: 15 points
→ Definition: Customer has zero historical transaction data with this 
  specific Merchant ID or web gateway
→ Example: First-ever transaction at crypto exchange Binance
→ Rationale: New merchants are higher risk; fraudsters test with new vendors


High-Velocity Transaction Frequency
→ Risk Weight: 25 points
→ Definition: More than 3 successful OR failed transactions within a 
  10-minute window (indicative of 'burst fraud')
→ Example: 5 failed card attempts followed by 1 successful transaction
→ Rationale: Card testing (trying different CVV combinations)


Odd-Hour Activity
→ Risk Weight: 20 points
→ Definition: High-value transactions (≥₦100,000) initiated between 
  12:00 AM and 5:00 AM West Africa Time
→ Example: ₦500,000 transfer at 2:47 AM
→ Rationale: Legitimate customers rarely make large transfers at night
→ Note: Exceptions exist (night shift workers, insomniacs) but pattern is clear


Device Anomaly
→ Risk Weight: 15 points
→ Definition: Transaction attempt from Device ID, IMEI, or MAC address 
  not previously white-listed for this specific account
→ Example: Transaction from brand new iPhone when customer uses Android
→ Rationale: Account takeover often involves fraudster's device


Geospatial Anomaly
→ Risk Weight: 20 points
→ Definition: Transaction location or IP address originating >100km from 
  customer's last 5 known successful login clusters
→ Example: Customer typically transacts in Lagos, sudden transaction from Kano
→ Rationale: Physical impossibility (can't be in two places at once)
→ Note: Check time delta - if last login was yesterday, travel is possible


Suspiciously Round Amounts
→ Risk Weight: 10 points
→ Definition: High-value round-number transactions (e.g., exactly ₦100,000 
  or ₦500,000) which frequently characterize social engineering scams
→ Example: ₦500,000.00 transfer (not ₦499,847.50)
→ Rationale: Scammers often demand round numbers; legitimate bills vary


1.2 High-Risk Transaction Segments
---------------------------------------------------------------------------

International Transfers (No History)
→ Risk Weight: 25 points
→ Definition: Cross-border payments from accounts with no prior history 
  of international spending or travel flags
→ Example: Domestic-only customer suddenly sends $5,000 to USA
→ Rationale: Scammers often trick victims into international transfers


VASP & Crypto Exchanges
→ Risk Weight: 20 points
→ Definition: Large outflows to Virtual Asset Service Providers (Binance, 
  Coinbase, Luno, etc.), particularly from accounts categorized under 
  'Vulnerable' or 'Elderly' customer segments
→ Example: 65-year-old retiree sends ₦2M to Binance
→ Rationale: Crypto is common in romance/investment scams targeting elderly


Gambling & Betting Platform Spikes
→ Risk Weight: 15 points
→ Definition: Sudden volume increases (>300% above 30-day mean) to known 
  gambling gateways (Bet9ja, Sportybet, 1xBet, etc.)
→ Example: Customer averages ₦10K/month gambling, suddenly deposits ₦200K
→ Rationale: Compulsive gambling or account takeover


Unlinked Peer-to-Peer (P2P) Transfers
→ Risk Weight: 20 points
→ Definition: Rapid fund movements to new, unrelated beneficiaries with 
  no shared history in the banking network
→ Example: 5 transfers to 5 different new beneficiaries in one day
→ Rationale: Money mule behavior or fraud cashout pattern


Authentication Failure Cascades
→ Risk Weight: 30 points
→ Definition: Multiple failed biometric or PIN attempts followed 
  immediately by a successful transaction
→ Example: 3 failed fingerprint attempts, then immediate PIN success
→ Rationale: Fraudster trying different auth methods after initial failure


1.3 Account Takeover (ATO) Indicators
---------------------------------------------------------------------------

Sudden Profile Modification
→ Risk Weight: 30 points
→ Definition: Change in registered email, phone number, or residential 
  address followed by an immediate transfer attempt (within 24 hours)
→ Example: Email changed at 2 PM, ₦800K transfer at 3 PM same day
→ Rationale: Classic ATO pattern - change contact info to block alerts


Credential Reset Cascades
→ Risk Weight: 30 points
→ Definition: Password OR Transaction PIN changes followed by immediate 
  high-value withdrawals (within 1 hour)
→ Example: PIN reset at 10:15 AM, ₦600K withdrawal at 10:30 AM
→ Rationale: Fraudster gains access, changes credentials, extracts funds quickly


New Device After SIM Swap
→ Risk Weight: 35 points (HIGHEST RISK)
→ Definition: Fresh app installation on new device OS, particularly when 
  following a detected SIM-swap event
→ Example: SIM swap detected Monday, new app install Tuesday, transfer Wednesday
→ Rationale: This is the #1 ATO indicator in Nigeria
→ Action: Automatic 48-hour transfer freeze should be enforced


=========================================================================
SECTION 2: DYNAMIC RISK SCORING FRAMEWORK
=========================================================================

Every transaction is processed through an AI-driven scoring engine
BEFORE authorization. Risk score = sum of all applicable red flag weights.


2.1 Risk Score Calculation
---------------------------------------------------------------------------

Example Calculation:
  - Odd-hour transaction (2:47 AM): +20 points
  - New device detected: +15 points
  - Round amount (₦500,000.00): +10 points
  - First-time international merchant: +15 points
  ───────────────────────────────────────────
  Total Risk Score: 60 points


2.2 Risk Score Interpretation & Actions
---------------------------------------------------------------------------

LOW RISK (Score: 0-30)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Assessment: Transaction aligns with 90-day historical behavioral patterns, 
           known device, and usual location

Action: Process transaction seamlessly (frictionless experience)

Notification: Real-time SMS/App alert AFTER transaction completes
             "You sent ₦25,000 to Chukwuma Okafor via Sentinel Bank"

Example: Regular monthly electricity bill payment


MEDIUM RISK (Score: 31-60)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Assessment: Single anomaly detected (e.g., new merchant but reasonable category)

Action: Trigger Step-up Authentication
        - SMS OTP to registered phone number
        - OR Email OTP to registered email
        - Transaction held in "Pending" state for 5 minutes
        - Auto-decline if OTP not entered

Notification: "We detected unusual activity. Please confirm this transaction 
              with OTP sent to 080****1234"

Example: First purchase at new online store (Jumia, Konga)


HIGH RISK (Score: 61-85)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Assessment: Multiple red flags present (e.g., new merchant + new device + 
           odd hours)

Action: **MANDATORY PUSH-TO-APP CHALLENGE**
        - Transaction held in "Pending - Security Check" state
        - Push notification sent to registered mobile app
        - Biometric confirmation required (FaceID/Fingerprint/PIN)
        - Display full transaction details + risk reason
        - 5-minute timeout (auto-decline if no response)

Notification: "Security Alert: Verify this transaction in your Sentinel app"
              Shows: Amount, Beneficiary, Bank, Risk Reason

Example: ₦450,000 transfer at 2 AM from new device to new beneficiary


CRITICAL RISK (Score: 86-100)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Assessment: Patterns match known fraud profiles (e.g., SIM-Swap + Password 
           reset + Immediate high-value withdrawal)

Action: **BLOCK TRANSACTION IMMEDIATELY**
        - Transaction declined instantly (no OTP, no challenge)
        - Freeze account access (all channels)
        - Block all cards linked to account
        - Disable digital banking channels
        - Trigger FRM specialist review
        - Generate urgent alert to fraud team
        - Send email to registered email (NOT SMS - phone may be compromised)

Notification: "Your account has been temporarily frozen due to suspicious 
              activity. Contact us immediately: fraud-desk@sentinelbank.ng"

Example: SIM swap detected + Fresh app install + Immediate ₦2M transfer 
        to crypto exchange


=========================================================================
SECTION 3: PUSH-TO-APP AUTHORIZATION PROCEDURES
=========================================================================

To eliminate the vulnerability of SMS-based OTP interception (SIM swap),
the system enforces Push-to-App challenges for high-risk transactions.


3.1 Security Hold Procedure
---------------------------------------------------------------------------
Step 1: Transaction Pause
  - Transaction placed in "Pending - Security Verification" state
  - Maximum hold: 300 seconds (5 minutes)
  - Display "Verifying transaction..." to merchant/beneficiary

Step 2: Encrypted Push Notification
  - Secure push sent to registered mobile app on verified device
  - Uses device-specific encryption key (established during app setup)
  - Cannot be intercepted like SMS

Step 3: Full Transparency Display
  The app MUST display ALL of the following:
  - Beneficiary Name (in bold)
  - Beneficiary Bank
  - Amount (including breakdown of fees)
  - Transaction Fee (e.g., ₦50 NIP fee)
  - Cybersecurity Levy (0.5% of principal)
  - Total Debit
  - Risk Reason (e.g., "New device detected", "Unusual location")
  - Transaction Reference

Step 4: User Action Required
  Options presented to customer:
  - APPROVE (green button) → Requires biometric/PIN
  - REJECT (red button) → Immediate decline

Step 5: Biometric Validation
  If customer taps APPROVE:
  - FaceID verification (iPhone)
  - OR Fingerprint verification (Android)
  - OR Secure Transaction PIN (6-digit, different from login PIN)
  - Must match enrolled biometric

Step 6: Response Handling
  
  If APPROVED:
  - Process transaction immediately
  - Log authentication method used
  - Update device trust score (+5 points toward trusted device)
  - Send confirmation: "Transaction successful"
  
  If REJECTED:
  - Cancel transaction immediately
  - Block card/account access (prevent further attempts)
  - Generate URGENT FRM alert with HIGH priority
  - Send confirmation SMS: "Transaction blocked per your request. 
    Your account is now frozen for security. Contact fraud-desk@sentinelbank.ng"
  - Assign to FRM specialist within 30 minutes

Step 7: Timeout Handling
  If no response within 5 minutes:
  - Auto-decline transaction
  - Send SMS: "Transaction timed out. If you did not initiate this, 
    contact us immediately"
  - Flag account for review (but don't freeze yet)


3.2 Why Push-to-App Replaces OTP
---------------------------------------------------------------------------
Problem with SMS OTP:
  - SIM swap attacks intercept SMS
  - SS7 vulnerabilities allow SMS interception
  - SMS can be delayed (up to hours)
  - No device binding (OTP works from any phone)

Advantages of Push-to-App:
  - Device-specific (bound to specific phone IMEI/MAC)
  - Encrypted end-to-end
  - Cannot be intercepted even with SIM swap
  - Instant delivery (no SMS gateway delays)
  - Biometric binding (can't approve without finger/face)
  - Shows full transaction details (customer sees what they're approving)


=========================================================================
SECTION 4: COMMON FRAUD SCENARIOS IN NIGERIA
=========================================================================

These are the most prevalent fraud types in Nigerian banking as of 2026.


4.1 SIM Swap Fraud
---------------------------------------------------------------------------
Attack Pattern:
  1. Fraudster obtains customer's BVN/NIN (through data breach or phishing)
  2. Fraudster visits MNO (MTN/Airtel/Glo/9mobile) store with fake ID
  3. Fraudster convinces agent to transfer phone number to new SIM
  4. Fraudster now receives all SMS (including banking OTPs)
  5. Fraudster resets banking password via SMS OTP
  6. Fraudster drains account

Detection Controls:
  - Real-time SIM swap detection via MNO API query
  - If SIM swap detected in last 48 hours:
    * Automatic transfer freeze for Tier 1 & 2 accounts
    * Tier 3 accounts: Mandatory push-to-app challenge
    * Email notification (NOT SMS) to registered email
  
Mitigation:
  - Email-based OTP as fallback (for password reset)
  - Require branch visit for transfer reactivation after SIM swap
  - Biometric re-enrollment required
  - 48-hour cooling-off period enforced

Prevention Tips (Customer Education):
  - "Never share your BVN or NIN with anyone"
  - "Set up SIM PIN lock to prevent unauthorized SIM swaps"
  - "Use app push notifications, not SMS OTP"


4.2 Phishing & Social Engineering
---------------------------------------------------------------------------
Attack Pattern:
  1. Customer receives call/email from "bank official"
  2. Fraudster creates urgency: "Your account will be blocked!"
  3. Fraudster pressures customer to share OTP/PIN
  4. Fraudster initiates transfer while on call with customer
  5. Customer unknowingly authorizes their own theft

Detection Controls:
  - In-app warnings during high-risk transfers:
    **"⚠️ WARNING: Sentinel Bank will NEVER ask for your PIN via phone.
    Are you being pressured into this transaction? If YES, tap REJECT."**
  
  - Voice stress analysis (future enhancement - Week 3)
  - Unusual beneficiary name patterns (e.g., "Agent", "Test", "Verification")

Mitigation:
  - Cooling-off period: 1-hour delay for first-time large beneficiaries
  - Customer education campaigns (emails, app banners, SMS)
  - Callback verification: Bank calls customer on registered number for 
    high-value first-time transfers >₦500,000

Prevention Tips:
  - "We will NEVER ask for your PIN, password, or OTP"
  - "If pressured, hang up and call our official number"
  - "Take time to think - don't rush financial decisions"


4.3 POS & Agency Banking Fraud
---------------------------------------------------------------------------
Attack Pattern:
  1. Compromised POS terminal (malware installed)
  2. Fraudster double-charges customers
  3. OR Fraudster skims card details for cloning
  4. Customer only discovers during statement review (too late)

Detection Controls:
  - Duplicate charge detection:
    * Same terminal
    * Similar amount (within 5%)
    * Within 5 minutes
    → If detected: Auto-reverse second charge
  
  - Real-time dual-connectivity failover (prevent double-debit)
  - Terminal health monitoring (detect malware)
  - Unusual terminal activity patterns

Mitigation:
  - Automatic reversal of duplicate charges (within 24 hours)
  - Terminal blacklisting (if fraud confirmed)
  - Agent sanctions (suspension, termination, legal action)
  - Customer SMS alerts for EVERY POS transaction (real-time)

Prevention:
  - "Check your SMS alert immediately after POS payment"
  - "Report double charges within 24 hours for instant reversal"


4.4 Account Takeover (ATO)
---------------------------------------------------------------------------
Attack Pattern:
  1. Fraudster obtains login credentials (phishing, data breach)
  2. Fraudster changes email/phone in account profile
  3. Fraudster resets password (OTP goes to new email/phone)
  4. Fraudster withdraws funds
  5. Customer discovers too late (alerts went to fraudster's contact)

Detection Controls:
  - Profile change alerts to OLD contact details:
    "Your email was changed. If you didn't do this, contact us immediately"
  
  - Mandatory cooling-off period: 24-hour delay after profile changes
    before high-value transactions allowed
  
  - Multi-factor authentication for profile changes (not just password)

Mitigation:
  - Instant account freeze if customer reports ATO
  - Password reset from branch only (after ATO report)
  - Full account audit (find unauthorized changes)

Prevention:
  - "Use strong, unique passwords"
  - "Enable biometric login (harder to steal than password)"
  - "Monitor your email for profile change alerts"


=========================================================================
SECTION 5: FRAUD RESPONSE PROTOCOLS
=========================================================================

Standardized response procedures ensure consistent fraud handling.


IMMEDIATE ACTIONS (Automated within 2 minutes)
---------------------------------------------------------------------------
Upon detection of Critical Risk (score 86-100):
  1. Freeze account access (all channels)
  2. Block all cards linked to account
  3. Disable digital banking channels (app, internet banking, USSD)
  4. Send security alert to registered email (NOT SMS if SIM swap suspected)
  5. Generate urgent FRM alert (high priority queue)
  6. Log all recent transactions (last 48 hours) for forensics


WITHIN 1 HOUR
---------------------------------------------------------------------------
  1. FRM specialist assigned to case
  2. Preliminary investigation initiated:
     - Review transaction logs
     - Check device IDs and locations
     - Analyze pattern against known fraud profiles
  3. Customer contacted via verified phone/email
     - Use ONLY the original registered contact details (not new ones)
     - Security questions asked to verify customer identity
  4. Temporary credit issued (if customer is legitimate victim)
     - Max ₦200,000 temporary credit while investigation ongoing
     - Prevents customer hardship during investigation


WITHIN 24 HOURS
---------------------------------------------------------------------------
  1. Full transaction forensics:
     - IP address analysis
     - Device fingerprinting
     - Network analysis (money trail)
     - NIBSS coordination (if funds transferred out)
  2. Contact other banks (if fund transfer involved)
     - Attempt to freeze funds at destination bank
     - Request beneficiary details
  3. Police report filed (for amounts >₦1,000,000)
  4. Customer debriefing:
     - Understand how fraud occurred
     - Gather evidence (emails, call recordings, etc.)
  5. Initial determination: Customer fraud vs. Bank fraud
     - Customer fraud: Customer bears loss
     - Bank fraud: Bank bears loss


RESOLUTION (14-21 DAYS)
---------------------------------------------------------------------------
  1. Complete investigation report prepared
  2. Permanent credit/debit adjustments made
  3. Account reactivation (if customer legitimate)
     - New cards issued
     - Password reset required
     - Biometric re-enrollment required
  4. Fraud database updates:
     - Beneficiary accounts flagged
     - Fraudster devices blacklisted
     - Fraud patterns documented
  5. Customer education:
     - Explain what happened
     - Provide security tips
     - Set up enhanced monitoring (if customer opts in)


=========================================================================
SECTION 6: REFERENCES & COMPLIANCE
=========================================================================

Regulatory Framework:
  - Central Bank of Nigeria (CBN) Consumer Protection Framework
  - Nigeria Data Protection Regulation (NDPR) 2019
  - CBN Guidance on Electronic Fraud Management (revised 2024)
  - CBN Exposure Draft on Cyber Resilience (2025)

Industry Standards:
  - ISO 27001 (Information Security Management)
  - PCI DSS v4.0 (Payment Card Industry Data Security Standard)
  - NIST Cybersecurity Framework

Internal Policies:
  - Customer Authentication Policy (CAP-002)
  - Data Privacy & Protection Policy (DPP-001)
  - Incident Response Plan (IRP-003)
  - Business Continuity Plan (BCP-001)


=========================================================================
DOCUMENT CONTROL
=========================================================================

**Document Owner**: Chief Risk Officer (CRO)
**Review Frequency**: Monthly (due to rapidly evolving fraud landscape)
**Last Updated**: {self.display_date}
**Next Review**: March 2026
**Version History**:
  - v4.0: Current version (added SIM swap controls)
  - v3.5: Added push-to-app authorization (Dec 2025)
  - v3.0: Added biometric authentication (Sept 2025)

**Emergency Contact**: 
  - Email: fraud-desk@sentinelbank.ng
  - Phone: +234-1-FRAUD-24 (Active 24/7/365)
  - WhatsApp: +234-901-FRM-DESK (monitoring only, no customer support)


=========================================================================
END OF DOCUMENT FRM-001
=========================================================================
"""
        
        return self._package_for_rag(
            "FRM-001", 
            "Fraud Detection & Prevention Guidelines", 
            "security", 
            "4.0", 
            guidelines
        )
    
    def generate_transaction_policies(self) -> Dict:
        """
        Generate comprehensive transaction processing policies.
        
        This document defines transaction limits, fees, reversals, and
        processing procedures. Critical for both customer service and
        automated transaction validation.
        
        Document Structure:
            - KYC tiering and transaction limits
            - Statutory levies and fees
            - International FX operations
            - Card transaction policies
            - Reversal policies (including "not possible" scenarios)
            - Dispute resolution timelines
            - Service charge schedule
        
        Target Audience:
            - Transaction processing systems
            - Customer service representatives
            - Compliance team
            
        Returns:
            Dict: Packaged document ready for RAG ingestion
        """
        
        policies = f"""
=========================================================================
{self.bank_name} - TRANSACTION PROCESSING & LIMITS POLICY
=========================================================================

**Document ID**: TSU-POL-002
**Version**: 4.0
**Effective Date**: January 2026
**Classification**: Internal Use Only

**Policy Custodian**: Head of Transaction Services
**Last Review**: {self.display_date}
**Next Review**: May 2026


=========================================================================
SECTION 1: KYC TIERING & TRANSACTION CAPACITY
=========================================================================

The Middleware must enforce these limits deterministically to comply with
Anti-Money Laundering (AML) regulations and CBN directives.


TIER 1 ACCOUNTS (Basic KYC)
---------------------------------------------------------------------------
**Requirements**: Name + Phone number only

**Limits**:
  - Daily Transaction Limit: ₦50,000
  - Maximum Account Balance: ₦300,000
  - Cumulative Monthly Limit: ₦200,000

**Channels Allowed**:
  - USSD banking (*737# codes)
  - Agent banking (via registered agents)
  - Basic internal transfers
  - Bill payments (utilities, airtime)

**Restrictions**:
  - NO international transactions
  - NO high-value merchant payments (>₦50,000)
  - NO loan eligibility
  - NO card issuance
  - NO cheque books

**Typical Customer Profile**: "Ayo" - unbanked person onboarding via agent


TIER 2 ACCOUNTS (Intermediate KYC)
---------------------------------------------------------------------------
**Requirements**: Tier 1 + BVN + NIN (National Identity Number)

**Limits**:
  - Daily Transaction Limit: ₦200,000
  - Maximum Account Balance: ₦500,000
  - Cumulative Monthly Limit: ₦2,000,000

**Channels Allowed**:
  - All Tier 1 channels
  - Mobile banking app
  - Internet banking
  - POS transactions
  - ATM withdrawals

**Restrictions**:
  - International transfers BLOCKED
  - Investment products restricted
  - Loan amounts capped at ₦500,000

**Typical Customer Profile**: "Tunde" - Young professional with BVN


TIER 3 ACCOUNTS (Full KYC)
---------------------------------------------------------------------------
**Requirements**: Tier 2 + Address Verification + Valid Photo ID + 
                  Utility Bill (not older than 3 months)

**Limits**:
  - Daily Transaction Limit: ₦5,000,000
  - Maximum Account Balance: Unlimited
  - Cumulative Monthly Limit: Unlimited

**Channels Allowed**:
  - All channels including international
  - SWIFT transfers
  - Trade finance
  - Treasury products

**Benefits**:
  - Full banking services
  - Loan and credit eligibility (up to ₦50M depending on assessment)
  - Investment products access
  - Private banking services (if balance >₦10M)

**Typical Customer Profile**: "Fatima" - Business owner with full documentation


=========================================================================
SECTION 2: STATUTORY LEVIES & FISCAL TAXES (2026 SCHEDULE)
=========================================================================

These are MANDATORY deductions remitted to the Federal Government.
They are NOT optional and apply to ALL customers.


Electronic Money Transfer Levy (EMTL)
---------------------------------------------------------------------------
**Rate**: Flat ₦50.00 charge
**Applied to**: Electronic RECEIPTS ≥₦10,000
**Direction**: Inbound (when YOU receive money)
**Examples**:
  - You receive ₦15,000 salary → ₦50 EMTL deducted
  - You receive ₦5,000 gift → No EMTL (below threshold)
**Legal Basis**: Stamp Duties Act 2020 (as amended)


National Cybersecurity Levy
---------------------------------------------------------------------------
**Rate**: 0.5% (0.005) of transaction value
**Applied to**: OUTBOUND electronic transfers
**Direction**: Outbound (when YOU send money)
**Examples**:
  - You send ₦100,000 → ₦500 Cybersecurity Levy
  - You send ₦10,000 → ₦50 Cybersecurity Levy
**Legal Basis**: Cybercrimes Act 2024 (Section 44)
**Exemptions**: 
  - Internal transfers (within same bank)
  - Loan disbursements
  - Salary payments (by companies)


Value Added Tax (VAT)
---------------------------------------------------------------------------
**Rate**: 7.5%
**Applied to**: Bank's COMMISSION/FEE (not principal amount)
**Examples**:
  - Transfer fee is ₦50 → VAT is ₦3.75 (7.5% of ₦50)
  - Principal amount is NOT subject to VAT
**Legal Basis**: Finance Act 2021


IMPORTANT NOTES:
- These charges are LEGALLY MANDATED
- Bank only remits to government (does not retain)
- Charges are AUTO-DEDUCTED (customer cannot opt out)
- Charges are SEPARATE from bank's own fees


=========================================================================
SECTION 3: INTERNATIONAL FX OPERATIONS (TRADE PORTAL)
=========================================================================

Outbound foreign transfers are processed through the Central Bank Trade
Monitoring System (TRMS). Strict documentation requirements apply.


Invisible Trade (Form A)
---------------------------------------------------------------------------
**Purpose**: For services, not physical goods
**Common Uses**:
  - School fees (tuition payments abroad)
  - Medical treatment abroad
  - Personal Travel Allowance (PTA) - up to $4,000/quarter
  - Business Travel Allowance (BTA) - up to $5,000/quarter
  - Subscriptions (Netflix, Spotify, software licenses)

**Documentation Required**:
  - Proof of purpose (school admission letter, medical report, etc.)
  - Valid means of ID
  - Tax Clearance Certificate (for amounts >$10,000)

**Fees**:
  - Processing fee: 1% of amount (Minimum ₦5,000)
  - SWIFT charges: $25 (approximately ₦37,500 at current rates)
  - Cybersecurity Levy: 0.5%

**Processing Time**: 1-3 business days (subject to FX liquidity)

**Limits**:
  - PTA: $4,000 per quarter (3 months)
  - BTA: $5,000 per quarter
  - School fees: No limit (but documentation must be perfect)


Visible Trade (Form M)
---------------------------------------------------------------------------
**Purpose**: For importation of physical goods

**Documentation Required**:
  - Proforma invoice from supplier
  - Import license (if applicable)
  - SONCAP certificate (Standards Organisation of Nigeria)
  - NAFDAC certificate (if food/drugs/cosmetics)
  - Bill of Lading or Airway Bill
  - Tax Clearance Certificate

**Processing Time**: 3-7 business days (extensive documentation review)

**Note**: This is complex. Most retail customers won't use Form M.
         This is for businesses importing goods.


=========================================================================
SECTION 4: CARD TRANSACTION & SECURITY POLICIES
=========================================================================

POS (Point of Sale) Transactions
---------------------------------------------------------------------------
**Daily Limit**: ₦500,000 (across all cards on account)
**Per-transaction Limit**: ₦200,000
**Security**:
  - PIN required for all amounts
  - OTP required for transactions >₦100,000
**Declined Transaction**: Do NOT retry more than 3 times
                          (triggers fraud alert)


Online/Web Transactions
---------------------------------------------------------------------------
**Daily Limit**: ₦500,000
**Security**: 
  - 3D Secure 2.0 MANDATORY (Visa/Mastercard SecureCode)
  - Push-to-app authorization for transactions >₦100,000
  - Device fingerprinting
**Merchant Categories Blocked** (by default):
  - Adult content
  - Gambling/betting (can be enabled on request)
  - Cryptocurrency exchanges (requires approval)


ATM Transactions
---------------------------------------------------------------------------
**Daily Withdrawal Limits**:
  - Standard accounts: ₦100,000
  - Premium accounts: ₦200,000
**Fee Structure**:
  - Own bank ATMs: First 3 withdrawals free, then ₦65 per withdrawal
  - Other bank ATMs: ₦65 per withdrawal (no free transactions)
**Note**: Limit is per CARD, not per account


International Transactions
---------------------------------------------------------------------------
**Default Status**: DISABLED (must be enabled per trip)
**How to Enable**:
  1. Log into mobile app
  2. Go to Cards → Manage Card
  3. Toggle "International Transactions" ON
  4. Set travel dates (start and end)
  5. Card auto-disables when you return

**Monthly FX Limit**: $500 equivalent for personal cards
**Processing**: 
  - Debit in Naira at current exchange rate
  - Rate: CBN rate + 3% markup (bank's margin)
**Common Issues**:
  - Declined due to insufficient Naira funds
  - Declined due to monthly limit exceeded
  - Declined because international toggle is OFF


Contactless Payments (Tap & Pay)
---------------------------------------------------------------------------
**Per-transaction Limit**: ₦15,000
**PIN Required**: For amounts ≥₦5,000
**No PIN**: For amounts <₦5,000 (just tap)
**Security**: Each card has maximum of 5 consecutive contactless transactions
             before PIN is required (resets after PIN entry)


=========================================================================
SECTION 5: REVERSAL POLICIES & "NOT POSSIBLE" SCENARIOS
=========================================================================

Automatic Reversals (No Customer Action Required)
---------------------------------------------------------------------------

Failed Internal Transfers
  - Timeline: Within 24 hours (usually within 2 hours)
  - Process: Automatic system reversal
  - Notification: SMS + App notification
  - Example: "Transfer to Chukwuma Okafor failed due to network error. 
             ₦50,000 has been credited back to your account."

Failed NIP (Interbank) Transfers
  - Timeline: 48-72 hours
  - Process: Coordination with NIBSS required
  - Notification: Status updates every 24 hours
  - Reason for delay: Need confirmation from destination bank

Failed ATM Withdrawals (Dispense Errors)
  - Timeline: Within 24 hours (after terminal reconciliation)
  - Process: Bank reconciles terminal cash vs. transactions
  - Notification: SMS confirmation
  - Note: CRITICAL - Customer must report within 24 hours


Manual Reversal Requests (Customer Must Request)
---------------------------------------------------------------------------

**Eligible Scenarios**:
1. Transfer to wrong account (customer's own error)
2. Duplicate transaction (charged twice for same thing)
3. Amount error (sent ₦100,000 instead of ₦10,000)

**Process**:
  Step 1: Customer logs complaint with Transaction Services Unit (TSU)
  Step 2: Investigation (2-5 business days)
          - Verify transaction was debited
          - Confirm beneficiary details
          - Check if beneficiary has received credit
  Step 3: Contact beneficiary bank for refund request
  Step 4: If beneficiary agrees: Credit applied to customer
          If beneficiary refuses: Cannot force reversal (see below)

**Important**: Bank cannot FORCE recipient to return funds.
              Recovery depends on beneficiary cooperation.


Reversal NOT POSSIBLE (Customer Bears Loss)
---------------------------------------------------------------------------

The following reversals CANNOT be processed:

1. **Beneficiary Bank Confirms Credit AND Withdrawal**
   → Reason: Funds already withdrawn by recipient
   → Action: Customer must pursue civil recovery (legal action)
   → Example: Sent ₦500K to wrong account. Recipient withdrew same day.

2. **Value Has Been Consummated**
   → Reason: Service/product already delivered
   → Examples:
     * Airtime purchased (already loaded on phone)
     * DSTV subscription paid (already activated)
     * Utility bill paid (already posted to your account)
   → Action: Cannot reverse. Contact merchant for refund directly.

3. **Funds Liquidated/Transferred Out**
   → Reason: Recipient moved money elsewhere
   → Example: You sent to Account A. Account A owner sent to Account B.
              Cannot reverse because money not in Account A anymore.

4. **Request Made >60 Days After Transaction**
   → Reason: Outside chargeback window
   → Legal Basis: Card scheme rules (Visa/Mastercard)
   → Action: Customer can pursue civil recovery

5. **Transaction Marked as Fraudulent by Recipient**
   → Reason: Recipient claims fraud, dispute is complex
   → Action: Requires investigation by both banks + NIBSS
   → Timeline: Can take 90+ days


=========================================================================
SECTION 6: DISPUTE RESOLUTION TIMELINE
=========================================================================

Standard Process for Transaction Disputes:


DAY 0: Complaint Logged
---------------------------------------------------------------------------
Actions:
  - Unique ticket ID generated (format: DIS-YYYYMMDD-XXXX)
  - NIBSS session ID captured (for interbank transactions)
  - Immediate acknowledgement sent via SMS + Email
  - Complaint recorded in CRM system

Customer Receives:
  "Your complaint has been logged. Ticket ID: DIS-20260217-1234. 
   We will investigate and respond within 48 hours."


DAY 1: Formal Acknowledgement & Assignment
---------------------------------------------------------------------------
Actions:
  - Email confirmation with full ticket details sent
  - Expected timeline communicated (depends on issue type)
  - Case assigned to specialist (based on complaint category)
  - Priority level determined

Customer Receives:
  Email with:
  - Ticket ID
  - Assigned specialist name
  - Expected resolution date
  - Contact number for updates


DAY 2-5: Investigation Phase
---------------------------------------------------------------------------
Actions:
  - Review transaction logs in core banking system
  - Check NIBSS/Switch logs for inter-bank transactions
  - Analyze error codes (e.g., 06=Declined, 91=Timeout, 96=System Error)
  - Contact beneficiary bank (if inter-bank transfer)
  - Request customer for additional information (if needed)

Customer Updates:
  - Status update every 48 hours
  - SMS if additional info needed

Common Error Codes:
  - 00: Approved (successful)
  - 06: Declined by issuer bank
  - 51: Insufficient funds
  - 91: Timeout (network issue)
  - 96: System malfunction


DAY 6-14: Resolution or Escalation
---------------------------------------------------------------------------

For Simple Cases (90% of disputes):
  - Root cause identified
  - Resolution action taken (reversal, credit, explanation)
  - Customer notified
  - Ticket closed

For Complex Cases (10% of disputes):
  - Escalate to Level 2 (specialist review)
  - May require legal review
  - Timeline extended to 21 days
  - Customer informed of delay with justification


DAY 14-21: Final Resolution (Complex Inter-Bank Chargebacks)
---------------------------------------------------------------------------

For Unresolved Inter-Bank Disputes:
  - Formal chargeback request submitted to NIBSS
  - Both banks present evidence
  - NIBSS makes determination
  - Final resolution applied

For Merchant Disputes (Card Transactions):
  - Follows Visa/Mastercard chargeback process
  - Temporary credit may be issued during investigation
  - Final determination can take 45-90 days


=========================================================================
SECTION 7: INDUSTRY-STANDARD SERVICE CHARGES
=========================================================================

Interbank Transfers (NIP - NIBSS Instant Payment)
---------------------------------------------------------------------------
  - Up to ₦5,000: ₦10.00
  - ₦5,001 to ₦50,000: ₦25.00
  - Above ₦50,000: ₦50.00

**Note**: First 3 NIP transfers per month are FREE for savings accounts


Internal Transfers (Within Sentinel Bank)
---------------------------------------------------------------------------
  - Between own accounts: FREE (unlimited)
  - To other Sentinel Bank customers: FREE (unlimited)


USSD Banking Sessions
---------------------------------------------------------------------------
  - Flat rate: ₦6.98 per session
  - Includes: USSD airtime purchase, balance inquiry, transfers
  - Shared between bank and telco (₦4 to telco, ₦2.98 to bank)


SMS Transaction Alerts
---------------------------------------------------------------------------
  - Rate: ₦4.00 per alert
  - Frequency: For EVERY transaction (debit or credit)
  - Mandatory: Cannot be opted out (CBN requirement for security)
  - Monthly average: ₦120 (assuming 30 transactions/month)


ATM Withdrawals
---------------------------------------------------------------------------
**Own Bank ATMs**:
  - First 3 withdrawals per month: FREE
  - 4th withdrawal onwards: ₦65 per withdrawal

**Other Bank ATMs**:
  - All withdrawals: ₦65 per withdrawal
  - (No free transactions at other banks)


Account Maintenance Fees
---------------------------------------------------------------------------
**Savings Account**:
  - Monthly fee: ₦50
  - Waived if: Maintain average monthly balance of ₦100,000

**Current Account**:
  - Monthly fee: ₦300
  - Commission on Turnover (COT): ₦1 per ₦1,000 on third-party deposits
  - (COT applies to business transactions only)

**Domiciliary Account** (Dollar/Pound/Euro):
  - Monthly fee: $5 (or equivalent)
  - Waived if: Maintain balance of $1,000 or equivalent


Other Fees
---------------------------------------------------------------------------
  - Hardware Token: ₦3,500 (one-time fee)
  - Card replacement: ₦1,500
  - Cheque book: ₦500-₦2,000 (depending on number of leaves)
  - Statement of account: ₦50 per month (via email, FREE in app)
  - Reference letter: ₦1,000
  - Account reactivation (after dormancy): ₦500


=========================================================================
DOCUMENT CONTROL
=========================================================================

**Policy Owner**: Head of Transaction Services
**Approver**: Chief Operations Officer (COO)
**Review Frequency**: Quarterly
**Last Updated**: {self.display_date}
**Next Review**: May 2026

**Contact for Questions**:
  - Email: transactionservices@sentinelbank.ng
  - Phone: +234-1-SENTINEL (Internal: Extension 4000)


=========================================================================
END OF DOCUMENT TSU-POL-002
=========================================================================
"""
        
        return self._package_for_rag(
            "TSU-POL-002", 
            "Transaction Processing Policies", 
            "operations", 
            "4.0", 
            policies
        )

    def generate_faq_document(self) -> Dict:
        """
        Generate customer-facing frequently asked questions document.
        
        This document provides self-service answers to common customer
        questions. Written in plain language for easy understanding.
        
        Document Structure:
            - Transfer & payment issues
            - Card problems
            - Mobile app & digital banking
            - Fraud & security
            - Account services
        
        Target Audience:
            - Customers (self-service)
            - Customer service representatives (quick reference)
            - Chatbots and AI assistants
            
        Returns:
            Dict: Packaged document ready for RAG ingestion
        """
        
        faq = f"""
=========================================================================
{self.bank_name} - CUSTOMER SERVICE FAQ
=========================================================================

**Document ID**: FAQ-001
**Version**: 2.0
**Last Updated**: {datetime.now().strftime('%B %Y')}
**Classification**: Public (Customer-Facing)


=========================================================================
SECTION 1: TRANSFER & PAYMENT ISSUES
=========================================================================

Q1: My transfer was debited but the receiver didn't get the money. 
    What should I do?
---------------------------------------------------------------------------
A: This is usually a temporary delay in the interbank network. Here's 
   what to do:

   1. Wait 2 hours first (most delays resolve automatically)
   2. Check your transaction history for the transaction reference number
   3. Confirm the account number you sent to (verify with recipient)
   4. If still not received after 2 hours:
      - Contact us with your transaction reference
      - We can track it through NIBSS (Nigeria Inter-Bank Settlement System)
      - We will resolve within 48 hours
   5. If money doesn't reach within 24 hours:
      - Automatic reversal will occur
      - You'll get SMS notification when credited back

   **Important**: Keep your transaction reference number handy!
   Format: NIP/240217/1234567890


Q2: Why was my transfer to another bank delayed?
---------------------------------------------------------------------------
A: Inter-bank transfers use the NIBSS network which can have delays during:

   **Peak Traffic Periods**:
   - Month-end (25th-30th): Salary payment days
   - First week of month: Rent payment period
   - Friday evenings: High transaction volume

   **Network Maintenance**:
   - Usually announced in advance
   - Typically done between 2 AM - 6 AM
   - Check our app for maintenance notifications

   **Public Holidays**:
   - Processing resumes next business day
   - Transactions submitted on Friday evening may process Monday morning

   **Normal Processing Time**: 2-5 minutes
   **Acceptable Delay**: Up to 2 hours during peak periods
   **Beyond 2 hours**: Please report to us for investigation

   **Pro Tip**: For urgent transfers, send before 3 PM on weekdays.


Q3: I sent money to the wrong account number. Can you reverse it?
---------------------------------------------------------------------------
A: Unfortunately, we CANNOT automatically reverse transfers sent to the 
   wrong account. Here's why and what you can do:

   **Why We Can't Auto-Reverse**:
   - Once money enters the recipient's account, only they can return it
   - Banking regulations don't allow forced reversals
   - Recipient has legal right to the funds (until proven otherwise)

   **What You Should Do**:
   Step 1: Contact the recipient IMMEDIATELY
           - Call or text the phone number linked to that account
           - Explain the error and request a refund
           - Many people are honest and will refund

   Step 2: If recipient refuses or is unreachable:
           - Report to us with evidence:
             * Screenshot of wrong account number entered
             * Proof of your intended recipient
             * Any communication with wrong recipient
           - We will contact the recipient's bank
           - Recovery process can take 14-21 days

   Step 3: Legal action (last resort):
           - If recipient refuses despite bank's intervention
           - You can file a civil case for recovery
           - Keep all evidence (transaction receipts, messages)

   **PREVENTION TIP**: 
   Always use the "Name Enquiry" feature before sending money!
   - Enter account number
   - System shows you the account name
   - VERIFY it matches your intended recipient
   - Then proceed with transfer


Q4: What is the maximum amount I can transfer per day?
---------------------------------------------------------------------------
A: It depends on your account KYC (Know Your Customer) tier:

   **Tier 1 (Basic)**: ₦50,000 per day
   - Requirements: Name + Phone number only
   - Limited features

   **Tier 2 (Intermediate)**: ₦200,000 per day
   - Requirements: Tier 1 + BVN + NIN
   - Mobile app access

   **Tier 3 (Full KYC)**: ₦5,000,000 per day
   - Requirements: Tier 2 + Address proof + ID + Utility bill
   - Full banking services

   **How to Upgrade Your Account**:
   Visit any branch with:
   - Valid ID (Driver's license, International passport, Voter's card, NIN)
   - Utility bill (NEPA, LAWMA, water bill - not older than 3 months)
   - BVN (if not already captured)
   - Passport photograph

   **Processing Time**: Same day (if you visit before 2 PM)


=========================================================================
SECTION 2: CARD ISSUES
=========================================================================

Q5: Why was my card declined even though I have money in my account?
---------------------------------------------------------------------------
A: Cards can be declined for MANY reasons. Here are the most common:

   **1. International Transaction (Card Not Enabled)**
   - Your card is disabled for foreign transactions by default
   - Enable it in the app: Cards → Manage Card → International ON
   - Set your travel dates (auto-disables when you return)

   **2. Insufficient Funds (Including Pending Transactions)**
   - Check your AVAILABLE balance (not just book balance)
   - You may have pending transactions not yet visible
   - Example: Hotel pre-authorization holds funds for 3-7 days

   **3. Daily Limit Exceeded**
   - POS limit: ₦500,000 per day
   - ATM limit: ₦100,000 per day (standard) or ₦200,000 (premium)
   - Limit resets at midnight

   **4. Security Block**
   - Suspicious activity detected (multiple failed PIN attempts)
   - Card temporarily blocked for your protection
   - Contact us to unblock

   **5. Expired Card**
   - Check expiry date (MM/YY format on your card)
   - Request new card if expired

   **6. Incorrect PIN**
   - After 3 wrong PIN attempts, card auto-locks for 24 hours
   - Do NOT keep trying (makes it worse)
   - Reset your PIN via app or call us

   **7. Merchant Category Blocked**
   - Betting/gambling sites blocked by default (can be enabled)
   - Some international merchants blocked (contact us to allow)

   **8. Technical Issue with Merchant's Terminal**
   - Try a different POS terminal
   - Or pay with transfer instead

   **What to Do If Declined**:
   - Check available balance in app
   - Verify card expiry date
   - Try a different merchant/terminal if possible
   - If still declining, contact us: 0700-SENTINEL


Q6: The ATM didn't give me cash but my account shows debit. What do I do?
---------------------------------------------------------------------------
A: This is called a "dispense error" or "ATM reversal case". Here's the 
   exact procedure:

   **IMMEDIATE ACTIONS** (Do this NOW):
   1. DO NOT retry the same ATM immediately
      (It will debit you again!)

   2. Note these details:
      - Exact ATM location (bank name, branch, street address)
      - Time of transaction (check your SMS alert)
      - Amount you tried to withdraw
      - Your account number

   3. Check if ATM printed a receipt
      - If yes: Keep it safely (it's evidence)
      - If no: No problem, we can still help

   **WHAT HAPPENS NEXT**:
   - Contact us within 24 hours (CRITICAL for quick resolution)
   - We initiate terminal reconciliation:
     * Count cash remaining in ATM
     * Compare with transaction records
     * If cash count shows your money still in machine → Reversal approved
   
   - Automatic reversal usually happens within 24-48 hours
   - You'll get SMS: "₦X has been credited back to your account. 
     Reference: REV/240217/XXXX. Sorry for the inconvenience."

   **IF DELAY OCCURS**:
   - For complex cases: We may issue temporary credit while investigating
   - Maximum investigation time: 7 days
   - 90% of dispense errors are reversed within 24 hours

   **STATISTICS** (So you know this is normal):
   - Dispense errors: ~0.01% of ATM transactions
   - Successful reversal rate: 99.8%
   - Average reversal time: 18 hours


Q7: How do I enable my card for international transactions?
---------------------------------------------------------------------------
A: For security, international transactions are DISABLED by default.
   Here's how to enable:

   **METHOD 1: Via Mobile App** (Easiest)
   1. Open Sentinel Bank app
   2. Login with your password/biometric
   3. Tap "Cards" menu at bottom
   4. Select the card you want to enable
   5. Tap "Manage Card"
   6. Toggle "International Transactions" to ON (green)
   7. Set your travel dates:
      - Travel start date
      - Travel end date
      - Card will auto-disable when you return (for your security)
   8. Done! Card is now active for international use

   **METHOD 2: Via Customer Service**
   1. Call 0700-SENTINEL (0700-736-8463)
   2. Request international activation
   3. Provide:
      - Card number (last 4 digits)
      - Travel destination
      - Travel dates
   4. Representative will enable within 5 minutes
   5. You'll get SMS confirmation

   **IMPORTANT NOTES**:
   - Some countries require additional verification
     (We may request your flight itinerary for compliance)
   
   - Monthly FX limit: $500 equivalent for personal cards
     (Enough for most vacations)
   
   - Charges: 
     * Transaction is debited in Naira at current exchange rate
     * Rate: CBN rate + 3% markup
     * Example: $100 purchase = ₦150,000 (if rate is ₦1,500/$)
   
   - Common destinations with no issues:
     * UK, USA, Canada, Dubai, South Africa, Ghana
   
   - Countries with frequent declines:
     * China (use Alipay instead)
     * Some Asian countries (inform us before travel)

   **PRO TIP**: Enable 2-3 days before travel, not at the airport!


Q8: My card is stuck in the ATM. What should I do?
---------------------------------------------------------------------------
A: Card retention happens due to:
   - Multiple wrong PIN attempts (3+ times)
   - Suspicious activity detected by fraud system
   - Card reported lost/stolen (and you forgot)
   - Technical malfunction of ATM

   **IMMEDIATE STEPS**:
   1. Call our Customer Care IMMEDIATELY: 0700-SENTINEL
      - We will block the card instantly (prevent unauthorized use)
      - Generate incident report
   
   2. Provide ATM details:
      - Bank name and branch (e.g., "GTBank, Ikeja branch")
      - ATM location (inside bank or outside?)
      - Time of retention
   
   3. Choose retrieval OR replacement:
      
      **OPTION A: Card Retrieval** (If you need that specific card)
      - Visit the bank branch where ATM is located
      - Bring valid ID
      - Card can usually be retrieved same day (if during banking hours)
      - Some banks keep retained cards for 7 days, then destroy
      
      **OPTION B: Card Replacement** (Easier for most people)
      - Request new card (takes 3-5 business days)
      - Fee: ₦1,500 for standard card
      - Old card will be deactivated
      - New card number (you'll need to update subscriptions)
   
   4. If you need cash urgently while waiting:
      - Visit any of our branches
      - Bring valid ID
      - We can process counter withdrawal
      - Or issue temporary ATM card (₦500, valid for 7 days)

   **PREVENTION**:
   - Remember your PIN correctly
   - Don't share your PIN with anyone
   - If you forget PIN, reset it BEFORE going to ATM


=========================================================================
SECTION 3: MOBILE APP & DIGITAL BANKING
=========================================================================

Q9: I can't log into the mobile app. What's wrong?
---------------------------------------------------------------------------
A: Common causes and solutions:

   **WRONG PASSWORD**
   - Symptom: "Invalid credentials" message
   - Solution: Use "Forgot Password" option
     * Reset link sent to registered email
     * New password must be 8+ characters
     * Must include: number, uppercase letter, symbol
     * Cannot reuse last 3 passwords

   **OUTDATED APP VERSION**
   - Symptom: App crashes on opening OR "Update required" message
   - Solution: 
     * Android: Open Play Store → My Apps → Sentinel Bank → Update
     * iOS: Open App Store → Updates → Sentinel Bank → Update
   - Important: Sometimes old versions are disabled for security
   - Always keep app updated!

   **NETWORK ISSUES**
   - Symptom: "Cannot connect to server" OR endless loading
   - Solution:
     * Check your internet connection (try opening browser)
     * Switch between WiFi and mobile data
     * Try airplane mode ON for 10 seconds, then OFF
     * Clear app cache: Phone Settings → Apps → Sentinel Bank → Clear Cache

   **ACCOUNT LOCKED**
   - Symptom: "Account temporarily locked" message
   - Cause: 3 wrong password attempts
   - Solution: 
     * Account auto-unlocks after 30 minutes
     * OR call Customer Care for immediate unlock
     * OR use "Forgot Password" to reset

   **DEVICE CHANGED**
   - Symptom: "Verify your identity" OR "OTP required"
   - Cause: First login on new device (security feature)
   - Solution:
     * OTP sent to registered phone number
     * Enter OTP to verify
     * Device will be remembered for future logins
   - If not receiving OTP: Call us to verify phone number on file


Q10: I'm not receiving OTPs. How can I fix this?
---------------------------------------------------------------------------
A: Try these steps IN ORDER:

   **STEP 1: VERIFY PHONE NUMBER**
   - Call Customer Care: 0700-SENTINEL
   - Confirm: "What phone number do you have on file for my account?"
   - Problem: Sometimes number gets unlinked after SIM replacement
   - Solution: Update number if incorrect (requires branch visit with ID)

   **STEP 2: CHECK NETWORK COVERAGE**
   - Ensure you have signal (check signal bars)
   - Try moving to area with better reception
   - Switch to different network if dual SIM

   **STEP 3: CHECK MESSAGE INBOX**
   - OTP might be in spam/junk folder
   - Some phones block messages from shortcodes
   - Check "Blocked messages" in your SMS app
   - Look for sender: "SENTINEL" or "SBNK"

   **STEP 4: RESTART PHONE**
   - Simple restart often resolves stuck messages
   - Hold power button → Restart
   - Wait 1 minute, try requesting OTP again

   **STEP 5: ALTERNATIVE AUTHENTICATION**
   - Try email OTP (if available for your transaction)
   - Use hardware token (if you have one)
   - Call Customer Care for phone-based verification

   **STEP 6: VISIT BRANCH** (If all else fails)
   - Bring valid ID
   - Update phone number with proper verification
   - May require additional KYC documentation
   - Processing time: 15-30 minutes

   **FOR IPHONE USERS**:
   - Check: Settings → Messages → Filter Unknown Senders
   - If enabled, OTP may be hidden
   - Disable filter or check "Unknown Senders" folder

   **FOR ANDROID USERS**:
   - Check: Messages app → Menu → Spam & Blocked
   - OTP may be marked as spam
   - Mark sender as "Not spam"


=========================================================================
SECTION 4: FRAUD & SECURITY
=========================================================================

Q11: I received a call from someone claiming to be from the bank asking 
     for my PIN. Is this real?
---------------------------------------------------------------------------
A: **NO! THIS IS 100% A SCAM.**

   **What We NEVER Do**:
   ❌ We NEVER ask for your PIN via phone
   ❌ We NEVER ask for your password via phone
   ❌ We NEVER ask for your OTP via phone, email, or SMS
   ❌ We NEVER ask you to send money to "verify" your account
   ❌ We NEVER ask you to install remote access apps (TeamViewer, AnyDesk)
   ❌ We NEVER threaten immediate account closure to pressure you
   ❌ We NEVER ask you to move money to a "safe account"

   **If You Shared Your PIN/OTP/Password**:
   ⚠️ ACT IMMEDIATELY (Every second counts!):
   
   1. Change your password RIGHT NOW
      - In the app: Menu → Settings → Change Password
      - Choose a strong, unique password
   
   2. Block your card
      - In the app: Cards → Block Card
      - This stops any card transactions immediately
   
   3. Call our OFFICIAL number: 0700-SENTINEL
      - Do NOT call any number the scammer provided
      - Do NOT use number from the "bank email" you received
      - Use the number on the back of your debit card
      - Or find it on our official website
   
   4. Report the fraud
      - Email: fraud-desk@sentinelbank.ng
      - Provide: Scammer's phone number, what they said, what you shared
   
   5. Monitor your account
      - Check transaction history every hour for next 24 hours
      - Report any unauthorized transactions immediately

   **How to Verify If a Call Is Really From Us**:
   - Hang up and call our official number yourself
   - We will never be offended if you verify
   - Real bank staff will UNDERSTAND and APPRECIATE your caution
   - Scammers will get angry or pressure you (red flag!)

   **Common Scam Tactics** (Be Aware):
   - "Your account will be closed today if you don't act now!"
   - "We detected suspicious activity. Verify with your PIN."
   - "You've won a prize! Send ₦5,000 to claim."
   - "Your BVN needs update. Send your OTP to complete."
   - "I'm from CBN/EFCC. We need your account details."

   **REMEMBER**: 
   We will NEVER pressure you to act immediately!
   If someone is rushing you, it's a scam. Hang up.


Q12: How do I know if a transaction notification is real?
---------------------------------------------------------------------------
A: **Genuine Bank Alerts Look Like This**:

   ✅ **From Official Sender ID**: "SENTINEL" or our registered shortcode
   ✅ **Specific Transaction Details**:
      - Exact amount debited/credited: "₦15,000.00"
      - Transaction time: "17/02/2026 2:45 PM"
      - Your current account balance: "Bal: ₦1,234,567.89"
      - Transaction type: "POS", "Transfer", "ATM", "Bill Payment"
      - Last 4 digits of your account: "...1234"
   ✅ **Professional Language**: No typos, proper grammar
   ✅ **Never Includes Links**: Real alerts don't ask you to click anything
   ✅ **Never Requests Information**: No "Reply with your PIN", etc.

   **Example of REAL Alert**:
   ```
   SENTINEL: Debit Alert
   Amt: NGN25,000.00
   Desc: POS Purchase - Shoprite Ikeja
   Bal: NGN1,456,789.12
   17Feb26 14:23
   Acct: ...4567
   ```

   **Fake Alert Warning Signs**:
   ❌ **Random Sender ID**: "BANKNG", "ALERT", "9876543210"
   ❌ **Vague Details**: "Your account has been debited" (no amount shown)
   ❌ **Poor Grammar**: "You account was debitted" (typos)
   ❌ **Clickable Links**: "Click here to reverse transaction"
   ❌ **Urgent Action Required**: "Reply STOP to reverse"
   ❌ **Requests Information**: "Send your PIN to confirm"
   ❌ **Wrong Balance**: Shows balance you never had

   **Example of FAKE Alert**:
   ```
   BANKNG: Alert!!!
   Your account has been debited. 
   Click https://fake-link.com to stop this 
   transaction now!!!
   ```

   **When In Doubt**:
   1. Login to your mobile app to verify (do NOT click links in SMS!)
   2. Check transaction history in app
   3. Call our official number: 0700-SENTINEL
   4. Visit nearest branch

   **What Scammers Hope For**:
   - You panic and click the link (installs malware)
   - You reply with sensitive information
   - You send money to "reverse" a fake transaction
   - You call the scammer's number (more social engineering)

   **Pro Tip**: 
   Save our official alert sender ID in your contacts as "Sentinel Bank REAL"
   Then you can quickly identify fake senders.


=========================================================================
SECTION 5: ACCOUNT SERVICES
=========================================================================

Q13: How do I request my bank statement?
---------------------------------------------------------------------------
A: Multiple options available depending on your needs:

   **OPTION 1: Mobile App** (Fastest, Recommended)
   - Open Sentinel Bank app
   - Tap "Statements" menu
   - Select date range:
     * Last 30 days (instant)
     * Last 90 days (instant)
     * Custom date range
   - Choose format:
     * PDF (for printing/email)
     * Excel/CSV (for analysis)
   - Download to your phone OR email to yourself
   - Cost: FREE for last 90 days!

   **OPTION 2: Email Request**
   - Send email to: statements@sentinelbank.ng
   - Include in your email:
     * Your full name (as in bank records)
     * Account number
     * Date range needed (e.g., "January 2025 to December 2025")
     * Reason for request (optional but helps us serve you better)
     * Preferred email address for delivery (if different from sender)
   - Response time: Within 24 hours (business days)
   - Fee: ₦50 per month requested
     * Example: 6 months statement = ₦300
     * Debited from your account

   **OPTION 3: USSD Code** (For Basic Info)
   - Dial *737*7# from your registered phone number
   - Shows last 5 transactions only (not full statement)
   - Useful for quick reference
   - Free

   **OPTION 4: Branch Visit**
   - Visit any Sentinel Bank branch nationwide
   - Bring valid ID
   - Can request any historical period (even 10 years ago!)
   - Choose:
     * Immediate collection (printed, collect same day)
     * Email delivery (sent within 1 hour)
   - Fee: ₦100 per month (charged once regardless of pages)
   - Processing time: 15-30 minutes (if you wait at branch)

   **Common Reasons for Statement Requests**:
   - Visa application (embassy requires bank statement)
   - Loan application
   - Auditing/accounting purposes
   - Dispute resolution (prove you paid)
   - Personal financial planning

   **Pro Tips**:
   - For visa applications: Request 6 months statement
   - For loans: Request 3-12 months depending on lender requirement
   - Save statements monthly (helps with taxes, budgeting)


Q14: Why am I being charged monthly service fees?
---------------------------------------------------------------------------
A: Let's break down all the charges you might see:

   **ACCOUNT MAINTENANCE CHARGES**:
   
   Savings Account:
   - Monthly fee: ₦50
   - Waived if: You maintain minimum balance of ₦100,000
   - Why charged: Account upkeep, system maintenance, customer support
   
   Current Account:
   - Monthly fee: ₦300
   - Commission on Turnover (COT): ₦1 per ₦1,000 on third-party deposits
     (Applies to business transactions only)
   - Why charged: Higher operational cost, business banking services

   **OTHER MONTHLY CHARGES** (These Are Separate):
   
   SMS Alerts:
   - Rate: ₦4 per alert
   - Frequency: EVERY transaction (debit or credit)
   - Cannot be opted out (CBN requirement for security)
   - Average monthly: ₦120 (if you have 30 transactions/month)
   - Why charged: Telco costs, SMS gateway fees

   USSD Banking:
   - Rate: ₦6.98 per session
   - Example: If you use *737# 10 times/month = ₦69.80
   - Shared with telco (they get ₦4, we get ₦2.98)
   - Why charged: USSD infrastructure cost

   ATM Withdrawals:
   - Own bank ATMs: First 3 free, then ₦65 per withdrawal
   - Other bank ATMs: ₦65 per withdrawal (all transactions)
   - Example: 8 ATM withdrawals at other banks = ₦520
   - Why charged: ATM maintenance, cash management, networking fees

   **HOW TO MINIMIZE CHARGES**:
   
   For Savings Account:
   - Keep balance above ₦100,000 → ₦50 maintenance waived
   - Use first 3 ATM withdrawals wisely → Save ₦65 each
   - Reduce unnecessary transactions → Fewer SMS alerts
   
   For Current Account:
   - Consolidate transactions → Fewer COT charges
   - Use online banking (free) instead of USSD → Save ₦6.98 per session
   - Maintain transaction discipline

   **TO VIEW YOUR CHARGES**:
   - In mobile app: Transactions → Filter by "Charges"
   - Monthly statement: Charges section shows breakdown
   - For questions about specific charge: Visit branch with statement

   **CHARGES YOU SHOULD NEVER SEE** (Report if you do):
   - Unexplained "miscellaneous charges"
   - Charges for transactions you didn't make
   - Double-charging (same charge appears twice)
   - Charges higher than published rates

   If you see unexpected charges, contact us immediately!


=========================================================================
CONTACT INFORMATION
=========================================================================

**24/7 Customer Care Hotline**:
  - Nigeria: 0700-SENTINEL (0700-736-8463)
  - International: +234-1-SENTINEL
  - WhatsApp: +234-901-000-0000 (Text only, no calls)

**Email Support** (Response within 24 hours):
  - General inquiries: customercare@sentinelbank.ng
  - Fraud reports: fraud-desk@sentinelbank.ng (24/7 monitoring)
  - Complaints: complaints@sentinelbank.ng

**Branch Locations**:
  - Visit: www.sentinelbank.ng/branches
  - Or use in-app branch locator: Menu → Find Branch

**Social Media** (Verified Accounts Only):
  - Twitter: @SentinelBankNG ✓
  - Facebook: /SentinelBankNigeria ✓
  - Instagram: @sentinelbankng ✓
  - (Beware of fake accounts! Look for blue checkmark)

**Banking Hours**:
  - Monday-Friday: 8:00 AM - 4:00 PM
  - Saturday: Selected branches, 9:00 AM - 1:00 PM
  - Sunday: Closed (but online banking works 24/7!)

**Emergency After-Hours**:
  - Card blocking: Available 24/7 via app or hotline
  - Fraud reporting: fraud-desk@sentinelbank.ng (24/7 response)


=========================================================================
END OF DOCUMENT FAQ-001
=========================================================================
"""
        
        return self._package_for_rag(
            "FAQ-001", 
            "Customer Service Frequently Asked Questions", 
            "knowledge_base", 
            "2.0", 
            faq
        )
    
    def generate_all_documents(self) -> List[Dict]:
        """
        Generate all four core policy documents.
        
        Returns:
            List[Dict]: List of packaged documents ready for RAG ingestion
        """
        return [
            self.generate_complaint_handling_policy(),
            self.generate_fraud_detection_guidelines(),
            self.generate_transaction_policies(),
            self.generate_faq_document()
        ]

    # ========================================================================
    # RAG INTEGRATION METHOD
    # ========================================================================
    def save_all_policies(self, output_dir: Path):
        """
        Generate and save all policy documents to disk for RAG ingestion.
        
        This method bridges the policy generator with the RAG system by
        saving documents as .txt files that can be loaded by the document
        ingestion pipeline.
        
        Args:
            output_dir (Path): Base directory where policy files will be saved.
                              Creates 'policies/' and 'faqs/' subdirectories.
        
        Output Directory Structure:
            output_dir/
            ├── policies/
            │   ├── POL-CCH-001.txt (Complaint handling)
            │   ├── FRM-001.txt     (Fraud detection)
            │   └── TSU-POL-002.txt (Transaction policies)
            └── faqs/
                └── FAQ-001.txt     (Customer FAQ)
        
        File Format:
            - Plain text (.txt) for maximum compatibility
            - UTF-8 encoding for special characters (₦ symbol, etc.)
            - Preserves formatting (headers, sections, bullet points)
        
        Integration with RAG:
            1. This method saves documents to disk
            2. ingest_documents.py loads .txt files
            3. Documents are chunked into semantic segments
            4. Embeddings generated and stored in ChromaDB
            5. rag_query.py queries the vector database
        
        Example:
            >>> generator = BankingPolicyGenerator()
            >>> output_path = Path("./knowledge_base")
            >>> generator.save_all_policies(output_path)
            
            Output:
            ✓ Generated: knowledge_base/policies/POL-CCH-001.txt
            ✓ Generated: knowledge_base/policies/FRM-001.txt
            ✓ Generated: knowledge_base/policies/TSU-POL-002.txt
            ✓ Generated: knowledge_base/faqs/FAQ-001.txt
        """
        # Ensure output_dir is a Path object (handles string inputs gracefully)
        output_dir = Path(output_dir)
        
        # Create subdirectories with proper structure
        policies_dir = output_dir / "policies"
        faqs_dir = output_dir / "faqs"
        
        # Create directories (parents=True creates intermediate dirs if needed)
        # exist_ok=True prevents error if directory already exists
        policies_dir.mkdir(parents=True, exist_ok=True)
        faqs_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate all documents (returns list of dictionaries)
        documents = self.generate_all_documents()
        
        print(f"Saving {len(documents)} policy documents...")
        
        # Process and save each document
        for doc in documents:
            # Determine target folder based on document category
            # knowledge_base documents go to faqs/, everything else to policies/
            if doc['category'] == 'knowledge_base':
                target_folder = faqs_dir
            else:
                target_folder = policies_dir
            
            # Create filename from document ID
            # Example: POL-CCH-001 becomes POL-CCH-001.txt
            filename = f"{doc['document_id']}.txt"
            filepath = target_folder / filename
            
            # Write content to file with UTF-8 encoding
            # This preserves special characters like ₦ (Naira symbol)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(doc['content'])
            
            print(f"✓ Generated: {filepath}")
        
        # Print summary
        print(f"\n✓ All policy documents saved to {output_dir}")
        policy_count = len([d for d in documents if d['category'] != 'knowledge_base'])
        faq_count = len([d for d in documents if d['category'] == 'knowledge_base'])
        print(f"  - Policies: {policy_count} files")
        print(f"  - FAQs: {faq_count} files")


# ============================================================================
# MAIN EXECUTION BLOCK
# ============================================================================
if __name__ == "__main__":
    """
    Main execution when script is run directly.
    
    This block:
    1. Creates policy generator instance
    2. Saves all documents to disk
    3. Provides user feedback
    4. Shows next steps for integration
    
    Usage:
        python policy_generator.py
    """
    print("\n" + "="*70)
    print(" "*15 + "BANK POLICY DOCUMENT GENERATOR")
    print(" "*10 + "AI Engineer 2 - Week 1 Knowledge Base Setup")
    print("="*70 + "\n")
    
    # Get output directory (current directory where script is located)
    current_dir = Path(__file__).parent
    
    # Initialize generator with bank name
    generator = BankingPolicyGenerator(bank_name="Sentinel Bank Nigeria")
    
    # Generate and save all documents to disk
    generator.save_all_policies(current_dir)
    
    # Success message with next steps
    print("\n" + "="*70)
    print(" "*20 + "DOCUMENT GENERATION COMPLETE!")
    print("="*70)
    print("\nGenerated Files:")
    print("  policies/POL-CCH-001.txt - Complaint Handling Policy")
    print("  policies/FRM-001.txt     - Fraud Detection Guidelines")
    print("  policies/TSU-POL-002.txt - Transaction Processing Policies")
    print("  faqs/FAQ-001.txt         - Customer Service FAQ")
    print("\nNext Step:")
    print("  cd ../rag_system")
    print("  python ingest_documents.py")
    print("\nThis will load the policies into ChromaDB for RAG queries.")
    print("="*70 + "\n")