"""
Project: AI-Driven Banking Middleware (Capstone)
Organization: The Sentinels / AI Fellowship NCC
Role: AI Engineer 2 - Week 1 Deliverable (UPDATED - v2)
Document: Full-Scale Grounding Policy Engine (Enhanced Production Version)

CHANGELOG v2:
  - All policies tightly aligned to actual dataset generator field names,
    merchant categories, channel names, fraud trace flags, SLA values,
    department codes, and recommended product criteria from data_generator.py
  - Policy language written for zero-ambiguity agent consumption

Author: AI Engineer 2 (Security & Knowledge Specialist)
Date: February 2026
"""

import json
import uuid
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path


# =============================================================================
# SHARED CONSTANTS — imported by rag_query.py for zero-drift alignment
# =============================================================================
# These values are the single source of truth for risk weights, SLA hours,
# and department metadata across the entire AI middleware system.
# data_generator.py, policy_generator.py, and rag_query.py all read from here.

# Merchant category risk weights (FRM-002, Section 1)
# Keys match merchant_category values in transactions.csv exactly.
MERCHANT_RISK: Dict[str, int] = {
    "fintech":     25,   # Highest — primary fraud exit channel
    "transport":   15,   # Card-testing indicator (Uber/Bolt)
    "education":   15,   # Social engineering scam cover
    "healthcare":  15,   # Emergency scam payments
    "telecoms":     5,   # Airtime resale at high volume
    "supermarket":  0,   # Routine essential spending
    "restaurants":  0,   # Routine food spending
    "fuel":         0,   # Routine predictable amounts
    "utilities":    0,   # Scheduled bill payments
}

# Fraud trace flag risk weights (FRM-001, Section 1)
# Keys match fraud_explainability_trace comma-separated values in transactions.csv exactly.
FLAG_WEIGHTS: Dict[str, int] = {
    "mobile_channel_risk": 15,   # channel="mobile_app" + is_fraud_score=1
    "high_amount_spike":   25,   # amount > account_balance*0.6 + is_fraud_score=1
    "multiple_failures":   20,   # status="failed" repeated + is_fraud_score=1
    "normal_pattern":       0,   # Baseline — no additional risk
}

# SLA hours per department (POL-CCH-001, Section 2)
# Must match sla_hours_limit values in complaints.csv exactly.
EXPECTED_SLA: Dict[str, int] = {
    "TSU": 48,
    "COC": 48,
    "FRM": 24,
    "DCS": 72,
    "AOD": 72,
    "CLS": 96,
}

# Full department names keyed by department_code (POL-CCH-001, Section 2)
DEPT_NAMES: Dict[str, str] = {
    "TSU": "Transaction Services Unit",
    "COC": "Card Operations Center",
    "FRM": "Fraud Risk Management",
    "DCS": "Digital Channels Support",
    "AOD": "Account Operations Department",
    "CLS": "Credit & Loan Services",
}

# Risk score thresholds (FRM-001, Section 2.2)
RISK_THRESHOLDS = {
    "LOW":      (0,  30),
    "MEDIUM":   (31, 60),
    "HIGH":     (61, 85),
    "CRITICAL": (86, 100),
}

# Product recommendation thresholds (PRS-001, Section 1)
# Must match data_generator.py hierarchy exactly.
PRODUCT_THRESHOLDS = {
    "Investment Plan": {"monthly_inflow_min": 2_000_000},
    "Car Loan":        {"car_loan_signal_score_min": 0.7},
    "Personal Loan":   {"monthly_inflow_min": 300_000, "salary_detected": True},
}

# Car loan signal score component weights (PRS-001, Section 2)
CAR_LOAN_SIGNAL_WEIGHTS = {
    "uber_tracker_min":       6,     # uber_tracker >= 6 required to add score
    "uber_tracker_score":     0.4,
    "salary_detected_score":  0.3,
    "monthly_inflow_min":     500_000,
    "monthly_inflow_score":   0.3,
    "eligibility_threshold":  0.7,
}


class BankingPolicyGenerator:
    """
    Enterprise-grade generator for comprehensive banking policies.

    Generates six core policy documents that serve as the ground truth
    for the RAG-based AI middleware system:

    1. Complaint Handling Policy (POL-CCH-001)       → Dispatcher Agent
    2. Fraud Detection Guidelines (FRM-001)           → Sentinel Agent
    3. Transaction Processing Policies (TSU-POL-002) → All Agents
    4. Customer Service FAQ (FAQ-001)                 → Customer-Facing
    5. Merchant Risk Profiles (FRM-002)               → Sentinel Agent
    6. Product Recommendation Policy (PRS-001)        → Trajectory Agent

    All policy content is aligned 1-to-1 with the dataset generator
    (data_generator.py) field names, enumerations, thresholds, and
    business logic to prevent agent hallucination or misrouting.

    Usage:
        generator = BankingPolicyGenerator(bank_name="Sentinel Bank Nigeria")
        generator.save_all_policies(Path("./knowledge_base"))
    """

    def __init__(self, bank_name: str = "Sentinel Bank Nigeria"):
        self.bank_name = bank_name
        self.generation_time = datetime.now().isoformat()
        self.display_date = datetime.now().strftime('%B %Y')

        self.system_meta = {
            "project": "AI-Driven Banking Middleware",
            "organization": "The Sentinels / AI Fellowship NCC",
            "jurisdiction": "Nigeria",
            "version": "2026.Q1.v2-COMPLETE",
            "data_classification": "Synthetic/Proprietary"
        }

    def _package_for_rag(self, doc_id: str, title: str, category: str,
                         version: str, text: str) -> Dict[str, Any]:
        return {
            "document_id": doc_id,
            "title": title,
            "category": category,
            "version": version,
            "metadata": {
                **self.system_meta,
                "title": title,
                "category": category,
                "version": version,
                "uuid": str(uuid.uuid4()),
                "last_modified": self.generation_time
            },
            "content": text.strip(),
            "last_updated": self.generation_time
        }

    # =========================================================================
    # DOCUMENT 1: COMPLAINT HANDLING POLICY (POL-CCH-001)
    # =========================================================================

    def generate_complaint_handling_policy(self) -> Dict:
        """
        Generate comprehensive complaint handling and routing policy.

        Dataset alignment:
          - department_code values: TSU, COC, FRM, DCS, AOD, CLS
          - priority_level values: Critical, High, Medium, Low
          - sla_hours_limit per department: TSU=48, COC=48, FRM=24,
            DCS=72, AOD=72, CLS=96
          - complaint channels: call_center, mobile_app, email, branch,
            social_media
          - fraud_related field: 0 or 1 (drives FRM routing)
          - is_fraud_score field: 1 → always Critical + FRM
          - transaction_status: failed/timeout/reversed → TSU or COC
          - channel: atm/pos → COC; others → TSU
        """
        policy_content = f"""
=========================================================================
{self.bank_name} - CUSTOMER COMPLAINT HANDLING & ROUTING POLICY
=========================================================================

Document ID     : POL-CCH-001
Version         : 2.1
Effective Date  : January 2025
Classification  : Internal Use Only — AI Agent Operational Reference
Review Cycle    : Quarterly

=========================================================================
SECTION 1: PURPOSE & SCOPE
=========================================================================

This policy establishes standardized, deterministic procedures for
receiving, classifying, routing, and resolving customer complaints
across all service channels at {self.bank_name}.

Primary Consumers of This Policy:
  - Dispatcher Agent: Uses routing rules in Section 2 to assign every
    inbound complaint to the correct department on first contact.
  - Sentinel Agent: Uses fraud_related field and FRM routing rules to
    escalate fraud-linked complaints immediately.
  - Human supervisors: Use escalation matrix in Section 4 for overrides.

Key Objectives:
  1. Route complaints to the correct department with >95% accuracy
     on first contact, eliminating manual re-routing overhead.
  2. Enforce SLA compliance by department (exact hours in Section 2).
  3. Provide deterministic escalation paths for every complaint tier.
  4. Enable full auditability — every routing decision must cite a
     policy section and department code from this document.

Complaint Intake Channels (complaint_channel field values):
  - call_center   : Phone call to customer care hotline
  - mobile_app    : Complaint submitted inside the Sentinel mobile app
  - email         : Complaint submitted via email
  - branch        : In-person complaint at any Sentinel Bank branch
  - social_media  : Twitter, Facebook, Instagram, LinkedIn DMs


=========================================================================
SECTION 2: COMPLAINT CATEGORIES & DEPARTMENT ROUTING
=========================================================================

ROUTING DECISION LOGIC FOR DISPATCHER AGENT
─────────────────────────────────────────────
The Dispatcher Agent MUST evaluate fields in this exact priority order:

  STEP 1: If fraud_related == 1 OR is_fraud_score == 1
          → Route to FRM (Priority: Critical). STOP. Do not evaluate further.

  STEP 2: If transaction channel (channel field) is "atm" or "pos"
          AND transaction_status is NOT "successful"
          → Route to COC (Priority: High). STOP.

  STEP 3: If transaction_status is "failed", "timeout", or "reversed"
          AND channel is "nibss_transfer", "mobile_app", "web", or "ussd"
          → Route to TSU (Priority: High). STOP.

  STEP 4: If complaint text references app, login, USSD, crash, error
          → Route to DCS (Priority: Medium).

  STEP 5: If complaint text references statement, balance, charges,
          account closure, BVN
          → Route to AOD (Priority: Medium or Low).

  STEP 6: If complaint text references loan, credit, repayment,
          disbursement, overdraft
          → Route to CLS (Priority: Medium).

  DEFAULT: Route to TSU (Priority: Medium).


──────────────────────────────────────────────────────────────────────────
2.1  TRANSACTION SERVICES UNIT (TSU)
──────────────────────────────────────────────────────────────────────────
Department Code   : TSU
Department Name   : Transaction Services Unit
SLA (Hours)       : 48 hours  ← sla_hours_limit = 48 in dataset
Team Size         : 12 specialists + 2 managers

HANDLES:
  - Transfer debited from sender but credit not received by beneficiary
  - Duplicate debit transactions (same amount charged twice)
  - Incorrect transaction amounts (wrong figure debited or credited)
  - Failed interbank NIP/NIBSS transfers (status = "failed" or "timeout")
  - Reversed transactions that customer disputes (status = "reversed")
  - Missing inbound credit (expected credit not posted)
  - Transaction narration or reference number discrepancies

CHANNELS THAT PRIMARILY GENERATE TSU COMPLAINTS:
  nibss_transfer, mobile_app, ussd, web, branch

PRIORITY ASSIGNMENT RULES FOR TSU:
  - High   : Amount > ₦100,000 OR status = "failed"/"timeout"/"reversed"
  - Medium : Amount ≤ ₦100,000 AND status = "successful" (dispute only)
  - Low    : Narration/reference query, no financial impact

SLA TARGETS:
  - Acknowledgement    : Within 2 hours
  - Resolution         : Within 48 hours (business hours)
  - Reversal (if due)  : Within 24 hours of confirmation
  - Interbank disputes : Up to 14 days (NIBSS coordination required)

ESCALATION TRIGGERS:
  - Unresolved after 72 hours
  - Amount exceeds ₦500,000
  - Customer threatens regulatory complaint to CBN

COMPLAINT TEXT KEYWORDS THAT TRIGGER TSU ROUTING:
  "transfer", "debit", "not received", "wrong amount",
  "reversal", "failed transaction", "duplicate charge",
  "credited", "debited twice", "NIBSS", "interbank"

EXAMPLES:
  ✓ "I transferred ₦100,000 to GTBank but recipient has not received it."
  ✓ "My account was debited twice for the same payment."
  ✓ "ATM did not dispense cash but my account shows debit." (SEE NOTE)
  ✗ "My card was declined at POS." → Route to COC, not TSU.

NOTE: ATM dispense error (channel = "atm") is COC. Pure interbank
      transfer failure (channel = "nibss_transfer") is TSU.


──────────────────────────────────────────────────────────────────────────
2.2  CARD OPERATIONS CENTER (COC)
──────────────────────────────────────────────────────────────────────────
Department Code   : COC
Department Name   : Card Operations Center
SLA (Hours)       : 48 hours  ← sla_hours_limit = 48 in dataset
Team Size         : 8 specialists + 1 manager

HANDLES:
  - Card declined at POS terminal or ATM (domestic or international)
  - Card retained/swallowed by ATM machine
  - Unauthorized card transactions (card present fraud)
  - Lost or stolen card blocking requests
  - PIN reset requests and PIN lock issues
  - Card activation failures (new card not working)
  - Chip or magnetic stripe malfunction
  - Contactless payment failures
  - Card replacement requests

CHANNELS THAT PRIMARILY GENERATE COC COMPLAINTS:
  atm, pos

PRIORITY ASSIGNMENT RULES FOR COC:
  - Critical : Fraud amount > ₦100,000 AND is_fraud_score = 1
  - High     : ATM card swallowed, POS failure with debit confirmed,
               card declined while account has sufficient funds
  - Medium   : PIN reset, card replacement request
  - Low      : Card information query, contactless setup

SLA TARGETS:
  - Emergency card blocking   : Immediate (within 5 minutes)
  - Card replacement          : 3–5 business days
  - Dispute investigation     : 7–14 days
  - PIN reset                 : Same day (within 4 hours)
  - ATM card retrieval        : 24–48 hours

ESCALATION TRIGGERS:
  - Fraud amount exceeds ₦100,000
  - Multiple unauthorized transactions on same card
  - International card fraud (higher complexity)

COMPLAINT TEXT KEYWORDS THAT TRIGGER COC ROUTING:
  "card", "PIN", "swallowed", "declined", "POS", "chip",
  "contactless", "ATM", "blocked card", "card stolen"

EXAMPLES:
  ✓ "My card was declined at Shoprite POS even though I have money."
  ✓ "ATM swallowed my card at First Bank Ikeja branch."
  ✓ "I see card transactions I did not make."
  ✗ "My NIBSS transfer was declined." → Route to TSU, not COC.


──────────────────────────────────────────────────────────────────────────
2.3  FRAUD RISK MANAGEMENT (FRM)
──────────────────────────────────────────────────────────────────────────
Department Code   : FRM
Department Name   : Fraud Risk Management
SLA (Hours)       : 24 hours  ← sla_hours_limit = 24 in dataset
Team Size         : 6 specialists + 1 senior manager
Operating Hours   : 24/7 including public holidays

HANDLES:
  - All transactions where is_fraud_score = 1
  - All complaints where fraud_related = 1
  - Suspected unauthorized account access or login
  - Account takeover (ATO) suspicion
  - SIM swap fraud concerns
  - Social engineering scam reports
  - Phishing attempt reporting
  - Suspicious beneficiary additions
  - Multiple authentication failures followed by successful transfer

ROUTING TRIGGER (DETERMINISTIC):
  IF fraud_related == 1 OR is_fraud_score == 1 → ALWAYS FRM, ALWAYS Critical.
  This overrides ALL other routing rules. No exceptions.

PRIORITY ASSIGNMENT RULES FOR FRM:
  - Critical : ALL fraud cases are automatically Critical priority.
               No FRM complaint is ever assigned Medium or Low.

SLA TARGETS:
  - Account freeze capability    : Immediate (within 2 minutes)
  - Investigation initiation     : Within 1 hour of report
  - Customer notification        : Within 4 hours
  - Preliminary assessment       : Within 24 hours ← matches sla_hours_limit
  - Full investigation           : 14–21 days
  - Police report filing         : Within 48 hours (amounts > ₦1,000,000)

ESCALATION TRIGGERS:
  - Fraud amount exceeds ₦1,000,000
  - Suspected insider involvement
  - Multiple customers affected by same fraud pattern
  - Media or social media exposure

COMPLAINT TEXT KEYWORDS THAT TRIGGER FRM ROUTING:
  "fraud", "hacked", "unauthorized", "scam", "phishing",
  "suspicious", "someone used my account", "account takeover",
  "I did not authorize", "compromised"

EXAMPLES:
  ✓ "Someone is making transactions on my account that I did not initiate."
  ✓ "I got a call asking for my OTP and now money is missing."
  ✓ "My account shows login from a location I have never been to."
  ✗ "I was charged twice for the same grocery purchase." → Route to TSU.

CRITICAL OPERATIONAL NOTE:
  NEVER route fraud cases to the IT department or Digital Channels Support.
  NEVER delay a fraud complaint for documentation — freeze the account
  first, gather paperwork second. Every minute of delay costs the customer.


──────────────────────────────────────────────────────────────────────────
2.4  DIGITAL CHANNELS SUPPORT (DCS)
──────────────────────────────────────────────────────────────────────────
Department Code   : DCS
Department Name   : Digital Channels Support
SLA (Hours)       : 72 hours  ← sla_hours_limit = 72 in dataset
Team Size         : 10 specialists + 2 managers

HANDLES:
  - Mobile app login failures (password or biometric errors)
  - Internet banking access problems
  - USSD service errors (unresponsive *737# or related codes)
  - App crashes, hangs, or freezes during transactions
  - Feature malfunction within digital channels
  - Biometric authentication failures (fingerprint or face ID)
  - App update issues causing functionality regression
  - Slow app performance or session timeouts
  - QR code payment failures
  - OTP delivery failures linked to app/platform (not SIM issues)

CHANNELS THAT PRIMARILY GENERATE DCS COMPLAINTS:
  mobile_app, ussd, web

PRIORITY ASSIGNMENT RULES FOR DCS:
  - High   : Issue prevents all digital transactions for customer
  - Medium : App feature broken but alternatives exist
  - Low    : Performance issue, minor UX bug

SLA TARGETS:
  - Level 1 troubleshooting  : Immediate (phone or in-app chat)
  - Technical escalation     : Within 4 hours
  - Resolution               : 24–72 hours ← matches sla_hours_limit

ESCALATION TRIGGERS:
  - Issue affects multiple customers simultaneously (systemic outage)
  - Security vulnerability suspected in digital channel
  - App completely unusable (no transactions possible)

COMPLAINT TEXT KEYWORDS THAT TRIGGER DCS ROUTING:
  "app", "login", "password", "internet banking", "USSD",
  "crash", "freeze", "error message", "biometric", "fingerprint",
  "face ID", "mobile app", "not loading"

EXAMPLES:
  ✓ "I cannot log into the mobile app. It says wrong password."
  ✓ "The app crashes every time I try to initiate a transfer."
  ✓ "USSD *737# is not responding at all."
  ✗ "My transfer failed after I logged in." → Route to TSU, not DCS.


──────────────────────────────────────────────────────────────────────────
2.5  ACCOUNT OPERATIONS DEPARTMENT (AOD)
──────────────────────────────────────────────────────────────────────────
Department Code   : AOD
Department Name   : Account Operations Department
SLA (Hours)       : 72 hours  ← sla_hours_limit = 72 in dataset
Team Size         : 15 specialists + 3 managers

HANDLES:
  - Account statement requests (any date range)
  - Account balance discrepancy inquiries
  - Account closure requests
  - Service charge disputes (maintenance fees, SMS fees)
  - Account type upgrade or downgrade requests
  - Name or address update issues
  - BVN linking or update issues
  - NIN linking requests
  - Dormant account reactivation
  - Account freeze (non-fraud related)
  - Cheque-related issues

PRIORITY ASSIGNMENT RULES FOR AOD:
  - High   : Balance discrepancy > ₦100,000, regulatory compliance issue
  - Medium : Account modification, BVN/NIN update
  - Low    : Statement request, service charge query, general account info

SLA TARGETS:
  - Statement generation      : Within 24 hours
  - Balance inquiry           : Immediate to 4 hours
  - Account modification      : 2–3 business days
  - Account closure           : 5–7 business days
  - BVN/NIN updates           : 24–48 hours

ESCALATION TRIGGERS:
  - Regulatory compliance issue
  - High-value customer (private banking tier)
  - Legal or court-mandated account action

COMPLAINT TEXT KEYWORDS THAT TRIGGER AOD ROUTING:
  "statement", "balance", "close account", "charges",
  "upgrade", "update details", "BVN", "NIN", "dormant",
  "maintenance fee", "account freeze"

EXAMPLES:
  ✓ "I need a bank statement for the last 6 months for a visa application."
  ✓ "Why am I being charged a monthly maintenance fee?"
  ✓ "I want to close my savings account."
  ✗ "I want to check if my interbank transfer was received." → TSU.


──────────────────────────────────────────────────────────────────────────
2.6  CREDIT & LOAN SERVICES (CLS)
──────────────────────────────────────────────────────────────────────────
Department Code   : CLS
Department Name   : Credit & Loan Services
SLA (Hours)       : 96 hours  ← sla_hours_limit = 96 in dataset
Team Size         : 6 specialists + 1 manager

HANDLES:
  - Loan disbursement delays or failures
  - Interest calculation disputes
  - Repayment schedule issues or missed auto-debit
  - Credit limit increase requests
  - Loan restructuring requests
  - Early repayment and settlement queries
  - Loan documentation problems
  - Overdraft facility queries and disputes

PRIORITY ASSIGNMENT RULES FOR CLS:
  - High   : Loan amount > ₦5,000,000, customer threatens default
  - Medium : Standard loan queries, repayment schedule issues
  - Low    : Information requests about loan products

SLA TARGETS:
  - Query response             : Within 48 hours
  - Disbursement issue         : 3–5 business days
  - Credit review              : 7–14 days
  - Restructuring decision     : 14–21 days ← matches sla_hours_limit = 96h for first contact

COMPLAINT TEXT KEYWORDS THAT TRIGGER CLS ROUTING:
  "loan", "credit", "disbursement", "interest", "repayment",
  "overdraft", "restructure", "settlement", "installment"

EXAMPLES:
  ✓ "My loan was approved but money has not been disbursed."
  ✓ "The interest on my car loan seems higher than agreed."
  ✓ "I want to pay off my personal loan early."
  ✗ "My salary credit is missing." → Route to TSU, not CLS.


=========================================================================
SECTION 3: COMPLAINT PRIORITY CLASSIFICATION
=========================================================================

PRIORITY MATRIX — EXACT FIELD VALUES USED IN DATASET:

┌──────────┬──────────────────────────────────────────────────────────┐
│ Priority │ Criteria                                                 │
├──────────┼──────────────────────────────────────────────────────────┤
│ Critical │ - fraud_related = 1 OR is_fraud_score = 1 (always)      │
│          │ - Account access fully blocked (cannot transact)         │
│          │ - Large unauthorized transfer > ₦500,000                 │
│          │ - Suspected account takeover (ATO)                       │
│          │ - SIM swap with simultaneous high-value transfer         │
│          │ Response: IMMEDIATE (within 5 minutes)                   │
├──────────┼──────────────────────────────────────────────────────────┤
│ High     │ - ATM card retention (channel = "atm")                   │
│          │ - Failed high-value transaction (₦100,000–₦500,000)      │
│          │ - Repeated service failure (same issue 3+ times)         │
│          │ - Corporate/business account issue                       │
│          │ - International transaction problems                     │
│          │ Response: Within 2 hours                                 │
├──────────┼──────────────────────────────────────────────────────────┤
│ Medium   │ - Standard transaction dispute < ₦100,000                │
│          │ - App technical issue (non-critical)                     │
│          │ - Service charge query                                   │
│          │ - Statement request                                      │
│          │ - General card issue (non-fraud)                         │
│          │ Response: Within 4 hours                                 │
├──────────┼──────────────────────────────────────────────────────────┤
│ Low      │ - General inquiries (no financial impact)                │
│          │ - Product information requests                           │
│          │ - Non-urgent statement requests                          │
│          │ - Educational queries                                    │
│          │ Response: Within 24 hours                                │
└──────────┴──────────────────────────────────────────────────────────┘


=========================================================================
SECTION 4: ESCALATION MATRIX
=========================================================================

LEVEL 1 — First Contact Resolution (FCR)
  Handler    : Customer Service Representative (CSR) or AI Agent
  Target     : 70% of complaints resolved at this level
  Trigger Up : Cannot resolve within SLA OR requires specialist access

LEVEL 2 — Specialist Escalation
  Handler    : Departmental specialist (subject matter expert)
  Target     : 25% of complaints escalate here
  Trigger    : Level 1 SLA breached (sla_breach_flag = 1) OR complexity
               requires backend system access OR high-value customer
  Trigger Up : Unresolved after 48 hours OR customer escalation request

LEVEL 3 — Management Escalation
  Handler    : Department manager or team lead
  Target     : ~4% of complaints escalate here
  Trigger    : 7 days unresolved OR legal threat OR private banking client
               OR media attention OR customer complaint to CBN
  Trigger Up : 14 days unresolved OR regulatory involvement

LEVEL 4 — Executive Escalation
  Handler    : Executive Committee (C-level)
  Target     : < 1% of complaints reach this level
  Trigger    : CBN formal inquiry, systemic failure, board-level risk,
               insider fraud suspicion, viral media damage


=========================================================================
SECTION 5: PROHIBITED ACTIONS
=========================================================================

1. NEVER route fraud_related=1 complaints to any department other than FRM.
2. NEVER delay account freeze when fraud is suspected — act first, document second.
3. NEVER promise resolution within a timeframe shorter than the SLA hours in Section 2.
4. NEVER share customer PII via WhatsApp, personal email, or unencrypted SMS.
5. NEVER close a complaint ticket without customer confirmation of resolution.
6. NEVER route based on the intake channel — route based on issue type only.
7. NEVER route an FRM complaint to DCS or TSU even if the fraud occurred on a
   digital channel or during a failed transfer.


=========================================================================
SECTION 6: DOCUMENTATION REQUIREMENTS
=========================================================================

MANDATORY FIELDS (all must be populated before ticket routing):

  complaint_id              : System-generated (format: CMP-XXXXXX)
  customer_id               : Account or customer UUID from system
  linked_transaction_id     : UUID from transactions table (if applicable)
  linked_reference          : transaction_reference_number (if applicable)
  department_code           : TSU | COC | FRM | DCS | AOD | CLS
  department_name           : Full name matching department_code
  priority_level            : Critical | High | Medium | Low
  sentiment                 : angry | neutral | calm
  complaint_channel         : call_center | mobile_app | email | branch | social_media
  assigned_agent_id         : Agent ID from department roster
  complaint_timestamp       : ISO datetime of complaint receipt
  sla_hours_limit           : Exact SLA hours from Section 2 per department
  sla_breach_flag           : 1 if resolution_time_hours > sla_hours_limit; else 0
  fraud_related             : 1 if is_fraud_score = 1; else 0
  complaint_text            : Full verbatim customer complaint text
  complaint_status          : open | resolved | escalated


=========================================================================
SECTION 7: POLICY OWNERSHIP & GOVERNANCE
=========================================================================

Policy Owner    : Head of Customer Experience
Approver        : Chief Operations Officer (COO)
Review Cycle    : Quarterly
Last Updated    : {self.display_date}
Contact         : customer.experience@sentinelbank.ng | Ext. 5000


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

    # =========================================================================
    # DOCUMENT 2: FRAUD DETECTION GUIDELINES (FRM-001)
    # =========================================================================

    def generate_fraud_detection_guidelines(self) -> Dict:
        """
        Generate exhaustive fraud detection and prevention guidelines.

        Dataset alignment:
          - fraud_explainability_trace flags (exact names from generator):
              mobile_channel_risk   : channel == "mobile_app"
              high_amount_spike     : amount > (balance * 0.6)
              multiple_failures     : status == "failed"
              normal_pattern        : no fraud detected
          - is_fraud_score field: 0 or 1
          - channel values: mobile_app, ussd, atm, pos, web, branch,
            nibss_transfer
          - Risk score thresholds: 0-30=LOW, 31-60=MEDIUM, 61-85=HIGH,
            86-100=CRITICAL
          - Merchant categories from MERCHANTS dict:
            supermarket, restaurants, fuel, transport, telecoms,
            utilities, fintech, education, healthcare
        """
        guidelines = f"""
=========================================================================
{self.bank_name} - FRAUD DETECTION & PREVENTION GUIDELINES
=========================================================================

Document Code       : FRM-001
Classification      : CONFIDENTIAL — AI Agent Operational Reference
Version             : 4.0
Effective Date      : January 2026
Review Frequency    : Monthly

Document Owner      : Chief Risk Officer (CRO)
Emergency Contact   : fraud-desk@sentinelbank.ng (Active 24/7)


=========================================================================
SECTION 1: FRAUD EXPLAINABILITY TRACE FLAGS
=========================================================================

CRITICAL ALIGNMENT NOTE FOR SENTINEL AGENT:
The fraud_explainability_trace field in transactions.csv contains a
comma-separated list of flags from the set below. The field contains
"normal_pattern" when no fraud is detected. The Sentinel Agent MUST
parse this field and map each flag to its risk weight using this section.

Each flag has a fixed risk weight (points). Risk score = sum of all
active flags, capped at 100. See Section 2 for thresholds and actions.

──────────────────────────────────────────────────────────────────────────
FLAG: mobile_channel_risk
──────────────────────────────────────────────────────────────────────────
Risk Weight       : +15 points
Trigger Condition : Transaction channel = "mobile_app"
                    AND is_fraud_score = 1
Definition        : Transaction was initiated via the mobile application
                    on a device that may not be the customer's primary
                    registered device, or the session exhibits behavioral
                    anomalies (velocity, geolocation, device fingerprint).
Why It Matters    : Mobile banking is the primary attack surface for
                    account takeover (ATO) and SIM-swap fraud in Nigeria.
                    Fraudsters use stolen credentials on fresh devices.
Standalone Action : +15 points added to risk score. Combine with other
                    flags to determine final action.
Example           : txn with channel="mobile_app" + is_fraud_score=1
                    → mobile_channel_risk flag present in trace


──────────────────────────────────────────────────────────────────────────
FLAG: high_amount_spike
──────────────────────────────────────────────────────────────────────────
Risk Weight       : +25 points
Trigger Condition : Transaction amount > (account current_balance × 0.6)
                    AND is_fraud_score = 1
Definition        : The transaction amount represents more than 60% of
                    the account's current balance, indicating an attempt
                    to rapidly drain the account in a single transaction.
Why It Matters    : Fraudsters who gain account access attempt to extract
                    the maximum possible amount in one move before
                    detection. High percentage-of-balance withdrawals are
                    a primary indicator of account draining behavior.
Standalone Action : +25 points added to risk score.
Example           : balance=₦750,000, amount=₦500,000 → 66.7% of balance
                    → high_amount_spike flag triggered


──────────────────────────────────────────────────────────────────────────
FLAG: multiple_failures
──────────────────────────────────────────────────────────────────────────
Risk Weight       : +20 points
Trigger Condition : transaction_status = "failed"
                    AND is_fraud_score = 1
Definition        : The transaction failed after being initiated, but
                    the attempt itself indicates abnormal retry behavior,
                    rapid successive attempts, or credential testing
                    consistent with brute-force or card-testing attacks.
Why It Matters    : Fraudsters test card details or account credentials
                    by making small or repeated failed transactions to
                    identify valid combinations before executing large
                    unauthorized transfers.
Standalone Action : +20 points added to risk score.
Example           : status="failed" + is_fraud_score=1 → flag present


──────────────────────────────────────────────────────────────────────────
FLAG: normal_pattern
──────────────────────────────────────────────────────────────────────────
Risk Weight       : 0 points
Trigger Condition : No fraud flags detected (is_fraud_score = 0 AND
                    no behavioral anomalies present)
Definition        : Transaction aligns with the customer's historical
                    behavioral patterns: known device, usual channel,
                    typical amount range, normal timing.
Action            : Process transaction without friction. Issue standard
                    SMS/app confirmation alert after completion.


=========================================================================
SECTION 2: RISK SCORE CALCULATION & ACTION THRESHOLDS
=========================================================================

CALCULATION METHOD:
  risk_score = SUM of all active flag weights (from Section 1 + FRM-002)
  risk_score is CAPPED at 100.

EXAMPLE CALCULATION:
  mobile_channel_risk:   +15 points
  high_amount_spike:     +25 points
  merchant (fintech):    +25 points  [from FRM-002]
  ─────────────────────────────────
  Total:                  65 points → HIGH risk level

──────────────────────────────────────────────────────────────────────────
RISK LEVEL: LOW  (Score 0–30)
──────────────────────────────────────────────────────────────────────────
Assessment      : Transaction aligns with normal customer behavior.
                  Known device, typical amount, expected channel.
Action          : PROCESS IMMEDIATELY — no challenge or friction.
Notification    : SMS + App push after transaction completes.
                  "You sent ₦X to [Name] via Sentinel Bank."
Channel Action  : All channels proceed normally.

──────────────────────────────────────────────────────────────────────────
RISK LEVEL: MEDIUM  (Score 31–60)
──────────────────────────────────────────────────────────────────────────
Assessment      : Single anomaly detected. Unusual but not definitively
                  fraudulent (e.g., new merchant, slightly elevated amount).
Action          : STEP-UP AUTHENTICATION via OTP.
                  - Send SMS OTP to registered phone number.
                  - OR email OTP to registered email.
                  - Hold transaction in "Pending" state for 5 minutes.
                  - Auto-decline if OTP not entered within 5 minutes.
Notification    : "We detected unusual activity. Confirm with OTP sent
                  to 080****1234."
Channel Caveat  : If SIM swap detected in last 48 hours, escalate to HIGH
                  and use push-to-app instead of SMS OTP.

──────────────────────────────────────────────────────────────────────────
RISK LEVEL: HIGH  (Score 61–85)
──────────────────────────────────────────────────────────────────────────
Assessment      : Multiple anomalies present. Strong fraud indicators.
                  Transaction should not proceed without explicit biometric
                  customer approval.
Action          : MANDATORY PUSH-TO-APP BIOMETRIC CHALLENGE.
                  Step 1: Place transaction in "Pending - Security Check".
                  Step 2: Send encrypted push notification to registered app.
                  Step 3: Display in app:
                          - Beneficiary Name (bold)
                          - Beneficiary Bank
                          - Amount + fees
                          - Risk reason (e.g., "New device detected")
                          - Transaction reference
                  Step 4: Customer must tap APPROVE or REJECT.
                  Step 5: APPROVE requires biometric (FaceID/Fingerprint/PIN).
                  Step 6: 5-minute timeout → auto-decline if no response.
requires_challenge : True
should_block       : False
Notification    : "Security Alert: Verify this transaction in your
                  Sentinel app."

──────────────────────────────────────────────────────────────────────────
RISK LEVEL: CRITICAL  (Score 86–100)
──────────────────────────────────────────────────────────────────────────
Assessment      : Pattern matches confirmed fraud profile. Transaction
                  must not proceed under any circumstances.
Action          : BLOCK TRANSACTION IMMEDIATELY.
                  Step 1: Decline transaction instantly (no OTP/challenge).
                  Step 2: Freeze account — ALL channels disabled.
                  Step 3: Block all cards linked to account.
                  Step 4: Disable mobile app, internet banking, USSD.
                  Step 5: Generate URGENT FRM alert (highest priority queue).
                  Step 6: Send security alert to registered EMAIL only
                          (NOT SMS — phone number may be compromised).
                  Step 7: Assign to FRM specialist within 30 minutes.
requires_challenge : False
should_block       : True
Notification    : Email: "Your account has been temporarily frozen due to
                  suspicious activity. Contact: fraud-desk@sentinelbank.ng"


=========================================================================
SECTION 3: PUSH-TO-APP AUTHORIZATION PROTOCOL
=========================================================================

WHY PUSH-TO-APP REPLACES SMS OTP:
  - SIM swap attacks intercept SMS — push-to-app is device-bound.
  - OTP via SMS can be delayed; push is near-instant.
  - Biometric binding means only the real customer can approve.
  - Full transaction details shown — customer knows exactly what to approve.

COMPLETE PUSH-TO-APP SEQUENCE:
  1. Transaction placed in "Pending - Security Verification" (max 300 seconds)
  2. Encrypted push notification sent to registered device (device-specific key)
  3. App displays full transaction context:
       - Beneficiary full name
       - Beneficiary bank name
       - Principal amount
       - NIP transfer fee (if applicable)
       - Cybersecurity levy (0.5% of principal)
       - Total debit from account
       - Risk reason triggering the challenge
       - Unique transaction reference number
  4. Customer taps APPROVE or REJECT (large, clear buttons)
  5. APPROVE flow:
       - FaceID / Fingerprint / 6-digit secure transaction PIN required
       - Transaction processed immediately on biometric success
       - Confirmation: "Transaction successful — ₦X sent to [Name]"
  6. REJECT flow:
       - Transaction cancelled immediately
       - Account access blocked (security hold)
       - FRM alert generated (URGENT priority)
       - SMS: "Transaction blocked per your request. Account frozen.
               Contact fraud-desk@sentinelbank.ng"
       - FRM specialist assigned within 30 minutes
  7. TIMEOUT flow (no response in 5 minutes):
       - Transaction auto-declined
       - SMS: "Transaction timed out. If you did not initiate this,
               contact us immediately."
       - Account flagged for review (not frozen yet)


=========================================================================
SECTION 4: COMMON FRAUD SCENARIOS IN NIGERIA (2026)
=========================================================================

4.1 SIM SWAP FRAUD
──────────────────────────────────────────────────────────────────────────
Attack Pattern:
  1. Fraudster obtains customer BVN/NIN via phishing or data breach.
  2. Fraudster presents fake ID at MNO store to swap phone number to new SIM.
  3. Fraudster now receives all SMS including banking OTPs.
  4. Fraudster resets banking password using SMS OTP.
  5. Fraudster drains account before customer notices.

Detection:
  - Real-time SIM swap detection via MNO API query on every login.
  - SIM swap in last 48 hours → automatic 48-hour transfer freeze.
  - Notify customer via EMAIL only (SMS compromised).

Mitigation:
  - Push-to-app replaces SMS OTP as primary challenge method.
  - Branch visit required to reactivate transfers after SIM swap.
  - Biometric re-enrollment mandatory after SIM swap event.

4.2 ACCOUNT TAKEOVER (ATO)
──────────────────────────────────────────────────────────────────────────
Fraud Trace Flags Commonly Present:
  mobile_channel_risk + high_amount_spike (two flags → 40 points minimum)

Attack Pattern:
  1. Fraudster obtains login credentials via phishing.
  2. Fraudster changes registered email and phone in account profile.
  3. Fraudster resets password (OTP goes to fraudster's new contact).
  4. Fraudster withdraws maximum amount.

Detection:
  - Profile change alert sent to OLD contact immediately.
  - 24-hour cooling-off period after profile changes before high-value
    transfers are permitted.
  - New device flag (mobile_channel_risk) triggers on first login.

4.3 PHISHING & SOCIAL ENGINEERING
──────────────────────────────────────────────────────────────────────────
Attack Pattern:
  1. Customer receives call/SMS from "bank official."
  2. Fraudster creates urgency: "Your account will be blocked!"
  3. Fraudster obtains OTP/PIN from panicked customer.
  4. Fraudster completes transfer while on call with customer.

Detection:
  - In-app warning during high-risk transfers:
    "SENTINEL BANK WILL NEVER ASK FOR YOUR PIN VIA PHONE.
    Are you being pressured into this transaction? If YES, tap REJECT."

4.4 POS & AGENCY BANKING FRAUD
──────────────────────────────────────────────────────────────────────────
Flags: multiple_failures (card testing at POS terminals)

Detection:
  - Duplicate charge detection: same terminal, similar amount (±5%),
    within 5 minutes → auto-reverse second charge.
  - Unusual terminal activity pattern → terminal blacklist.

4.5 FINTECH & CRYPTO FRAUD
──────────────────────────────────────────────────────────────────────────
Merchant Category: fintech (high-risk per FRM-002)

Pattern:
  - Large one-time transfers to fintech gateways from accounts with no
    prior fintech transaction history.
  - Elderly or "Vulnerable" segment customers sending to crypto exchanges.

Detection:
  - Combine merchant_category="fintech" risk (+25 from FRM-002) with
    mobile_channel_risk or high_amount_spike for composite score.


=========================================================================
SECTION 5: FRAUD RESPONSE PROTOCOLS
=========================================================================

IMMEDIATE (Automated within 2 minutes of Critical detection):
  - Freeze account (all channels)
  - Block all linked cards
  - Disable app, internet banking, USSD
  - Send security alert to registered EMAIL only
  - Generate urgent FRM alert (highest priority)
  - Log all transactions from last 48 hours for forensics

WITHIN 1 HOUR:
  - FRM specialist assigned
  - Preliminary investigation: transaction logs, device IDs, locations
  - Customer contacted via original registered contact (not new details)
  - Temporary credit up to ₦200,000 if customer confirmed as victim

WITHIN 24 HOURS (SLA target for FRM):
  - Full forensic analysis: IP, device fingerprint, network/money trail
  - NIBSS coordination if funds transferred out
  - Contact destination bank to freeze beneficiary account
  - Police report filed (amounts > ₦1,000,000)
  - Preliminary determination: customer fraud vs. bank fraud

RESOLUTION (14–21 DAYS):
  - Full investigation report
  - Permanent credit/debit adjustments
  - Account reactivation with new credentials
  - Fraud database updates (blacklist devices, beneficiaries, patterns)
  - Customer education session


=========================================================================
SECTION 6: REGULATORY COMPLIANCE REFERENCES
=========================================================================

  - CBN Consumer Protection Framework
  - Nigeria Data Protection Regulation (NDPR) 2019
  - CBN Guidance on Electronic Fraud Management (revised 2024)
  - CBN Exposure Draft on Cyber Resilience (2025)
  - ISO 27001 Information Security Management
  - PCI DSS v4.0


=========================================================================
DOCUMENT CONTROL
=========================================================================

Owner           : Chief Risk Officer (CRO)
Review Freq     : Monthly
Last Updated    : {self.display_date}
Emergency       : fraud-desk@sentinelbank.ng | +234-1-FRAUD-24 (24/7)


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

    # =========================================================================
    # DOCUMENT 3: TRANSACTION PROCESSING POLICIES (TSU-POL-002)
    # =========================================================================

    def generate_transaction_policies(self) -> Dict:
        """
        Generate comprehensive transaction processing policies.

        Dataset alignment:
          - KYC tiers aligned to account_type: savings, solo, current
          - channel values: mobile_app, ussd, atm, pos, web, branch,
            nibss_transfer
          - transaction_status: successful, failed, reversed, pending,
            timeout, queued, processing
          - failure_reason values: none, insufficient_fund,
            network_error, system_failure, system_timeout,
            invalid_account_number, daily_transaction_limit_exceeded,
            compliance_restriction, suspected_fraud, issuer_unavailable
          - currency: NGN
        """
        policies = f"""
=========================================================================
{self.bank_name} - TRANSACTION PROCESSING & LIMITS POLICY
=========================================================================

Document ID     : TSU-POL-002
Version         : 4.0
Effective Date  : January 2026
Classification  : Internal Use Only — AI Agent Operational Reference

Policy Custodian : Head of Transaction Services
Last Review      : {self.display_date}
Next Review      : May 2026


=========================================================================
SECTION 1: TRANSACTION STATUS DEFINITIONS
=========================================================================

The transaction_status field in transactions.csv can hold the following
values. Every downstream agent must understand what each status means
before making a routing or response decision.

STATUS: successful
  Definition   : Transaction completed without error. Funds moved as
                 intended. No agent action required unless customer disputes.
  failure_reason: "none"
  Agent Action  : No action. Send standard confirmation alert.

STATUS: failed
  Definition   : Transaction was initiated but could not be completed due
                 to a system, network, or compliance error. The debit was
                 NOT applied (or will be auto-reversed within 24 hours).
  failure_reason: insufficient_fund | network_error | system_failure |
                  system_timeout | invalid_account_number |
                  daily_transaction_limit_exceeded | compliance_restriction |
                  suspected_fraud | issuer_unavailable
  Agent Action  : Route to TSU. Priority = High if amount > ₦100,000.

STATUS: reversed
  Definition   : Transaction was initially processed but subsequently
                 reversed. Funds returned to originating account.
  failure_reason: varies (often "network_error" or "system_failure")
  Agent Action  : Route to TSU if customer disputes the reversal.

STATUS: pending
  Definition   : Transaction submitted but not yet processed. Awaiting
                 network confirmation or authorization step.
  Agent Action  : Monitor. Escalate to TSU if pending > 30 minutes.

STATUS: timeout
  Definition   : Transaction was not completed within the network timeout
                 window. System could not confirm success or failure.
  failure_reason: "system_timeout"
  Agent Action  : Route to TSU. Priority = High (may or may not have debited).

STATUS: queued
  Definition   : Transaction received and queued for batch processing.
                 Common for scheduled payments and bulk transfers.
  Agent Action  : No immediate action. Notify customer of expected processing
                  time.

STATUS: processing
  Definition   : Transaction is actively being processed by the payment
                 switch. Intermediate state before "successful" or "failed."
  Agent Action  : No action. Monitor for final status update.


=========================================================================
SECTION 2: KYC TIERING & TRANSACTION LIMITS
=========================================================================

TIER 1 — BASIC KYC
  Account Types     : savings (newly onboarded, limited KYC)
  Requirements      : Name + Phone number only
  Daily Tx Limit    : ₦50,000
  Max Balance       : ₦300,000
  Monthly Limit     : ₦200,000
  Channels Allowed  : ussd, branch, nibss_transfer (low value only)
  Restrictions      : NO international transactions
                      NO card issuance
                      NO loans
  failure_reason if exceeded: "daily_transaction_limit_exceeded"
                               OR "compliance_restriction"

TIER 2 — INTERMEDIATE KYC
  Account Types     : savings + solo (for ages 16–30)
  Requirements      : Tier 1 + BVN + NIN
  Daily Tx Limit    : ₦200,000
  Max Balance       : ₦500,000
  Monthly Limit     : ₦2,000,000
  Channels Allowed  : mobile_app, ussd, atm, pos, web, branch,
                      nibss_transfer
  Restrictions      : NO international transfers
                      Investment products restricted
                      Loans capped at ₦500,000

TIER 3 — FULL KYC
  Account Types     : current (full banking, business-grade)
  Requirements      : Tier 2 + Address verification + Valid photo ID
                      + Utility bill (< 3 months old)
  Daily Tx Limit    : ₦5,000,000
  Max Balance       : Unlimited
  Monthly Limit     : Unlimited
  Channels Allowed  : ALL channels including international SWIFT
  Benefits          : Full product suite, loans up to ₦50,000,000,
                      investment products, private banking (if balance
                      consistently > ₦10,000,000)


=========================================================================
SECTION 3: STATUTORY LEVIES (MANDATORY — CANNOT BE WAIVED)
=========================================================================

Electronic Money Transfer Levy (EMTL)
  Rate         : Flat ₦50.00
  Applied to   : Inbound (CREDIT) transactions ≥ ₦10,000
  Direction    : Deducted from recipient (you receive ₦X, ₦50 deducted)
  Legal Basis  : Stamp Duties Act 2020

National Cybersecurity Levy
  Rate         : 0.5% of transaction value
  Applied to   : Outbound (DEBIT) electronic transfers
  Example      : Send ₦100,000 → Cybersecurity levy = ₦500
  Legal Basis  : Cybercrimes Act 2024 (Section 44)
  Exemptions   : Internal transfers (same bank), salary payroll,
                 loan disbursements

Value Added Tax (VAT)
  Rate         : 7.5% applied to BANK FEES only (not principal)
  Example      : NIP fee = ₦50 → VAT = ₦3.75
  Note         : Principal amount is NOT subject to VAT


=========================================================================
SECTION 4: CHANNEL-SPECIFIC TRANSACTION RULES
=========================================================================

CHANNEL: mobile_app
  Daily Limit       : Up to Tier limit
  OTP Required      : For amounts > ₦50,000 (Tier 2 and above)
  Push Challenge    : For amounts > ₦200,000 OR fraud flag present
  Device Binding    : Transactions from unrecognized device → mobile_channel_risk flag

CHANNEL: ussd
  Daily Limit       : ₦50,000 (Tier 1 cap applies across all USSD sessions)
  Fee               : ₦6.98 per USSD session
  OTP               : Not applicable (PIN-based authentication only)

CHANNEL: atm
  Daily Withdrawal Limit  : ₦100,000 (standard) | ₦200,000 (premium)
  Per-Transaction Max     : ₦40,000 per withdrawal (most ATMs)
  Fee (own bank ATMs)     : First 3 free per month, then ₦65 each
  Fee (other bank ATMs)   : ₦65 per transaction (no free tier)
  Dispense Error Protocol : If ATM debits but does not dispense cash:
                            auto-reversal within 24 hours after reconciliation
  failure_reason if no funds: "insufficient_fund"
  failure_reason if limit exceeded: "daily_transaction_limit_exceeded"

CHANNEL: pos
  Daily Limit          : ₦500,000 (all cards on account combined)
  Per-Transaction Max  : ₦200,000
  PIN Required         : Always
  OTP Required         : For amounts > ₦100,000
  Contactless Limit    : ₦15,000 per tap; PIN required for ≥ ₦5,000
  Contactless PIN Rule : Required after 5 consecutive contactless taps
  Duplicate Detection  : Same terminal + similar amount (±5%) + within
                         5 minutes → auto-reverse second charge

CHANNEL: web
  Daily Limit          : ₦500,000
  3D Secure            : Mandatory (Visa/Mastercard SecureCode)
  Push Challenge       : Required for amounts > ₦100,000
  Blocked Categories   : Adult content, cryptocurrency (requires approval),
                         gambling (opt-in required)

CHANNEL: branch
  Daily Limit          : Per Tier limit (counter withdrawals)
  ID Required          : Valid government-issued photo ID
  Large Cash           : Amounts > ₦5,000,000 require 24-hour pre-notice

CHANNEL: nibss_transfer
  Processing Time      : 2–5 minutes (normal), up to 2 hours (peak periods)
  Peak Periods         : Month-end (25th–30th), Friday evenings
  Maintenance Windows  : 2:00 AM – 6:00 AM WAT (announced in advance)
  failure_reason codes : network_error, system_timeout, issuer_unavailable,
                         invalid_account_number


=========================================================================
SECTION 5: REVERSAL POLICIES
=========================================================================

AUTOMATIC REVERSALS (No customer action required):

  Internal Transfer Failure
    Timeline    : Within 24 hours (usually within 2 hours)
    Process     : Automatic system reversal
    Notification: SMS + App push → "₦X credited back to your account."

  NIP/Interbank Transfer Failure
    Timeline    : 48–72 hours (NIBSS coordination required)
    Process     : TSU initiates NIBSS trace and recall
    Status Flow : failed/timeout → TSU investigation → reversal or credit

  ATM Dispense Error
    Timeline    : Within 24 hours after terminal cash reconciliation
    Process     : Terminal count vs. transaction log comparison
    Notification: SMS → "ATM reversal: ₦X credited to your account."

MANUAL REVERSAL REQUESTS (Customer must initiate via TSU):

  Eligible: Wrong account, duplicate transaction, amount error
  Process : TSU investigates → contacts beneficiary bank → requests recall
  Timeline: 2–14 days depending on beneficiary response

REVERSAL NOT POSSIBLE — CUSTOMER BEARS LOSS:
  1. Beneficiary bank confirms credit AND withdrawal (funds withdrawn)
  2. Value consummated (airtime loaded, bills paid, subscriptions activated)
  3. Funds transferred onward by beneficiary (money not in original account)
  4. Request made > 60 days after transaction (chargeback window expired)


=========================================================================
SECTION 6: FAILURE REASON CODES — AGENT INTERPRETATION GUIDE
=========================================================================

failure_reason         → Meaning & Recommended Agent Action
─────────────────────────────────────────────────────────────────────────
none                   → No failure. Transaction successful.
insufficient_fund      → Customer balance insufficient. Advise customer
                         to top up and retry. No reversal needed.
network_error          → NIBSS or switch connectivity issue. Auto-reversal
                         expected. Route to TSU if not reversed in 2 hours.
system_failure         → Core banking or middleware error. Escalate to TSU
                         immediately. Check if account was debited.
system_timeout         → Transaction timed out in switch. Outcome unknown.
                         TREAT AS FAILED until confirmed. Route to TSU.
invalid_account_number → Beneficiary account number does not exist or is
                         inactive. No debit applied. Advise customer to
                         verify account number and retry.
daily_tx_limit_exceeded→ Customer has hit their KYC tier daily limit.
                         Advise on limit and how to upgrade KYC tier.
                         Route to AOD if customer wants to upgrade.
compliance_restriction → Transaction blocked by AML/regulatory rule.
                         Do not disclose specific rule to customer.
                         Route to AOD or FRM depending on context.
suspected_fraud        → Transaction blocked by fraud engine.
                         is_fraud_score may be 1. Route to FRM always.
issuer_unavailable     → Destination bank system unreachable.
                         Auto-reversal expected within 24 hours.
                         Route to TSU if not resolved in 24 hours.


=========================================================================
SECTION 7: SERVICE CHARGE SCHEDULE
=========================================================================

Interbank NIP Transfers (nibss_transfer channel):
  Amount ≤ ₦5,000           : ₦10.00
  Amount ₦5,001 – ₦50,000   : ₦25.00
  Amount > ₦50,000           : ₦50.00
  First 3 per month (savings): FREE

Internal (Intra-Sentinel) Transfers:
  Between own accounts       : FREE (unlimited)
  To other Sentinel customers: FREE (unlimited)

SMS Transaction Alerts       : ₦4.00 per alert (mandatory per CBN)
USSD Banking Sessions        : ₦6.98 per session
ATM (own bank, 4th onwards)  : ₦65.00 per withdrawal
ATM (other banks)            : ₦65.00 per withdrawal
Account Maintenance (savings): ₦50/month (waived if balance ≥ ₦100,000)
Account Maintenance (current): ₦300/month + ₦1/₦1,000 COT (business)
Card Replacement              : ₦1,500
Statement (> 90 days, email) : ₦50 per month
Account Reactivation (dormant): ₦500


=========================================================================
DOCUMENT CONTROL
=========================================================================

Policy Owner   : Head of Transaction Services
Approver       : Chief Operations Officer (COO)
Last Updated   : {self.display_date}
Contact        : transactionservices@sentinelbank.ng | Ext. 4000


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

    # =========================================================================
    # DOCUMENT 4: CUSTOMER SERVICE FAQ (FAQ-001)
    # =========================================================================

    def generate_faq_document(self) -> Dict:
        """
        Generate customer-facing FAQ document.
        Aligned to exact channel names, account types, limits, and
        fee schedule from the dataset generator and TSU-POL-002.
        """
        faq = f"""
=========================================================================
{self.bank_name} - CUSTOMER SERVICE FAQ
=========================================================================

Document ID     : FAQ-001
Version         : 2.0
Last Updated    : {self.display_date}
Classification  : Public — Customer-Facing & Agent Reference


=========================================================================
SECTION 1: TRANSFER & PAYMENT ISSUES
=========================================================================

Q1: My transfer was debited but the receiver did not get the money.
─────────────────────────────────────────────────────────────────────────
A: This usually indicates a temporary NIBSS network delay or a failed
   interbank transaction (transaction_status = "failed" or "timeout").

   Step 1: Wait 2 hours — most delays self-resolve via auto-reversal.
   Step 2: Check your transaction history in the app for the reference number
           (format: TXN + 12 digits).
   Step 3: Confirm the beneficiary account number and bank with the recipient.
   Step 4: If still unresolved after 2 hours, contact us with your
           transaction reference. We will trace it via NIBSS.
   Step 5: If not received within 24 hours, automatic reversal is triggered
           and you will receive an SMS confirmation.

   Common failure reasons to quote when reporting:
     - network_error → NIBSS unreachable during transfer
     - system_timeout → Switch did not respond in time
     - issuer_unavailable → Destination bank system was offline

Q2: I sent money to the wrong account. Can it be reversed?
─────────────────────────────────────────────────────────────────────────
A: Reversal is NOT automatic for wrong-account transfers. Here is the process:

   Step 1: Contact the recipient directly and request voluntary refund.
   Step 2: If unsuccessful, contact our Transaction Services Unit (TSU)
           with evidence: screenshot of intended vs. actual account number.
   Step 3: TSU will formally request recall from the beneficiary's bank.
   Step 4: Recovery takes 2–14 days depending on beneficiary cooperation.

   REVERSAL NOT POSSIBLE if: recipient has already withdrawn the funds.
   Use the "Name Enquiry" feature before every transfer to verify the
   account name matches your intended recipient.

Q3: What are my daily transfer limits?
─────────────────────────────────────────────────────────────────────────
A: Limits depend on your KYC tier and account type:
     Tier 1 (basic savings, phone + name only)   : ₦50,000/day
     Tier 2 (savings/solo, BVN + NIN linked)     : ₦200,000/day
     Tier 3 (current account, full KYC)           : ₦5,000,000/day

   If exceeded: failure_reason = "daily_transaction_limit_exceeded"
   To upgrade: Visit any branch with valid photo ID + utility bill.


=========================================================================
SECTION 2: CARD ISSUES
=========================================================================

Q4: Why was my card declined even though I have sufficient balance?
─────────────────────────────────────────────────────────────────────────
A: Common reasons for card decline at POS or ATM:

   1. INTERNATIONAL TRANSACTIONS DISABLED (default state)
      Enable in app: Cards → Manage Card → International Transactions ON.
      Set travel start and end dates.

   2. DAILY LIMIT EXCEEDED
      POS limit : ₦500,000/day (all cards combined)
      ATM limit : ₦100,000/day (standard) or ₦200,000 (premium)
      Resets at midnight.

   3. INCORRECT PIN (3 wrong attempts = auto-lock for 24 hours)
      Reset via app or call Customer Care.

   4. MERCHANT CATEGORY BLOCKED
      Betting, gambling, crypto exchanges are blocked by default.
      Contact us to enable specific categories.

   5. SECURITY HOLD (suspicious activity detected)
      Fraud engine applied risk block. Contact us to verify identity
      and unblock. failure_reason = "suspected_fraud" in system logs.

Q5: The ATM did not dispense cash but my account was debited.
─────────────────────────────────────────────────────────────────────────
A: This is a dispense error on channel = "atm". Standard protocol:

   IMMEDIATE: Note ATM location, exact time, amount, and any receipt.
              Do NOT retry the same ATM.
   REPORT: Contact us within 24 hours with ATM details.
   RESOLUTION: Auto-reversal within 24–48 hours after terminal
               reconciliation confirms the dispense error.
   SUCCESS RATE: 99.8% of ATM dispense errors are successfully reversed.


=========================================================================
SECTION 3: MOBILE APP & DIGITAL BANKING
=========================================================================

Q6: I cannot log into the mobile app.
─────────────────────────────────────────────────────────────────────────
A: Troubleshoot in this order:

   WRONG PASSWORD        → Use "Forgot Password" (reset link to email).
   OUTDATED APP VERSION  → Update via Play Store / App Store.
   NETWORK ISSUE         → Toggle airplane mode off/on. Switch between
                           WiFi and mobile data. Clear app cache.
   ACCOUNT LOCKED        → 3 wrong password attempts locks for 30 minutes.
                           Call Customer Care for immediate unlock.
   NEW DEVICE            → OTP sent to registered phone to verify new device.

   If still unable to log in after all steps → Route complaint to DCS.

Q7: I am not receiving OTPs.
─────────────────────────────────────────────────────────────────────────
A: Check in this order:

   1. Verify registered phone number is correct (call Customer Care).
   2. Check network signal and mobile data.
   3. Check SMS spam/blocked messages folder. Sender: "SENTINEL" or "SBNK".
   4. Restart phone.
   5. For iPhone: Settings → Messages → Filter Unknown Senders → disable.
   6. Request email OTP as alternative (if available for your transaction).
   7. Visit branch with valid ID to update registered phone number if incorrect.


=========================================================================
SECTION 4: FRAUD & SECURITY
=========================================================================

Q8: Someone is claiming to be from Sentinel Bank and asking for my PIN.
─────────────────────────────────────────────────────────────────────────
A: THIS IS A SCAM. Sentinel Bank will NEVER ask for your PIN, password,
   OTP, or any authentication credential via phone, email, or SMS.

   WHAT TO DO IF YOU SHARED YOUR CREDENTIALS:
   Step 1: Change password immediately in app: Settings → Change Password.
   Step 2: Block your card: Cards → Block Card.
   Step 3: Call our OFFICIAL number: 0700-SENTINEL.
   Step 4: Email: fraud-desk@sentinelbank.ng with scammer details.
   Step 5: Monitor transactions hourly for next 24 hours.

   THINGS WE WILL NEVER ASK FOR: PIN, password, OTP, "safe account" transfer,
   remote access app installation (TeamViewer, AnyDesk).


=========================================================================
SECTION 5: ACCOUNT SERVICES
=========================================================================

Q9: How do I request a bank statement?
─────────────────────────────────────────────────────────────────────────
A: Options by channel:
   Mobile App (FREE for last 90 days) : Statements → Select date range →
                                        Download PDF or Excel.
   Email request                       : statements@sentinelbank.ng.
                                         Include: name, account number,
                                         date range, delivery email.
                                         Fee: ₦50/month requested.
                                         Response: Within 24 hours.
   USSD (*737*7#)                      : Last 5 transactions only. Free.
   Branch visit                        : Any period including historical.
                                         Fee: ₦100/month. Same-day ready.

Q10: Why am I being charged monthly fees?
─────────────────────────────────────────────────────────────────────────
A: Standard charges that may appear on your account:

   Account maintenance (savings)     : ₦50/month (waived if balance ≥ ₦100,000)
   Account maintenance (current)     : ₦300/month
   SMS transaction alerts            : ₦4 per alert (mandatory, CBN requirement)
   USSD sessions (*737#)             : ₦6.98 per session
   ATM withdrawals (other banks)     : ₦65 per withdrawal
   ATM withdrawals (own bank, >3/mo) : ₦65 per withdrawal

   If you see a charge not listed above, report it to Account Operations
   Department (AOD) immediately with your statement reference.


=========================================================================
SECTION 6: CONTACT INFORMATION
=========================================================================

24/7 Customer Care     : 0700-SENTINEL (0700-736-8463)
Fraud Emergency        : fraud-desk@sentinelbank.ng (24/7 monitoring)
Complaints             : complaints@sentinelbank.ng
Statement Requests     : statements@sentinelbank.ng
Branch Locator         : In-app → Find Branch
Banking Hours          : Mon–Fri 8:00 AM – 4:00 PM
                         Saturday (selected branches): 9:00 AM – 1:00 PM


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

    # =========================================================================
    # DOCUMENT 5: MERCHANT RISK PROFILES (FRM-002)  ← NEW
    # =========================================================================

    def generate_merchant_risk_profiles(self) -> Dict:
        """
        Generate merchant-category risk profiles for the Sentinel Agent.

        Dataset alignment (EXACT merchant categories from MERCHANTS dict):
          "supermarket"  : Shoprite, SPAR, Justrite, Ebeano, Market Square
          "restaurants"  : Chicken Republic, Kilimanjaro, Mr Biggs, Dominos,
                           Cold Stone, Bukka Hut
          "fuel"         : NNPC Mega Station, TotalEnergies, Mobil, Oando,
                           Conoil, MRS
          "transport"    : Uber, Bolt, LagRide, ABC Transport, Peace Mass Transit
          "telecoms"     : MTN, Airtel, Glo, 9mobile
          "utilities"    : Ikeja Electric, EKEDC, IBEDC, AEDC, DSTV, GOtv,
                           StarTimes
          "fintech"      : Paystack, Flutterwave, Interswitch, Remita, Monnify
          "education"    : University Tuition, WAEC, JAMB, Private School Fees
          "healthcare"   : Teaching Hospital, Private Hospital, Medplus, HealthPlus

        Risk weight values integrate directly into calculate_fraud_risk()
        method in rag_query.py.
        """
        profiles = f"""
=========================================================================
{self.bank_name} - MERCHANT RISK PROFILES
=========================================================================

Document Code       : FRM-002
Classification      : CONFIDENTIAL — Sentinel Agent Operational Reference
Version             : 1.0
Effective Date      : January 2026
Review Frequency    : Monthly

Document Owner      : Chief Risk Officer (CRO)
Companion Document  : FRM-001 (Fraud Detection & Prevention Guidelines)

USAGE BY SENTINEL AGENT:
  This document provides merchant_category risk weights that are ADDED
  to the base risk score calculated from fraud_explainability_trace flags
  in FRM-001. The Sentinel Agent queries this document using the
  merchant_category field from transactions.csv and adds the returned
  risk weight to the total risk score.

QUERY PATTERN:
  Input  : transaction['merchant_category']  →  e.g., "fintech"
  Output : merchant risk weight (points)     →  e.g., +25
  Action : Add to base score from FRM-001 flags, cap total at 100.


=========================================================================
SECTION 1: MERCHANT CATEGORY RISK TIERS
=========================================================================

──────────────────────────────────────────────────────────────────────────
TIER 1 — HIGH RISK MERCHANT CATEGORIES  (+25 points)
──────────────────────────────────────────────────────────────────────────

CATEGORY: fintech
Risk Weight       : +25 points
Known Merchants   : Paystack, Flutterwave, Interswitch, Remita, Monnify
Risk Rationale    :
  Fintech payment gateways are the PRIMARY channel for moving stolen funds
  out of compromised accounts. Fraudsters use victim accounts to fund
  merchant accounts or P2P wallets on fintech platforms, making tracing
  difficult. High-value fintech transactions from accounts with no prior
  fintech history are the strongest single fraud signal in this dataset.
Heightened Alert Conditions (add additional +10 points):
  - First-ever fintech transaction for this account
  - Amount > ₦200,000 to a fintech gateway
  - Transaction at hours between midnight and 5:00 AM WAT
  - Customer segment = "elderly" (age > 60 in customers.csv)
Sentinel Agent Rule:
  IF merchant_category = "fintech" AND is_fraud_score = 1:
    Add +25 points to risk score.
  IF merchant_category = "fintech" AND amount > 200000 AND
     no prior fintech transactions:
    Treat as if multiple_failures flag present (+20 additional).
Recommended Escalation:
  Combined score ≥ 61 (fintech + any one FRM-001 flag) → HIGH risk.
  Mandatory push-to-app challenge for ALL fintech transactions > ₦100,000
  regardless of fraud score, as a blanket protective control.

──────────────────────────────────────────────────────────────────────────
TIER 2 — MEDIUM-HIGH RISK MERCHANT CATEGORIES  (+15 points)
──────────────────────────────────────────────────────────────────────────

CATEGORY: transport
Risk Weight       : +15 points
Known Merchants   : Uber, Bolt, LagRide, ABC Transport, Peace Mass Transit
Risk Rationale    :
  Transport platforms (especially ride-hailing apps) are used as
  money-testing venues. Fraudsters initiate small Uber or Bolt transactions
  to test whether a stolen card or account access is active before
  attempting larger withdrawals. Multiple transport transactions within
  a short window is a card-testing indicator.
Heightened Alert Conditions (add additional +10 points):
  - 3+ transport transactions within 30 minutes to same platform
  - Transport transaction outside customer's residential_state
    (customers.csv: residential_state field)
  - Transport transaction at 12:00 AM – 5:00 AM WAT
Positive Contextual Signal:
  - Uber/Bolt transactions ≥ 6 in last 90 days on this account
    → REDUCES suspicion (regular commuter behavior)
  - This is the car_loan_signal_score trigger in transactions.csv
    (uber_tracker ≥ 6 adds +0.4 to car_loan_signal_score)
Sentinel Agent Rule:
  IF merchant_category = "transport" AND is_fraud_score = 1:
    Add +15 points.
  IF uber_tracker < 6 AND 3+ transport txns in 30 minutes:
    Treat as card-testing pattern. Add +15 points.

CATEGORY: education
Risk Weight       : +15 points
Known Merchants   : University Tuition, WAEC, JAMB, Private School Fees
Risk Rationale    :
  Education payments are legitimate but frequently used in social
  engineering scams. Fraudsters convince victims to make urgent "school
  fee" payments under false pretenses. Large, round-number education
  payments from accounts with no prior education payment history are
  suspicious.
Heightened Alert Conditions (add additional +10 points):
  - Payment > ₦500,000 (unusually large for typical fees)
  - No prior education transactions on this account (ever)
  - Payment destination is international (outside Nigeria)
Sentinel Agent Rule:
  IF merchant_category = "education" AND is_fraud_score = 1:
    Add +15 points.

CATEGORY: healthcare
Risk Weight       : +15 points
Known Merchants   : Teaching Hospital, Private Hospital, Medplus, HealthPlus
Risk Rationale    :
  Healthcare payments are generally legitimate but are occasionally
  used as cover for scam payments ("my mother needs surgery money").
  Large pharmacy or hospital payments from accounts with unusual
  activity patterns require secondary review.
Heightened Alert Conditions (add additional +10 points):
  - Payment > ₦1,000,000 (significantly above average healthcare spend)
  - First-ever healthcare merchant on this account
Sentinel Agent Rule:
  IF merchant_category = "healthcare" AND is_fraud_score = 1:
    Add +15 points.

──────────────────────────────────────────────────────────────────────────
TIER 3 — LOW RISK MERCHANT CATEGORIES  (+5 points)
──────────────────────────────────────────────────────────────────────────

CATEGORY: telecoms
Risk Weight       : +5 points
Known Merchants   : MTN, Airtel, Glo, 9mobile
Risk Rationale    :
  Telecom payments (airtime, data bundles) are generally low-risk but
  can indicate money-laundering via airtime resale at very high volumes.
  Standard individual top-ups are benign.
Heightened Alert Conditions (add additional +10 points):
  - Airtime purchase > ₦50,000 in single transaction (resale indicator)
  - 5+ telecom transactions in one day (bulk resale pattern)
Sentinel Agent Rule:
  IF merchant_category = "telecoms" AND is_fraud_score = 1:
    Add +5 points.

──────────────────────────────────────────────────────────────────────────
TIER 4 — ZERO-RISK MERCHANT CATEGORIES  (0 points)
──────────────────────────────────────────────────────────────────────────
These categories are established, everyday spending patterns. No additional
risk weight is added to the fraud score for transactions in these categories.

CATEGORY: supermarket
Risk Weight       : 0 points
Known Merchants   : Shoprite, SPAR, Justrite, Ebeano, Market Square
Rationale         : Essential grocery spending. Highly predictable pattern.
                    Shoprite and SPAR have robust transaction security.
Exception         : IF amount > ₦500,000 at a supermarket → flag as anomaly
                    (+15 points). Supermarkets rarely process amounts this high.

CATEGORY: restaurants
Risk Weight       : 0 points
Known Merchants   : Chicken Republic, Kilimanjaro, Mr Biggs, Dominos,
                    Cold Stone, Bukka Hut
Rationale         : Everyday food spending. Very low fraud incidence.
Exception         : IF amount > ₦200,000 at a single restaurant
                    → flag as anomaly (+15 points).

CATEGORY: fuel
Risk Weight       : 0 points
Known Merchants   : NNPC Mega Station, TotalEnergies, Mobil, Oando,
                    Conoil, MRS
Rationale         : Routine fuel purchases. Extremely predictable.
                    Amount per transaction is naturally bounded by tank size.
Exception         : IF amount > ₦100,000 at fuel station
                    → flag as anomaly (+10 points).

CATEGORY: utilities
Risk Weight       : 0 points
Known Merchants   : Ikeja Electric, EKEDC, IBEDC, AEDC, DSTV, GOtv, StarTimes
Rationale         : Utility bill payments are scheduled, predictable, and
                    directly verifiable. DSTV, GOtv, and StarTimes are
                    subscription services with fixed recurring amounts.
Exception         : IF amount > ₦500,000 for a single utility bill
                    → flag as anomaly (+10 points).


=========================================================================
SECTION 2: MERCHANT VELOCITY RULES (ADDITIONAL RISK WEIGHTS)
=========================================================================

These rules apply ON TOP OF the category risk weights above.
They are based on transaction frequency patterns, not merchant type.

First Transaction with New Merchant (on this account)
  Condition     : No prior transaction with this merchant_name on record
  Risk Weight   : +10 points
  Rationale     : New merchant relationships carry inherent uncertainty.
  Rule          : Apply to ALL categories including Tier 4 (zero-risk).

Repeated Transactions to Same Merchant in Short Window
  Condition     : 3+ transactions to same merchant within 60 minutes
  Risk Weight   : +20 points
  Rationale     : Velocity to same merchant indicates card testing,
                  session hijacking, or retry fraud pattern.

Large Single Transaction with New Merchant
  Condition     : Amount > ₦100,000 AND merchant is new to this account
  Risk Weight   : +25 points (replaces +10 new merchant weight — use higher)
  Rationale     : High-value first transaction with unknown merchant is
                  the highest-risk combination outside fintech.


=========================================================================
SECTION 3: COMPOSITE RISK SCORE EXAMPLES
=========================================================================

EXAMPLE 1 — FINTECH HIGH RISK:
  Flags from FRM-001:
    mobile_channel_risk  : +15
    high_amount_spike    : +25
  Merchant (FRM-002):
    fintech              : +25
  Velocity (Section 2):
    First fintech txn    : +10 (new merchant)
  ─────────────────────
  Total                  : 75 → HIGH → Mandatory push-to-app challenge

EXAMPLE 2 — TRANSPORT CARD TESTING:
  Flags from FRM-001:
    multiple_failures    : +20
  Merchant (FRM-002):
    transport            : +15
  Velocity (Section 2):
    3 txns in 30 min     : +20
  ─────────────────────
  Total                  : 55 → MEDIUM → Step-up OTP authentication

EXAMPLE 3 — SUPERMARKET NORMAL PURCHASE:
  Flags from FRM-001:
    normal_pattern       : +0
  Merchant (FRM-002):
    supermarket          : +0
  Velocity (Section 2):
    Regular merchant     : +0
  ─────────────────────
  Total                  : 0 → LOW → Process without friction

EXAMPLE 4 — FINTECH CRITICAL BLOCK:
  Flags from FRM-001:
    mobile_channel_risk  : +15
    high_amount_spike    : +25
    multiple_failures    : +20
  Merchant (FRM-002):
    fintech              : +25
  ─────────────────────
  Total                  : 85 → HIGH (capped at 85 — borderline Critical)
  Note: Add +10 for new merchant → 95 → CRITICAL → Block immediately


=========================================================================
SECTION 4: MERCHANT CATEGORY TO DEPARTMENT ROUTING
=========================================================================

If a merchant-related complaint is raised, use this table to determine
which department should handle it alongside the Sentinel fraud assessment:

  Merchant Category  → Primary Department  → Escalate to if Fraud Detected
  ─────────────────────────────────────────────────────────────────────────
  fintech            → TSU (disputed txn)  → FRM (if is_fraud_score = 1)
  transport          → COC (if card used)  → FRM (if is_fraud_score = 1)
  supermarket        → COC (if POS issue)  → TSU (if transfer issue)
  restaurants        → COC (if POS issue)  → TSU (if transfer issue)
  fuel               → COC (if POS issue)  → TSU (if transfer issue)
  utilities          → TSU (bill payment)  → AOD (if charge disputed)
  telecoms           → TSU (airtime/data)  → DCS (if USSD channel)
  education          → TSU (bank transfer) → FRM (if scam suspected)
  healthcare         → TSU (bank transfer) → FRM (if scam suspected)


=========================================================================
DOCUMENT CONTROL
=========================================================================

Owner           : Chief Risk Officer (CRO)
Review Freq     : Monthly (aligned with FRM-001 review cycle)
Last Updated    : {self.display_date}
Companion       : FRM-001 (Fraud Detection & Prevention Guidelines)
Emergency       : fraud-desk@sentinelbank.ng | +234-1-FRAUD-24 (24/7)


=========================================================================
END OF DOCUMENT FRM-002
=========================================================================
"""
        return self._package_for_rag(
            "FRM-002",
            "Merchant Risk Profiles",
            "security",
            "1.0",
            profiles
        )

    # =========================================================================
    # DOCUMENT 6: PRODUCT RECOMMENDATION POLICY (PRS-001)  ← NEW
    # =========================================================================

    def generate_product_recommendation_policy(self) -> Dict:
        """
        Generate product recommendation eligibility policy for Trajectory Agent.

        Dataset alignment (EXACT field names and thresholds from data_generator.py):
          recommended_product values: "Car Loan", "Personal Loan",
                                      "Investment Plan", None
          Eligibility logic mirrors the data_generator EXACTLY:

          Car Loan:
            car_loan_signal_score >= 0.7
            which requires ANY combination of:
              uber_tracker >= 6    → +0.4 to signal score
              salary_detected=True → +0.3 to signal score
              monthly_inflow > 500000 → +0.3 to signal score

          Personal Loan:
            salary_detected = True
            AND monthly_inflow > 300000
            AND car_loan_signal_score < 0.7 (not eligible for Car Loan)

          Investment Plan:
            monthly_inflow > 2,000,000
            (highest threshold, regardless of salary or Uber)

          salary_detected:
            salary_tracker[customer_id] >= 2
            (2+ credits > ₦200,000 from fintech merchants = salary signal)

          monthly_inflow_tracker:
            cumulative credit transactions for this customer_id

          uber_tracker:
            merchant_name in ["Uber", "Bolt", "LagRide"] → counter + 1
        """
        policy = f"""
=========================================================================
{self.bank_name} - PRODUCT RECOMMENDATION POLICY
=========================================================================

Document Code       : PRS-001
Classification      : Internal — Trajectory Agent Operational Reference
Version             : 1.0
Effective Date      : January 2026
Review Frequency    : Quarterly

Document Owner      : Head of Retail Banking Products
Companion Documents : TSU-POL-002 (Transaction Policies),
                      POL-CCH-001 (Complaint Handling Policy)

USAGE BY TRAJECTORY AGENT:
  This document defines the EXACT eligibility criteria for each product
  recommendation in the Sentinel Bank dataset. The Trajectory Agent uses
  this policy to:
    1. Validate whether a recommended_product field in transactions.csv
       is correctly assigned.
    2. Generate product offers for customers who do not yet have a
       recommendation but meet eligibility criteria.
    3. Produce explainable, policy-grounded reasons for every recommendation.

FIELD REFERENCE FROM TRANSACTIONS.CSV:
  recommended_product     : "Car Loan" | "Personal Loan" | "Investment Plan" | None
  salary_detected         : True | False
  car_loan_signal_score   : float, range 0.0 – 1.0
  amount                  : float (individual transaction amount in NGN)
  merchant_name           : string (e.g., "Uber", "Bolt", "Paystack")
  merchant_category       : string (e.g., "transport", "fintech")

FIELDS FROM CUSTOMERS.CSV AND ACCOUNTS.CSV (JOINED BY AGENT):
  age                     : integer (18–70 in dataset)
  account_type            : "savings" | "solo" | "current"
  monthly_inflow          : computed from monthly_inflow_tracker[customer_id]
                            = sum of all credit transaction amounts per customer


=========================================================================
SECTION 1: PRODUCT ELIGIBILITY CRITERIA
=========================================================================

──────────────────────────────────────────────────────────────────────────
PRODUCT: Car Loan
──────────────────────────────────────────────────────────────────────────
Recommended When  : recommended_product = "Car Loan" in dataset
Policy Trigger    : car_loan_signal_score >= 0.7

SIGNAL SCORE COMPOSITION (EXACT FORMULA FROM DATA GENERATOR):
  Component 1 — Transport Usage Signal:
    Condition  : uber_tracker[customer_id] >= 6
                 (6 or more transactions where merchant_name is
                  "Uber", "Bolt", or "LagRide" in last 90 days)
    Score Add  : +0.4 to car_loan_signal_score
    Rationale  : Frequent ride-hailing usage signals lack of personal
                 vehicle, strong latent demand, and ability to budget
                 for regular transport costs (which would convert to
                 loan repayments).

  Component 2 — Salary Detection Signal:
    Condition  : salary_detected = True
                 (salary_tracker[customer_id] >= 2, meaning at least
                  2 credit transactions > ₦200,000 from merchant_category
                  = "fintech" exist on this account)
    Score Add  : +0.3 to car_loan_signal_score
    Rationale  : Regular salary income indicates stable repayment
                 capacity. Two or more large fintech credits is the
                 proxy for salary/payroll in this dataset.

  Component 3 — Monthly Inflow Signal:
    Condition  : monthly_inflow_tracker[customer_id] > 500,000
                 (cumulative credit amount across all transactions
                  for this customer exceeds ₦500,000)
    Score Add  : +0.3 to car_loan_signal_score
    Rationale  : High monthly cash inflow indicates financial capacity
                 to service a car loan without cash flow distress.

MINIMUM SIGNAL SCORE THRESHOLD:
  car_loan_signal_score >= 0.7 → ELIGIBLE for Car Loan recommendation
  car_loan_signal_score  < 0.7 → NOT ELIGIBLE (check other products)

EXAMPLE COMBINATIONS THAT REACH THRESHOLD:
  Uber ≥ 6 + Salary Detected        → 0.4 + 0.3 = 0.7 ✓ ELIGIBLE
  Uber ≥ 6 + Inflow > ₦500K         → 0.4 + 0.3 = 0.7 ✓ ELIGIBLE
  Salary Detected + Inflow > ₦500K  → 0.3 + 0.3 = 0.6 ✗ NOT ELIGIBLE
  All three signals present          → 0.4 + 0.3 + 0.3 = 1.0 ✓ STRONGLY ELIGIBLE

AGENT VALIDATION QUERY:
  "What is the eligibility criteria for Car Loan?"
  Expected response: Signal score >= 0.7, composed of Uber (0.4),
  salary detection (0.3), and/or monthly inflow > ₦500,000 (0.3).

ADDITIONAL POLICY REQUIREMENTS (For Human Credit Officers):
  - Account age        : Minimum 12 months with {self.bank_name}
  - Credit history     : No active loan defaults
  - KYC tier           : Tier 2 or Tier 3 minimum
  - Minimum age        : 21 years (from customers.csv age field)
  - Maximum age        : 60 years at loan maturity

──────────────────────────────────────────────────────────────────────────
PRODUCT: Personal Loan
──────────────────────────────────────────────────────────────────────────
Recommended When  : recommended_product = "Personal Loan" in dataset
Policy Trigger    : salary_detected = True
                    AND monthly_inflow_tracker[customer_id] > 300,000

DATASET LOGIC (EXACT FROM DATA GENERATOR):
  Personal Loan is the FALLBACK recommendation when:
    - salary_detected = True (salary_tracker >= 2, credits > ₦200,000 from fintech)
    - monthly_inflow > ₦300,000
    - BUT car_loan_signal_score < 0.7 (not eligible for Car Loan)
  In other words: the customer has stable income but insufficient
  transport/inflow signals to qualify for the higher-value Car Loan.

ELIGIBILITY CRITERIA:
  Primary Signals:
    salary_detected          = True (mandatory — no salary, no personal loan)
    monthly_inflow           > ₦300,000

  Disqualifying Conditions:
    car_loan_signal_score    >= 0.7 → recommend Car Loan instead
    monthly_inflow           > ₦2,000,000 → recommend Investment Plan instead

  Context: In the dataset, once monthly_inflow > ₦2,000,000, the
  Investment Plan trigger fires first. The Personal Loan applies in
  the ₦300,000 – ₦2,000,000 monthly inflow band.

ADDITIONAL POLICY REQUIREMENTS (For Human Credit Officers):
  - Account type       : Tier 2 or Tier 3 (savings with BVN + NIN minimum)
  - Debt-to-income     : DTI ratio < 40% (outstanding commitments vs income)
  - KYC tier           : Tier 2 minimum (BVN + NIN required)
  - Minimum age        : 21 years
  - Maximum loan       : ₦5,000,000 (routed to CLS for formal assessment)

AGENT VALIDATION QUERY:
  "What are the eligibility criteria for Personal Loan?"
  Expected: salary_detected=True AND monthly_inflow > ₦300,000 AND
  car_loan_signal_score < 0.7.

──────────────────────────────────────────────────────────────────────────
PRODUCT: Investment Plan
──────────────────────────────────────────────────────────────────────────
Recommended When  : recommended_product = "Investment Plan" in dataset
Policy Trigger    : monthly_inflow_tracker[customer_id] > 2,000,000

DATASET LOGIC (EXACT FROM DATA GENERATOR):
  Investment Plan fires when monthly_inflow > ₦2,000,000 regardless of
  salary_detected or car_loan_signal_score. It is the HIGHEST-TIER product
  in the recommendation hierarchy. This threshold captures customers
  with high net cash inflow who have the financial slack to invest
  rather than borrow.

ELIGIBILITY CRITERIA:
  Primary Signal:
    monthly_inflow           > ₦2,000,000 (cumulative credit amount)

  Supporting Signals (for prioritization, not blocking):
    Account type             : "current" (Tier 3) — highest eligibility
    Age range                : 25–60 preferred (wealth accumulation phase)
    Balance pattern          : Consistently high balance (> ₦5,000,000
                               average across last 90 days)
    Spending pattern         : Low outflow ratio vs. inflow (savings behavior)

  Note: In the dataset generator, Investment Plan can be recommended
  even alongside Car Loan or Personal Loan signals if monthly_inflow
  exceeds ₦2,000,000. The Trajectory Agent should prioritize presenting
  Investment Plan first to high-inflow customers.

AVAILABLE INVESTMENT PRODUCTS:
  - Fixed Deposit (30, 60, 90, 180, 365-day tenors)
  - Treasury Bill participation (via {self.bank_name} treasury desk)
  - Mutual Fund (money market, equity, balanced)
  - Dollar Fixed Deposit (domiciliary account required)

ADDITIONAL POLICY REQUIREMENTS (For Relationship Managers):
  - KYC tier           : Tier 3 required (full KYC, current account preferred)
  - Minimum investment : ₦500,000 (Fixed Deposit), ₦100,000 (Mutual Fund)
  - NDIC coverage      : Fixed deposits covered up to ₦5,000,000


=========================================================================
SECTION 2: RECOMMENDATION HIERARCHY & PRIORITY
=========================================================================

When a customer qualifies for multiple products simultaneously, use this
priority order for the Trajectory Agent's primary recommendation:

  PRIORITY 1: Investment Plan  (monthly_inflow > ₦2,000,000)
              → Customer has high liquidity; invest first, borrow later.

  PRIORITY 2: Car Loan         (car_loan_signal_score >= 0.7)
              → Strong income + transport signals. High conversion rate.

  PRIORITY 3: Personal Loan    (salary_detected + inflow > ₦300K)
              → Stable income but insufficient signals for Car Loan.

  PRIORITY 4: No Recommendation (recommended_product = None)
              → Customer does not yet meet any threshold.
              → Trajectory Agent should note which signals are closest
                to threshold and suggest account behaviors to qualify.

RECOMMENDATION REASONING TEMPLATE (for Trajectory Agent output):
  "Based on {self.bank_name} Product Recommendation Policy PRS-001,
   [Customer Name] is recommended for [Product] because:
   - salary_detected = [True/False] (salary_tracker threshold: 2 credits > ₦200,000)
   - monthly_inflow = ₦[X] (threshold: ₦[Y])
   - car_loan_signal_score = [X] (threshold: 0.7)
   - uber_tracker = [X] trips (threshold: 6 trips)
   This meets [met_criteria]. Unmet criteria: [unmet_criteria]."


=========================================================================
SECTION 3: SALARY DETECTION METHODOLOGY
=========================================================================

The salary_detected field is a derived boolean based on salary_tracker.
The Trajectory Agent must understand exactly how it is computed to
correctly validate or explain recommendations.

SALARY DETECTION RULE:
  salary_detected = True
  IF: COUNT of credit transactions WHERE:
        amount > ₦200,000
        AND merchant_category = "fintech"
        (Paystack, Flutterwave, Interswitch, Remita, Monnify)
      >= 2 transactions on this customer's accounts

REASONING:
  In Nigeria, most corporate salary payments are routed via payroll
  systems built on fintech infrastructure (Paystack Payroll, Remita).
  Two or more large credits (> ₦200,000) from fintech channels is a
  robust proxy for regular salaried employment.

COMMON SALARY MERCHANTS IN DATASET:
  Paystack      : Widely used by SMEs and startups for payroll
  Remita        : Primary platform for government salary payments (IPPIS)
  Flutterwave   : Used by mid-size and large corporations
  Interswitch   : Legacy enterprise payroll routing
  Monnify       : Growing startup and SME payroll platform

EDGE CASES FOR AGENT AWARENESS:
  - Freelancers may receive large fintech credits but irregularly.
    salary_detected may be True for freelancers with multiple clients.
  - Business owners may receive large fintech credits as business revenue.
    Trajectory Agent should cross-check account_type = "current" which
    suggests a business context → Investment Plan more appropriate.
  - Salary_detected = False does NOT mean no income.
    It means the salary pattern was not detected within the transaction
    sample. Other income forms (cash, branch deposits) are not captured.


=========================================================================
SECTION 4: MONTHLY INFLOW CALCULATION
=========================================================================

monthly_inflow_tracker[customer_id] is the CUMULATIVE sum of all CREDIT
transaction amounts across all accounts linked to this customer_id.

CALCULATION (from data_generator.py):
  FOR EACH transaction:
    IF transaction_type = "credit":
      monthly_inflow_tracker[customer_id] += transaction.amount

THRESHOLDS USED IN PRODUCT RULES:
  > ₦300,000  → Personal Loan minimum threshold
  > ₦500,000  → Car Loan inflow signal threshold (+0.3 to signal score)
  > ₦2,000,000 → Investment Plan threshold

AGENT GUIDANCE — INFLOW INTERPRETATION:
  ₦0 – ₦300,000        → Low inflow. No product recommendation triggered.
                          Suggest account upgrade and salary routing.
  ₦300,001 – ₦500,000  → Personal Loan eligible IF salary_detected = True.
                          Car Loan NOT yet eligible (inflow signal missing).
  ₦500,001 – ₦2,000,000 → Car Loan eligible IF uber_tracker >= 6 OR
                            salary_detected = True (reaching score 0.7).
                            Personal Loan also eligible.
  > ₦2,000,000          → Investment Plan recommended as primary product.
                           Car Loan and Personal Loan remain secondary offers.


=========================================================================
SECTION 5: CROSS-DATASET VALIDATION QUERIES
=========================================================================

The Trajectory Agent uses this section to validate recommendations
against the full customer profile across all four datasets.

VALIDATION QUERY 1 — Car Loan:
  Required Data : customers.csv (age), accounts.csv (account_type),
                  transactions.csv (car_loan_signal_score, salary_detected,
                  merchant_name, monthly_inflow)
  Pass Condition: age >= 21 AND account_type IN ['savings','current']
                  AND car_loan_signal_score >= 0.7
  Fail Reason   : List which of the three signal components are missing
                  and what behavior would trigger them.

VALIDATION QUERY 2 — Personal Loan:
  Required Data : transactions.csv (salary_detected, monthly_inflow)
  Pass Condition: salary_detected = True AND monthly_inflow > 300000
  Fail Reason   : If salary_detected = False → "No consistent salary
                  credit detected. Customer needs 2+ fintech credits
                  > ₦200,000 to qualify."

VALIDATION QUERY 3 — Investment Plan:
  Required Data : transactions.csv (monthly_inflow),
                  accounts.csv (account_type, current_balance)
  Pass Condition: monthly_inflow > 2,000,000
  Enhancement   : If current_balance > 5,000,000 consistently →
                  recommend premium investment tier.

COMPLAINTS INTEGRATION (via complaints.csv):
  The Trajectory Agent must check complaints_df before making
  recommendations. If a customer has open fraud complaints
  (fraud_related = 1, complaint_status = "open" or "escalated"):
    → DO NOT make product recommendations until complaints resolved.
    → Flag account for FRM review first.
  If a customer has SLA breach complaints (sla_breach_flag = 1):
    → Note service quality issue in recommendation report.
    → Route service quality concern to AOD or the breaching department.


=========================================================================
SECTION 6: SAMPLE OUTPUT FORMAT FOR TRAJECTORY AGENT
=========================================================================

The Trajectory Agent should produce recommendations in this format
for every customer analysed, citing this policy document:

PRODUCT RECOMMENDATION REPORT
──────────────────────────────────────────────────────────────────────────
Customer        : [Full Name] | ID: [customer_id]
Account Type    : [savings / solo / current]
Age             : [X] years

ELIGIBILITY ASSESSMENT (per PRS-001):
  salary_detected          : [True / False]
  car_loan_signal_score    : [X.X] / 1.0
  uber_tracker             : [X] transport transactions
  monthly_inflow           : ₦[X,XXX,XXX]

RECOMMENDATION:
  Primary Product          : [Car Loan / Personal Loan / Investment Plan / None]
  Confidence               : [High / Medium / Low]
  Policy Basis             : PRS-001, Section [X]

CRITERIA MET:
  ✓ [List each criterion met with actual vs. threshold value]

CRITERIA UNMET:
  ✗ [List each criterion not met with gap to threshold]

NEXT BEST ACTION:
  [Specific, actionable advice for what the customer can do to qualify
   for the next product tier — e.g., "Route salary payments via Remita
   to trigger salary_detected = True within 2 months."]

COMPLIANCE FLAGS:
  Active Fraud Complaints  : [Yes / No]
  SLA Breaches on Account  : [X] breaches in last 90 days
  Recommended Action       : [Proceed / Hold pending FRM clearance]

Policy Reference           : {self.bank_name} PRS-001 v1.0 ({self.display_date})
──────────────────────────────────────────────────────────────────────────


=========================================================================
DOCUMENT CONTROL
=========================================================================

Policy Owner    : Head of Retail Banking Products
Approver        : Chief Retail Banking Officer
Review Cycle    : Quarterly (thresholds reviewed against product performance)
Last Updated    : {self.display_date}
Contact         : retailproducts@sentinelbank.ng | Ext. 6000


=========================================================================
END OF DOCUMENT PRS-001
=========================================================================
"""
        return self._package_for_rag(
            "PRS-001",
            "Product Recommendation Policy",
            "policy",
            "1.0",
            policy
        )

    # =========================================================================
    # ORCHESTRATION
    # =========================================================================

    # =========================================================================
    # DATASET VALIDATION
    # =========================================================================

    def validate_entire_dataset(
        self,
        customers_csv:    Path = None,
        accounts_csv:     Path = None,
        transactions_csv: Path = None,
        complaints_csv:   Path = None,
        sample_size:      int  = 500,
    ) -> Dict:
        """
        Validate all four datasets against the six generated policies.

        Default paths resolve to:
            app/sentinel bank dataset/customers.csv
            app/sentinel bank dataset/accounts.csv
            app/sentinel bank dataset/transactions.csv
            app/sentinel bank dataset/complaints.csv

        Override any path by passing it explicitly, e.g.:
            engine.validate_entire_dataset(
                customers_csv=Path("my/other/folder/customers.csv")
            )

        Args:
            customers_csv    : Path to customers.csv
            accounts_csv     : Path to accounts.csv
            transactions_csv : Path to transactions.csv
            complaints_csv   : Path to complaints.csv
            sample_size      : Max rows to sample per validation test

        Returns:
            Dict with keys: routing, fraud_detection, product_recommendations,
                            sla_compliance, overall_score, timestamp
        """
        import pandas as pd
        from datetime import datetime as dt

        # ------------------------------------------------------------------
        # Resolve paths — fall back to module-level constants
        # ------------------------------------------------------------------
        DATASET_DIR = Path("app/sentinel bank dataset")

        customers_csv    = Path(customers_csv)    if customers_csv    else DATASET_DIR / "customers.csv"
        accounts_csv     = Path(accounts_csv)     if accounts_csv     else DATASET_DIR / "accounts.csv"
        transactions_csv = Path(transactions_csv) if transactions_csv else DATASET_DIR / "transactions.csv"
        complaints_csv   = Path(complaints_csv)   if complaints_csv   else DATASET_DIR / "complaints.csv"

        print("\n" + "=" * 70)
        print(" " * 18 + "COMPLETE DATASET VALIDATION")
        print("=" * 70)

        # ------------------------------------------------------------------
        # Verify all files exist before loading
        # ------------------------------------------------------------------
        missing = []
        for p in [customers_csv, accounts_csv, transactions_csv, complaints_csv]:
            if not p.exists():
                missing.append(str(p))

        if missing:
            print("\n❌  MISSING FILES — cannot proceed:")
            for m in missing:
                print(f"     {m}")
            print(
                f"\n   Update DATASET_DIR in this file to the correct folder.\n"
                f"   Current value: '{DATASET_DIR}'\n"
            )
            return {"error": "missing_files", "missing": missing}

        # ------------------------------------------------------------------
        # Load datasets
        # ------------------------------------------------------------------
        print("\nLoading datasets...")
        customers_df    = pd.read_csv(customers_csv)
        accounts_df     = pd.read_csv(accounts_csv)
        transactions_df = pd.read_csv(transactions_csv)
        complaints_df   = pd.read_csv(complaints_csv)

        print(f"  ✓ customers.csv     : {len(customers_df):,} rows")
        print(f"  ✓ accounts.csv      : {len(accounts_df):,} rows")
        print(f"  ✓ transactions.csv  : {len(transactions_df):,} rows")
        print(f"  ✓ complaints.csv    : {len(complaints_df):,} rows")

        results = {}

        # ==================================================================
        # TEST 1: COMPLAINT ROUTING ACCURACY (POL-CCH-001)
        # ==================================================================
        print("\n" + "-" * 70)
        print("TEST 1: Complaint Routing Accuracy  (POL-CCH-001)")
        print("-" * 70)

        # Department routing rules from map_transaction_to_department()
        # mirrored exactly from data_generator.py
        def expected_dept(row) -> str:
            if row.get("fraud_related", 0) == 1:
                return "FRM"
            if str(row.get("complaint_text", "")).lower().find("atm") != -1 or \
               str(row.get("complaint_text", "")).lower().find("card") != -1 or \
               str(row.get("complaint_text", "")).lower().find("pos") != -1 or \
               str(row.get("complaint_text", "")).lower().find("declined") != -1 or \
               str(row.get("complaint_text", "")).lower().find("swallowed") != -1 or \
               str(row.get("complaint_text", "")).lower().find("pin") != -1:
                return "COC"
            if str(row.get("complaint_text", "")).lower().find("app") != -1 or \
               str(row.get("complaint_text", "")).lower().find("login") != -1 or \
               str(row.get("complaint_text", "")).lower().find("ussd") != -1 or \
               str(row.get("complaint_text", "")).lower().find("crash") != -1:
                return "DCS"
            if str(row.get("complaint_text", "")).lower().find("statement") != -1 or \
               str(row.get("complaint_text", "")).lower().find("bvn") != -1 or \
               str(row.get("complaint_text", "")).lower().find("close") != -1 or \
               str(row.get("complaint_text", "")).lower().find("charges") != -1:
                return "AOD"
            if str(row.get("complaint_text", "")).lower().find("loan") != -1 or \
               str(row.get("complaint_text", "")).lower().find("credit") != -1 or \
               str(row.get("complaint_text", "")).lower().find("repayment") != -1 or \
               str(row.get("complaint_text", "")).lower().find("interest") != -1:
                return "CLS"
            return "TSU"

        sample = complaints_df.head(min(sample_size, len(complaints_df)))
        correct = 0
        misrouted = []

        for _, row in sample.iterrows():
            predicted = expected_dept(row)
            actual    = row.get("department_code", "")
            if predicted == actual:
                correct += 1
            else:
                misrouted.append({
                    "complaint_id": row.get("complaint_id"),
                    "predicted":    predicted,
                    "actual":       actual,
                    "text_snippet": str(row.get("complaint_text", ""))[:80]
                })

        routing_accuracy = (correct / len(sample)) * 100 if len(sample) > 0 else 0

        # SLA breach rate
        sla_breach_rate = (
            complaints_df["sla_breach_flag"].mean() * 100
            if "sla_breach_flag" in complaints_df.columns else 0
        )

        # Priority distribution
        priority_dist = (
            complaints_df["priority_level"].value_counts().to_dict()
            if "priority_level" in complaints_df.columns else {}
        )

        results["routing"] = {
            "accuracy":         routing_accuracy,
            "correct":          correct,
            "total_sampled":    len(sample),
            "misrouted_count":  len(misrouted),
            "sla_breach_rate":  sla_breach_rate,
            "priority_dist":    priority_dist,
            "top_misroutes":    misrouted[:5],
        }

        status = "✅" if routing_accuracy >= 90 else "⚠️ " if routing_accuracy >= 75 else "❌"
        print(f"  {status} Routing Accuracy   : {routing_accuracy:.1f}%  (target > 90%)")
        print(f"     Correct / Sampled : {correct} / {len(sample)}")
        print(f"     SLA Breach Rate   : {sla_breach_rate:.1f}%")
        print(f"     Priority dist     : {priority_dist}")
        if misrouted[:3]:
            print(f"     Sample misroutes  :")
            for m in misrouted[:3]:
                print(f"       [{m['complaint_id']}] predicted={m['predicted']} actual={m['actual']}")
                print(f"       text: \"{m['text_snippet']}...\"")

        # ==================================================================
        # TEST 2: FRAUD DETECTION COVERAGE (FRM-001 + FRM-002)
        # ==================================================================
        print("\n" + "-" * 70)
        print("TEST 2: Fraud Detection Coverage  (FRM-001 + FRM-002)")
        print("-" * 70)

        # Use module-level MERCHANT_RISK and FLAG_WEIGHTS constants
        # (single source of truth — also imported by rag_query.py)
        def compute_risk_score(row) -> int:
            score = 0
            trace = str(row.get("fraud_explainability_trace", "normal_pattern"))
            for flag in trace.split(","):
                score += FLAG_WEIGHTS.get(flag.strip(), 0)
            merchant_cat = str(row.get("merchant_category", "")).lower()
            score += MERCHANT_RISK.get(merchant_cat, 0)
            return min(score, 100)

        fraud_txns    = transactions_df[transactions_df["is_fraud_score"] == 1]
        non_fraud_txns = transactions_df[transactions_df["is_fraud_score"] == 0]

        fraud_sample = fraud_txns.head(min(sample_size, len(fraud_txns)))
        non_fraud_sample = non_fraud_txns.head(min(200, len(non_fraud_txns)))

        fraud_scores     = [compute_risk_score(r) for _, r in fraud_sample.iterrows()]
        non_fraud_scores = [compute_risk_score(r) for _, r in non_fraud_sample.iterrows()]

        avg_fraud_score     = sum(fraud_scores) / len(fraud_scores)         if fraud_scores     else 0
        avg_non_fraud_score = sum(non_fraud_scores) / len(non_fraud_scores) if non_fraud_scores else 0

        # True positive: fraud txn scores >= 31 (at least MEDIUM risk)
        true_positives  = sum(1 for s in fraud_scores if s >= 31)
        # False positive: non-fraud txn scores >= 61 (HIGH or CRITICAL)
        false_positives = sum(1 for s in non_fraud_scores if s >= 61)

        tp_rate = (true_positives / len(fraud_scores) * 100)     if fraud_scores     else 0
        fp_rate = (false_positives / len(non_fraud_scores) * 100) if non_fraud_scores else 0

        risk_distribution = {
            "LOW (0-30)":      sum(1 for s in fraud_scores if s <= 30),
            "MEDIUM (31-60)":  sum(1 for s in fraud_scores if 31 <= s <= 60),
            "HIGH (61-85)":    sum(1 for s in fraud_scores if 61 <= s <= 85),
            "CRITICAL (86+)":  sum(1 for s in fraud_scores if s >= 86),
        }

        # Trace flag coverage
        all_traces = transactions_df["fraud_explainability_trace"].dropna() \
            if "fraud_explainability_trace" in transactions_df.columns else []
        flag_counts = {}
        for trace in all_traces:
            for flag in str(trace).split(","):
                f = flag.strip()
                flag_counts[f] = flag_counts.get(f, 0) + 1

        results["fraud_detection"] = {
            "avg_fraud_score":       avg_fraud_score,
            "avg_non_fraud_score":   avg_non_fraud_score,
            "true_positive_rate":    tp_rate,
            "false_positive_rate":   fp_rate,
            "fraud_txn_count":       len(fraud_txns),
            "fraud_rate_pct":        len(fraud_txns) / len(transactions_df) * 100,
            "risk_distribution":     risk_distribution,
            "flag_counts":           flag_counts,
            "samples_tested":        len(fraud_scores),
        }

        status = "✅" if avg_fraud_score >= 31 else "⚠️ " if avg_fraud_score >= 15 else "❌"
        print(f"  {status} Avg Fraud Risk Score  : {avg_fraud_score:.1f}/100  (target > 31)")
        print(f"     Avg Non-Fraud Score   : {avg_non_fraud_score:.1f}/100  (target < 31)")
        print(f"     True Positive Rate    : {tp_rate:.1f}%  (fraud flagged as ≥ MEDIUM)")
        print(f"     False Positive Rate   : {fp_rate:.1f}%  (legit flagged as ≥ HIGH)")
        print(f"     Total Fraud Txns      : {len(fraud_txns):,} of {len(transactions_df):,} ({results['fraud_detection']['fraud_rate_pct']:.1f}%)")
        print(f"     Risk Distribution     :")
        for level, count in risk_distribution.items():
            print(f"       {level:<18}: {count}")
        print(f"     Trace Flag Counts     :")
        for flag, cnt in sorted(flag_counts.items(), key=lambda x: -x[1])[:5]:
            print(f"       {flag:<30}: {cnt:,}")

        # ==================================================================
        # TEST 3: PRODUCT RECOMMENDATION VALIDATION (PRS-001)
        # ==================================================================
        print("\n" + "-" * 70)
        print("TEST 3: Product Recommendation Accuracy  (PRS-001)")
        print("-" * 70)

        def expected_product(row) -> str:
            """Mirror exact logic from data_generator.py"""
            monthly_inflow       = float(row.get("monthly_inflow_tracker", 0) or 0)
            salary_detected      = bool(row.get("salary_detected", False))
            car_loan_score       = float(row.get("car_loan_signal_score", 0) or 0)

            if monthly_inflow > 2_000_000:
                return "Investment Plan"
            if car_loan_score >= 0.7:
                return "Car Loan"
            if salary_detected and monthly_inflow > 300_000:
                return "Personal Loan"
            return "None"

        # Build monthly_inflow_tracker from transactions (credit side)
        credit_txns = transactions_df[transactions_df["transaction_type"] == "credit"].copy()
        # Join to get customer_id via account_id
        credit_with_acct = credit_txns.merge(
            accounts_df[["account_id", "customer_id"]], on="account_id", how="left"
        )
        monthly_inflow_map = (
            credit_with_acct.groupby("customer_id")["amount"].sum().to_dict()
        )

        rec_sample = transactions_df[
            transactions_df["recommended_product"].notna()
        ].head(min(sample_size, len(transactions_df)))

        correct_recs   = 0
        incorrect_recs = []
        product_counts = {}

        for _, row in rec_sample.iterrows():
            actual_product = str(row.get("recommended_product", "None"))

            # Augment row with monthly_inflow_tracker (not in raw transactions)
            acct_row    = accounts_df[accounts_df["account_id"] == row.get("account_id")]
            customer_id = acct_row["customer_id"].iloc[0] if len(acct_row) > 0 else None
            inflow      = monthly_inflow_map.get(customer_id, 0)

            augmented = row.to_dict()
            augmented["monthly_inflow_tracker"] = inflow

            predicted = expected_product(augmented)

            product_counts[actual_product] = product_counts.get(actual_product, 0) + 1

            if predicted == actual_product:
                correct_recs += 1
            else:
                incorrect_recs.append({
                    "predicted": predicted,
                    "actual":    actual_product,
                    "score":     row.get("car_loan_signal_score", 0),
                    "salary":    row.get("salary_detected", False),
                    "inflow":    inflow,
                })

        rec_accuracy = (correct_recs / len(rec_sample)) * 100 if len(rec_sample) > 0 else 0

        results["product_recommendations"] = {
            "accuracy":         rec_accuracy,
            "correct":          correct_recs,
            "total_sampled":    len(rec_sample),
            "product_counts":   product_counts,
            "incorrect_sample": incorrect_recs[:5],
        }

        status = "✅" if rec_accuracy >= 80 else "⚠️ " if rec_accuracy >= 65 else "❌"
        print(f"  {status} Recommendation Accuracy : {rec_accuracy:.1f}%  (target > 80%)")
        print(f"     Correct / Sampled      : {correct_recs} / {len(rec_sample)}")
        print(f"     Product Distribution   :")
        for prod, cnt in sorted(product_counts.items(), key=lambda x: -x[1]):
            print(f"       {prod:<20}: {cnt:,}")
        if incorrect_recs[:3]:
            print(f"     Sample mismatches      :")
            for m in incorrect_recs[:3]:
                print(f"       predicted={m['predicted']} | actual={m['actual']} "
                      f"| score={m['score']:.2f} | salary={m['salary']} | inflow=₦{m['inflow']:,.0f}")

        # ==================================================================
        # TEST 4: SLA COMPLIANCE (POL-CCH-001 × complaints.csv)
        # ==================================================================
        print("\n" + "-" * 70)
        print("TEST 4: SLA Compliance by Department  (POL-CCH-001)")
        print("-" * 70)

        # Use module-level EXPECTED_SLA constant (single source of truth)
        sla_results = {}
        if "department_code" in complaints_df.columns and \
           "sla_breach_flag" in complaints_df.columns and \
           "resolution_time_hours" in complaints_df.columns:

            for dept, expected_hours in EXPECTED_SLA.items():
                dept_complaints = complaints_df[complaints_df["department_code"] == dept]
                if len(dept_complaints) == 0:
                    continue
                breach_count  = dept_complaints["sla_breach_flag"].sum()
                breach_rate   = breach_count / len(dept_complaints) * 100
                avg_res_hours = dept_complaints["resolution_time_hours"].mean()
                # Verify sla_hours_limit field matches policy
                if "sla_hours_limit" in dept_complaints.columns:
                    mismatch = dept_complaints[
                        dept_complaints["sla_hours_limit"] != expected_hours
                    ]
                    limit_correct = len(mismatch) == 0
                else:
                    limit_correct = None

                sla_results[dept] = {
                    "expected_sla_hours": expected_hours,
                    "avg_resolution_hrs": avg_res_hours,
                    "total_complaints":   len(dept_complaints),
                    "breach_count":       int(breach_count),
                    "breach_rate_pct":    breach_rate,
                    "sla_limit_correct":  limit_correct,
                }

                status = "✅" if breach_rate <= 30 else "⚠️ " if breach_rate <= 50 else "❌"
                print(f"  {status} {dept}  SLA={expected_hours}h | "
                      f"Avg Resolution={avg_res_hours:.0f}h | "
                      f"Breaches={breach_count}/{len(dept_complaints)} ({breach_rate:.1f}%)"
                      + (f" | limit_field_match={'✓' if limit_correct else '✗'}"
                         if limit_correct is not None else ""))

        results["sla_compliance"] = sla_results

        # ==================================================================
        # OVERALL SCORE
        # ==================================================================
        print("\n" + "=" * 70)
        print(" " * 22 + "VALIDATION SUMMARY")
        print("=" * 70)

        # Weighted overall score
        overall_score = (
            routing_accuracy              * 0.35 +
            min(avg_fraud_score * 1.5, 100) * 0.30 +   # scale: target 31+ → ~47
            rec_accuracy                  * 0.25 +
            max(0, 100 - sla_breach_rate) * 0.10        # lower breach rate = higher score
        )
        overall_score = min(overall_score, 100)

        results["overall_score"] = overall_score
        results["timestamp"]     = dt.now().isoformat()
        results["dataset_paths"] = {
            "customers":    str(customers_csv),
            "accounts":     str(accounts_csv),
            "transactions": str(transactions_csv),
            "complaints":   str(complaints_csv),
        }

        print(f"\n  Complaint Routing Accuracy  : {routing_accuracy:.1f}%   (weight 35%)")
        print(f"  Fraud Detection Avg Score   : {avg_fraud_score:.1f}/100  (weight 30%)")
        print(f"  Product Rec Accuracy        : {rec_accuracy:.1f}%   (weight 25%)")
        print(f"  SLA Compliance (inv breach) : {max(0, 100 - sla_breach_rate):.1f}%   (weight 10%)")
        print(f"\n  ──────────────────────────────────────────")
        print(f"  OVERALL SCORE               : {overall_score:.1f}%")

        if overall_score >= 90:
            print("\n  ✅  EXCELLENT — Production Ready!")
        elif overall_score >= 80:
            print("\n  ✅  GOOD — Minor improvements recommended.")
        elif overall_score >= 70:
            print("\n  ⚠️   ACCEPTABLE — Some policy gaps to address.")
        else:
            print("\n  ❌  NEEDS WORK — Review policy documents and dataset alignment.")

        print("=" * 70 + "\n")
        return results


    def generate_all_documents(self) -> List[Dict]:
        """
        Generate all six policy documents in order.

        Returns:
            List[Dict]: Six packaged documents ready for RAG ingestion.
                        Order: POL-CCH-001, FRM-001, TSU-POL-002,
                               FAQ-001, FRM-002, PRS-001
        """
        return [
            self.generate_complaint_handling_policy(),   # Dispatcher Agent
            self.generate_fraud_detection_guidelines(),  # Sentinel Agent (base)
            self.generate_transaction_policies(),        # All agents
            self.generate_faq_document(),                # Customer-facing
            self.generate_merchant_risk_profiles(),      # Sentinel Agent (merchant)
            self.generate_product_recommendation_policy()  # Trajectory Agent
        ]

    def save_all_policies(self, output_dir: Path):
        """
        Generate and save all six policy documents to disk for RAG ingestion.

        Output Directory Structure:
            output_dir/
            ├── policies/
            │   ├── POL-CCH-001.txt   (Complaint handling — Dispatcher)
            │   ├── FRM-001.txt       (Fraud detection — Sentinel)
            │   ├── TSU-POL-002.txt   (Transaction policies — All agents)
            │   ├── FRM-002.txt       (Merchant risk profiles — Sentinel)
            │   └── PRS-001.txt       (Product recommendation — Trajectory)
            └── faqs/
                └── FAQ-001.txt       (Customer FAQ)

        Args:
            output_dir (Path): Base directory for saved files.
        """
        output_dir = Path(output_dir)
        policies_dir = output_dir / "policies"
        faqs_dir = output_dir / "faqs"

        policies_dir.mkdir(parents=True, exist_ok=True)
        faqs_dir.mkdir(parents=True, exist_ok=True)

        documents = self.generate_all_documents()

        print(f"\nGenerating {len(documents)} policy documents...")
        print("=" * 60)

        for doc in documents:
            if doc['category'] == 'knowledge_base':
                target_folder = faqs_dir
            else:
                target_folder = policies_dir

            filename = f"{doc['document_id']}.txt"
            filepath = target_folder / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(doc['content'])

            size_kb = len(doc['content'].encode('utf-8')) / 1024
            print(f"  ✓ {doc['document_id']}.txt  "
                  f"({doc['title'][:40]})  [{size_kb:.1f} KB]")

        print("=" * 60)
        policy_count = len([d for d in documents if d['category'] != 'knowledge_base'])
        faq_count = len([d for d in documents if d['category'] == 'knowledge_base'])
        print(f"\n  Policies saved : {policy_count} files → {policies_dir}")
        print(f"  FAQs saved     : {faq_count} file  → {faqs_dir}")
        print(f"\n✅ All {len(documents)} documents ready for RAG ingestion.")
        print("\nNext step:")
        print("  cd ../rag_system && python ingest_documents.py\n")


# =============================================================================
# DATASET PATHS — UPDATE THIS SECTION IF FILES MOVE
# =============================================================================

# Root folder containing all four CSV datasets
DATASET_DIR = Path("app/sentinel bank dataset")

# Individual dataset file paths (resolved relative to DATASET_DIR)
CUSTOMERS_CSV    = DATASET_DIR / "customers.csv"
ACCOUNTS_CSV     = DATASET_DIR / "accounts.csv"
TRANSACTIONS_CSV = DATASET_DIR / "transactions.csv"
COMPLAINTS_CSV   = DATASET_DIR / "complaints.csv"


# =============================================================================
# MAIN EXECUTION BLOCK
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  SENTINEL BANK — POLICY DOCUMENT GENERATOR v2")
    print("  AI Engineer 2 | Week 1 Knowledge Base Setup")
    print("  Documents: 6 (4 original + FRM-002 + PRS-001)")
    print("=" * 60)

    # Verify dataset files exist before proceeding
    print("\nVerifying dataset paths...")
    all_found = True
    for csv_path in [CUSTOMERS_CSV, ACCOUNTS_CSV, TRANSACTIONS_CSV, COMPLAINTS_CSV]:
        exists = csv_path.exists()
        status = "✓" if exists else "✗ MISSING"
        print(f"  {status}  {csv_path}")
        if not exists:
            all_found = False

    if not all_found:
        print(
            f"\n⚠️  One or more dataset files not found under '{DATASET_DIR}'.\n"
            f"   Update the DATASET_DIR constant at the top of this file\n"
            f"   to match the actual folder location on your machine.\n"
            f"   Policy documents will still be generated regardless.\n"
        )

    print()

    current_dir = Path(__file__).parent

    generator = BankingPolicyGenerator(bank_name="Sentinel Bank Nigeria")
    generator.save_all_policies(current_dir)

    print("Generated Documents:")
    print("  policies/POL-CCH-001.txt — Complaint Handling (Dispatcher)")
    print("  policies/FRM-001.txt     — Fraud Detection (Sentinel base)")
    print("  policies/TSU-POL-002.txt — Transaction Policies (All agents)")
    print("  policies/FRM-002.txt     — Merchant Risk Profiles (Sentinel)")
    print("  policies/PRS-001.txt     — Product Recommendations (Trajectory)")
    print("  faqs/FAQ-001.txt         — Customer FAQ")
    print()
    print("Dataset Paths (used by validate_entire_dataset):")
    print(f"  CUSTOMERS    : {CUSTOMERS_CSV}")
    print(f"  ACCOUNTS     : {ACCOUNTS_CSV}")
    print(f"  TRANSACTIONS : {TRANSACTIONS_CSV}")
    print(f"  COMPLAINTS   : {COMPLAINTS_CSV}")
    print("=" * 60 + "\n")