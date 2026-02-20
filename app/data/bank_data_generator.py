# ==========================================================
# PROJECT SENTINNEL - FULLY INTEGRATED MASTER DATA ENGINE
# ==========================================================

import uuid
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from collections import defaultdict


fake = Faker()
CURRENT_YEAR = 2026
NUM_CUSTOMERS = 3000

# ==========================================================
# UNIQUE TRACKERS
# ==========================================================

used_customer_ids=set()
used_emails=set()
used_phones=set()
used_bvns=set()
used_nins=set()
used_account_numbers=set()

# ==========================================================
# TELECOM PROVIDERS
# ==========================================================

TELCO_PREFIXES={
    "MTN":["0803","0806","0703","0706","0813","0810","0903","0906"],
    "Airtel":["0802","0808","0708","0812","0902","0907"],
    "Glo":["0805","0807","0705","0815","0905"],
    "9mobile":["0809","0817","0818","0909","0908"]
}

# ==========================================================
# STATES (36)
# ==========================================================

STATES=[
"Abia","Adamawa","Akwa Ibom","Anambra","Bauchi","Bayelsa",
"Benue","Borno","Cross River","Delta","Ebonyi","Edo",
"Ekiti","Enugu","Gombe","Imo","Jigawa","Kaduna",
"Kano","Katsina","Kebbi","Kogi","Kwara","Lagos",
"Nasarawa","Niger","Ogun","Ondo","Osun","Oyo",
"Plateau","Rivers","Sokoto","Taraba","Yobe","Zamfara"
]

# ==========================================================
# GENDER-ALIGNED NAMES
# ==========================================================

MALE_NAMES=[
"Ade","Chris","Abdullahi","Oludayo","Mubarak","Uche",
"Emeka","Michael","Taofeek","Sunday","Seyi",
"Reuben","Olatunji","Solomon","Kehinde","Victor"
]

FEMALE_NAMES=[
"Zainab","Naheemat","Aisha","Aminat","Glory",
"Joy","Anna","Mary","Esther","Blossom",
"Perpetual","Aaliyah","Funke","Funmilola",
"Titilope","Halimah","Mariam"
]

LAST_NAMES=[
"Ekwugum","Oluwole","Adebiyi","Onyekachi","Badrudeen",
"Willie","David","Ekpo","Friday","Hassan","Ajayi",
"Tijani","Bello","Lawal","Williams","Garba",
"Abubakar","Umar","Yusuf","Musa","Adeleke",
"Salako","Alimi","Smith","Adebayo","Ajala",
"Ogunleye","Ogunkoya","Sanni","Shabi",
"Oladele","Uthman"
]

# ==========================================================
# EMAIL GENERATOR (BIAS + UNIQUE)
# ==========================================================

def generate_email(first,last):

    domains=random.choices(
        ["gmail.com","yahoo.com","outlook.com","protonmail.com","10minutemail.com"],
        weights=[70,25,3,1,1]
    )[0]

    while True:
        suffix=str(random.randint(10,999)) if random.random()<0.35 else ""
        email=f"{first.lower()}.{last.lower()}{suffix}@{domains}"

        if email not in used_emails:
            used_emails.add(email)
            return email

# ==========================================================
# PHONE GENERATOR UNIQUE + TELCO
# ==========================================================

def generate_phone():

    while True:

        telco=random.choice(list(TELCO_PREFIXES.keys()))
        prefix=random.choice(TELCO_PREFIXES[telco])

        number="+234"+prefix[1:]+''.join(str(random.randint(0,9)) for _ in range(7))

        if number not in used_phones:
            used_phones.add(number)
            return number,telco

# ==========================================================
# UNIQUE NUMBER GENERATOR (BVN/NIN)
# ==========================================================

def generate_unique_number(pool,digits=11):

    while True:
        num=str(random.randint(10**(digits-1),10**digits-1))

        if num not in pool:
            pool.add(num)
            return num

# ==========================================================
# ACCOUNT NUMBER UNIQUE
# ==========================================================

def generate_account_number():

    while True:
        acc=str(random.randint(10**9,10**10-1))

        if acc not in used_account_numbers:
            used_account_numbers.add(acc)
            return acc

# ==========================================================
# TRANSACTION HELPERS
# ==========================================================

CHANNELS=["mobile_app","ussd","atm","pos","web","branch","nibss_transfer"]

BANKS=[
"GTBank","Access Bank","Zenith Bank","UBA","First Bank",
"Fidelity Bank","Union Bank","Sterling Bank","Wema Bank",
"Opay","Kuda","PalmPay", "Providus Bank", "Lotus Bank",
"Premium Trust Bank","Polaris Bank", "Stanbic IBTC", 
]

STATUS_WEIGHTS={
"successful":85,
"failed":5,
"reversed":3,
"pending":2,
"timeout":2,
"queued":2,
"processing":1
}

FAILURE_REASONS=[
"none","insufficient_fund","network_error","system_failure",
"system_timeout","invalid_account_number",
"daily_transaction_limit_exceeded",
"compliance_restriction","suspected_fraud",
"issuer_unavailable"
]

def generate_status():
    return random.choices(
        list(STATUS_WEIGHTS.keys()),
        weights=list(STATUS_WEIGHTS.values())
    )[0]

def generate_failure(status):
    if status in ["failed","timeout"]:
        return random.choice(FAILURE_REASONS[1:])
    return "none"

def generate_device(channel):
    if channel=="mobile_app":
        return "DEV-"+uuid.uuid4().hex[:12].upper()
    return None

def generate_reference():
    return "TXN"+str(random.randint(10**11,10**12-1))

# ==========================================================
# CUSTOMER GENERATION
# ==========================================================

customers=[]

for _ in range(NUM_CUSTOMERS):

    while True:
        customer_id=str(uuid.uuid4())
        if customer_id not in used_customer_ids:
            used_customer_ids.add(customer_id)
            break

    gender=random.choice(["male","female"])

    first=random.choice(MALE_NAMES if gender=="male" else FEMALE_NAMES)
    last=random.choice(LAST_NAMES)

    age=random.randint(18,70)
    dob=datetime(CURRENT_YEAR-age,random.randint(1,12),random.randint(1,28))

    bvn=generate_unique_number(used_bvns)
    nin=generate_unique_number(used_nins)

    phone,telco=generate_phone()
    email=generate_email(first,last)

    customers.append({

        "customer_id":customer_id,
        "first_name":first,
        "last_name":last,
        "full_name":f"{first} {last}",
        "gender":gender,
        "age":age,
        "date_of_birth":dob.date(),
        "bvn":bvn,
        "nin":nin,
        "phone_number":phone,
        "telco_provider":telco,
        "email":email,
        "state_of_origin":random.choice(STATES),
        "residential_state":random.choice(STATES),
        "banking_branch":random.choice(STATES),
        "onboarding_date":fake.date_between("-5y","today")
    })

customers_df=pd.DataFrame(customers)

# ==========================================================
# ACCOUNT GENERATION (UNCHANGED STRUCTURE)
# ==========================================================

accounts=[]

for _,cust in customers_df.iterrows():

    acc_types=["savings"]

    if 16<=cust["age"]<=30:
        acc_types.append("solo")

    if cust["age"]>=18:
        acc_types.append("current")

    for acc_type in acc_types:

        if acc_type=="solo":
            balance=random.uniform(1000,200000)
        elif acc_type=="savings":
            balance=random.uniform(5000,2000000)
        else:
            balance=random.uniform(500000,10000000)

        accounts.append({

            "account_id":str(uuid.uuid4()),
            "customer_id":cust["customer_id"],
            "account_number":generate_account_number(),
            "account_type":acc_type,
            "currency":"NGN",
            "current_balance":round(balance,2),
            "opened_date":fake.date_between(cust["onboarding_date"],"today")
        })

accounts_df=pd.DataFrame(accounts)

# ==========================================================
# FRAUD PROFILE
# ==========================================================

high_risk=set(random.sample(list(customers_df.customer_id),int(0.05*NUM_CUSTOMERS)))

def fraud_logic(customer):
    if customer in high_risk:
        return random.random()<0.08
    return random.random()<0.01

# ==========================================================
# TRANSACTIONS WITH FULL FEATURES
# ==========================================================

# ----------------------------------------------------------
# MERCHANT DEFINITIONS
# ----------------------------------------------------------

MERCHANTS = {
    "supermarket": ["Shoprite","SPAR","Justrite","Ebeano","Market Square"],
    "restaurants": ["Chicken Republic","Kilimanjaro","Mr Biggs","Dominos","Cold Stone","Bukka Hut"],
    "fuel": ["NNPC Mega Station","TotalEnergies","Mobil","Oando","Conoil","MRS"],
    "transport": ["Uber","Bolt","LagRide","ABC Transport","Peace Mass Transit"],
    "telecoms": ["MTN","Airtel","Glo","9mobile"],
    "utilities": ["Ikeja Electric","EKEDC","IBEDC","AEDC","DSTV","GOtv","StarTimes"],
    "fintech": ["Paystack","Flutterwave","Interswitch","Remita","Monnify"],
    "education": ["University Tuition","WAEC","JAMB","Private School Fees"],
    "healthcare": ["Teaching Hospital","Private Hospital","Medplus","HealthPlus"]
}

# Personalization trackers
salary_tracker = defaultdict(int)
uber_tracker = defaultdict(int)
monthly_inflow_tracker = defaultdict(float)
monthly_outflow_tracker = defaultdict(float)


transactions=[]

for _,acc in accounts_df.iterrows():

    balance=acc["current_balance"]

    for _ in range(random.randint(15,40)):

        channel=random.choice(CHANNELS)
        device=generate_device(channel)

        txn_type=random.choice(["debit","credit"])

        amount=max(100,np.random.exponential(50000))

        merchant_category = random.choice(list(MERCHANTS.keys()))
        merchant_name = random.choice(MERCHANTS[merchant_category])

        if txn_type == "credit":
            monthly_inflow_tracker[acc["customer_id"]] += amount
    
        # Salary detection pattern (recurring large credit)
        if amount > 200000 and merchant_category == "fintech":
            salary_tracker[acc["customer_id"]] += 1

        salary_detected = salary_tracker[acc["customer_id"]] >= 2

        # Uber usage tracking
        if merchant_name in ["Uber","Bolt","LagRide"]:
            uber_tracker[acc["customer_id"]] += 1

        car_loan_score = 0

        if uber_tracker[acc["customer_id"]] >= 6:
            car_loan_score += 0.4

        if salary_detected:
            car_loan_score += 0.3

        if monthly_inflow_tracker[acc["customer_id"]] > 500000:
            car_loan_score += 0.3

        recommended_product = None

        if car_loan_score >= 0.7:
            recommended_product = "Car Loan"
        elif salary_detected and monthly_inflow_tracker[acc["customer_id"]] > 300000:
            recommended_product = "Personal Loan"
        elif monthly_inflow_tracker[acc["customer_id"]] > 2000000:
            recommended_product = "Investment Plan"

        if txn_type=="debit":
            amount=min(amount,balance*0.8)
            new_balance=balance-amount
        else:
            new_balance=balance+amount

        status=generate_status()

        if status in ["failed","reversed"]:
            new_balance=balance

        fraud = fraud_logic(acc["customer_id"])

        fraud_trace = []

        if fraud:
            if channel == "mobile_app":
                fraud_trace.append("mobile_channel_risk")

            if amount > (balance * 0.6):
                fraud_trace.append("high_amount_spike")

            if status == "failed":
                fraud_trace.append("multiple_failures")

        fraud_explainability_trace = ",".join(fraud_trace) if fraud_trace else "normal_pattern"


        transactions.append({

            "transaction_id":str(uuid.uuid4()),
            "transaction_reference_number":generate_reference(),
            "account_id":acc["account_id"],
            "channel":channel,
            "device_id":device,
            "counterparty_bank":random.choice(BANKS),
            "narration":fake.sentence(nb_words=6),
            "transaction_type":txn_type,
            "amount":round(amount,2),
            "currency":"NGN",
            "transaction_balance":round(new_balance,2),
            "transaction_status":status,
            "failure_reason":generate_failure(status),
            "is_fraud_score":int(fraud),
            "fraud_explainability_trace": fraud_explainability_trace,
            "merchant_category": merchant_category,
            "merchant_name": merchant_name,
            "salary_detected": salary_detected,
            "car_loan_signal_score": car_loan_score,
            "recommended_product": recommended_product,
            "transaction_timestamp":fake.date_time_between(acc["opened_date"],"now")
        })

        balance=new_balance

transactions_df=pd.DataFrame(transactions)

# ========================================================================
# COMPLAINT DATASET GENERATOR (Dispatcher-ready + SLA-aware + Fraud-aware)
# ========================================================================

# ==========================================================
# DEPARTMENT DEFINITIONS (POL-CCH-001 ALIGNED)
# ==========================================================

DEPARTMENTS = {
    "TSU": {
        "name": "Transaction Services Unit",
        "sla_hours": 48,
        "agents": ["TSU_A1","TSU_A2","TSU_A3","TSU_A4","TSU_A5"]
    },
    "COC": {
        "name": "Card Operations Center",
        "sla_hours": 48,
        "agents": ["COC_A1","COC_A2","COC_A3"]
    },
    "FRM": {
        "name": "Fraud Risk Management",
        "sla_hours": 24,
        "agents": ["FRM_A1","FRM_A2","FRM_A3"]
    },
    "DCS": {
        "name": "Digital Channels Support",
        "sla_hours": 72,
        "agents": ["DCS_A1","DCS_A2","DCS_A3"]
    },
    "AOD": {
        "name": "Account Operations Department",
        "sla_hours": 72,
        "agents": ["AOD_A1","AOD_A2","AOD_A3"]
    },
    "CLS": {
        "name": "Credit & Loan Services",
        "sla_hours": 96,
        "agents": ["CLS_A1","CLS_A2"]
    }
}

PRIORITY_LEVELS = {
    "Critical": 5,
    "High": 4,
    "Medium": 3,
    "Low": 2
}

SENTIMENTS = ["angry","neutral","calm"]

COMPLAINT_CHANNELS = ["call_center","mobile_app","email","branch","social_media"]

# ==========================================================
# COMPLAINT–TRANSACTION LINKING
# ==========================================================

def map_transaction_to_department(txn):
    """
    Route using logic aligned to POL-CCH-001
    """
    if txn["is_fraud_score"] == 1:
        return "FRM", "Critical"

    if txn["channel"] in ["atm","pos"]:
        return "COC", "High"

    if txn["transaction_status"] in ["failed","timeout","reversed"]:
        return "TSU", "High"

    return "TSU", "Medium"

# ==========================================================
# COMPLAINT TEXT GENERATOR (LLM + Dispatcher Ready)
# ==========================================================

def generate_complaint_text(txn, dept_code, sentiment):
    ref = txn["transaction_reference_number"]
    amount = txn["amount"]
    channel = txn["channel"]
    status = txn["transaction_status"]

    base_messages = {
        "TSU": [
            f"My transfer of ₦{amount} with reference {ref} was debited but the recipient has not received it.",
            f"I was charged ₦{amount} but the transaction failed. Please investigate reference {ref}.",
            f"This transaction with reference {ref} was reversed incorrectly."
        ],
        "COC": [
            f"My card transaction of ₦{amount} at POS failed but my account was debited. Ref {ref}.",
            f"The ATM transaction of ₦{amount} did not dispense cash but I was debited. Ref {ref}.",
            f"My card was declined even though I have sufficient balance."
        ],
        "FRM": [
            f"I noticed an unauthorized transaction of ₦{amount}. Reference {ref}. I did not authorize this.",
            f"My account appears to have been compromised. This transaction {ref} is suspicious.",
            f"I believe I am a victim of fraud. Please freeze my account immediately."
        ],
        "DCS": [
            f"I attempted a transaction of ₦{amount} but the mobile app failed with an error.",
            f"The banking app crashed during transaction {ref}.",
            f"I cannot complete transactions via USSD or mobile app."
        ],
        "AOD": [
            f"There is an issue with my account balance after transaction {ref}.",
            f"I need clarification on charges related to transaction {ref}.",
            f"My account statement does not reflect transaction {ref} correctly."
        ],
        "CLS": [
            f"My loan repayment linked to transaction {ref} was not processed correctly.",
            f"There is an issue with my loan disbursement.",
            f"I need clarification regarding interest applied to my account."
        ]
    }

    text = random.choice(base_messages.get(dept_code, base_messages["TSU"]))

    # Sentiment amplification
    if sentiment == "angry":
        text = "This is unacceptable. " + text + " I need urgent resolution immediately."
    elif sentiment == "calm":
        text = "Kindly assist. " + text

    return text


# ==========================================================
# GENERATE COMPLAINTS
# ==========================================================

complaints = []

complaint_counter = 1

for _, txn in transactions_df.iterrows():

    # 15% normal complaint rate
    complaint_probability = 0.15

    # Fraud auto-trigger
    if txn["is_fraud_score"] == 1:
        complaint_probability = 0.85

    if random.random() > complaint_probability:
        continue

    dept_code, priority = map_transaction_to_department(txn)
    dept = DEPARTMENTS[dept_code]

    # Sentiment logic
    if priority == "Critical":
        sentiment = "angry"
    else:
        sentiment = random.choices(SENTIMENTS, weights=[0.3,0.4,0.3])[0]

    # Complaint timestamps
    complaint_time = txn["transaction_timestamp"] + timedelta(
        minutes=random.randint(5, 720)
    )

    # Resolution time simulation
    resolution_hours = random.randint(2, dept["sla_hours"] + 48)
    resolution_time = complaint_time + timedelta(hours=resolution_hours)

    # SLA breach detection
    sla_breach = 1 if resolution_hours > dept["sla_hours"] else 0

    # Agent assignment simulation
    assigned_agent = random.choice(dept["agents"])

    # Generate the complaint text
    complaint_text = generate_complaint_text(txn, dept_code, sentiment)

    complaints.append({
        "complaint_id": f"CMP-{str(complaint_counter).zfill(6)}",
        "customer_id": txn["account_id"],   # linked through account
        "linked_transaction_id": txn["transaction_id"],
        "linked_reference": txn["transaction_reference_number"],
        "department_code": dept_code,
        "department_name": dept["name"],
        "priority_level": priority,
        "sentiment": sentiment,
        "complaint_channel": random.choice(COMPLAINT_CHANNELS),
        "assigned_agent_id": assigned_agent,
        "complaint_timestamp": complaint_time,
        "resolution_timestamp": resolution_time,
        "resolution_time_hours": resolution_hours,
        "sla_hours_limit": dept["sla_hours"],
        "sla_breach_flag": sla_breach,
        "complaint_status": random.choice(["open","resolved","escalated"]),
        "fraud_related": int(txn["is_fraud_score"]),
        "complaint_text": complaint_text,
        "complaint_narration": f"Customer reported issue regarding transaction {txn['transaction_reference_number']}"
    })

    complaint_counter += 1

complaints_df = pd.DataFrame(complaints)



# ==========================================================
# EXPORT
# ==========================================================

customers_df.to_csv("customers.csv",index=False)
accounts_df.to_csv("accounts.csv",index=False)
transactions_df.to_csv("transactions.csv",index=False)
complaints_df.to_csv("complaints.csv", index=False)

print("complaints.csv generated with dispatcher-aligned intelligence.")
print("Fully integrated Nigerian banking dataset generated successfully.")
