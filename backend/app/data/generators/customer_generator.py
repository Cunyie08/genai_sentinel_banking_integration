from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# Nigerian states and major cities
NIGERIAN_STATES = [
    "Lagos",
    "Kano",
    "Rivers",
    "Kaduna",
    "Oyo",
    "Katsina",
    "Anambra",
    "Borno",
    "Delta",
    "Ogun",
    "Edo",
    "Kwara",
    "Enugu",
    "Bauchi",
    "Plateau",
    "Imo",
    "Akwa Ibom",
    "Osun",
    "Ondo",
    "Abia",
    "Sokoto",
    "Ekiti",
    "Kogi",
    "Bayelsa",
    "Adamawa",
    "Nasarawa",
    "Kebbi",
    "Cross River",
    "Gombe",
    "Zamfara",
    "Ebonyi",
    "Taraba",
    "Jigawa",
    "Benue",
    "Yobe",
    "Niger",
    "FCT",
]

# Common Nigerian first names
NIGERIAN_FIRST_NAMES = [
    # Yoruba
    "Adeola",
    "Adebayo",
    "Oluwaseun",
    "Temitope",
    "Babatunde",
    "Folake",
    "Ayodeji",
    "Olumide",
    "Titilayo",
    "Kehinde",
    "Taiwo",
    # Igbo
    "Chukwuemeka",
    "Ngozi",
    "Chioma",
    "Obinna",
    "Adaeze",
    "Kelechi",
    "Chinedu",
    "Ifeoma",
    "Nnamdi",
    "Amara",
    # Hausa
    "Abubakar",
    "Fatima",
    "Ibrahim",
    "Zainab",
    "Musa",
    "Hauwa",
    "Yusuf",
    "Aisha",
    "Usman",
    "Halima",
    # Others
    "Emmanuel",
    "Grace",
    "Daniel",
    "Blessing",
    "David",
    "Faith",
    "Samuel",
    "Joy",
    "Michael",
    "Peace",
]

# Common Nigerian surnames
NIGERIAN_SURNAMES = [
    # Yoruba
    "Adeyemi",
    "Ogunleye",
    "Oladipo",
    "Ajayi",
    "Ojo",
    "Williams",
    "Adebayo",
    "Oluwole",
    "Adewale",
    "Okonkwo",
    # Igbo
    "Okafor",
    "Nwankwo",
    "Eze",
    "Okeke",
    "Nwosu",
    "Obi",
    "Onyeka",
    "Anyanwu",
    "Udeh",
    "Ike",
    # Hausa
    "Bello",
    "Suleiman",
    "Abdullahi",
    "Mohammed",
    "Aliyu",
    "Umar",
    "Hassan",
    "Ahmad",
    "Yusuf",
    "Ibrahim",
    # Others
    "Johnson",
    "Peter",
    "Paul",
    "James",
    "John",
]

ACCOUNT_TYPES = ["Savings", "Current", "Fixed Deposit", "Student Account"]


def generate_account_number():
    """Generate a 10-digit Nigerian bank account number"""
    return "".join([str(random.randint(0, 9)) for _ in range(10)])


def generate_nigerian_phone():
    """Generate Nigerian phone number format"""
    prefixes = [
        "0803",
        "0806",
        "0813",
        "0816",
        "0703",
        "0706",
        "0810",
        "0814",
        "0903",
        "0906",
    ]
    return random.choice(prefixes) + "".join(
        [str(random.randint(0, 9)) for _ in range(7)]
    )


def generate_customer_data(num_customers=1000):
    """Generate synthetic Nigerian customer data"""
    customers = []

    for i in range(num_customers):
        first_name = random.choice(NIGERIAN_FIRST_NAMES)
        last_name = random.choice(NIGERIAN_SURNAMES)
        state = random.choice(NIGERIAN_STATES)

        customer = {
            "account_number": generate_account_number(),
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}",
            "phone": generate_nigerian_phone(),
            "state": state,
            "lga": f"{fake.city()} LGA",  # Simplified LGA generation
            "address": f"{fake.building_number()} {fake.street_name()}, {state}",
            "account_type": random.choice(ACCOUNT_TYPES),
            "balance": round(random.uniform(5000, 5000000), 2),  # ₦5k to ₦5M
            "is_active": random.choice([True, True, True, False]),  # 75% active
            "created_at": fake.date_time_between(start_date="-5y", end_date="now"),
        }
        customers.append(customer)

    return customers


if __name__ == "__main__":
    # Test generation
    customers = generate_customer_data(10)
    for customer in customers[:3]:
        print(customer)
