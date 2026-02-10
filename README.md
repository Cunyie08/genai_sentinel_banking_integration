# Sentinel Bank Project

AI-driven banking middleware for intelligent customer service, fraud prevention, and personalized banking.

## Project Structure

```
Sentinnel_bank_project/
├── .venv/                    # Virtual environment
├── .gitignore
├── README.md
└── backend/                  # Backend service
    ├── app/
    │   ├── api/             # FastAPI routes
    │   │   └── routes/      # API endpoints
    │   ├── models/          # Database models
    │   ├── data/            # Data generation scripts
    │   │   └── generators/  # Faker scripts
    │   ├── database/        # Database connection & setup
    │   ├── services/        # Business logic
    │   ├── middleware/      # Authentication & middleware
    │   ├── cache/           # Redis caching
    │   └── monitoring/      # Performance metrics
    ├── tests/               # Unit tests
    ├── requirements.txt     # Python dependencies
    ├── main.py             # FastAPI entry point
    └── .env.example        # Environment variables template
```

## Setup

1. **Create virtual environment:**

   ```powershell
   python -m venv .venv
   .venv\Scripts\Activate
   ```

2. **Install dependencies:**

   ```powershell
   cd backend
   pip install -r requirements.txt
   ```

3. **Configure environment:**

   ```powershell
   cp .env.example .env
   # Edit .env with your database credentials
   ```

4. **Run the application:**
   ```powershell
   python main.py
   ```

## Team Structure

- **Member 1:** AI Engineer (Architect & Orchestrator)
- **Member 2:** AI Engineer (Security & Knowledge Specialist)
- **Member 3:** AI Developer (Backend & Data Architect)
- **Member 4:** AI Developer (Frontend & Experience Designer)
