# Sentinel Bank - Database Setup Script
# Run this to populate the database with synthetic data

# Activate virtual environment
..\.venv\Scripts\Activate.ps1

# Run the seeding script
python app\database\seed.py

# Deactivate when done
deactivate
