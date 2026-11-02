# run_pipeline.py
import sys
from app import create_app, init_db
from ingestion.pipeline import run_etl

# 1. Create the app to get access to the DB config
app = create_app()

# 2. Push the application context
# This allows the script to use 'db.session' and your models
if __name__ == "__main__":
    with app.app_context():
        print("Starting ETL Pipeline...")
        try:
            init_db()
            summary = run_etl()
            print(f"ETL Pipeline completed: {summary}")
        except Exception as e:
            print(f"ETL Pipeline Failed: {e}")
            # A non-zero exit lets a scheduler see the failure
            sys.exit(1)
