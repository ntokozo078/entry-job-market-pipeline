# run_pipeline.py
from app import create_app
from ingestion.pipeline import run_etl

# 1. Create the app to get access to the DB config
app = create_app()

# 2. Push the application context
# This allows the script to use 'db.session' and your models
if __name__ == "__main__":
    with app.app_context():
        print("Starting ETL Pipeline...")
        try:
            run_etl()
            print("ETL Pipeline completed successfully.")
        except Exception as e:
            print(f"ETL Pipeline Failed: {e}")