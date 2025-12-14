# ingestion/pipeline.py
import logging
from app.models import db, Job
from ingestion.extractors.adzuna import fetch_adzuna_jobs
from ingestion.extractors.scraper import scrape_careers24

# Setup basic logging to see what's happening in Render logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_etl():
    """
    Main function to run the ETL (Extract, Transform, Load) pipeline.
    This can be called by a script OR by the website button.
    """
    logger.info("--- Starting ETL Pipeline ---")

    # 1. EXTRACT
    # Fetch from both sources
    adzuna_jobs = fetch_adzuna_jobs()
    careers24_jobs = scrape_careers24()
    
    # Combine results
    all_raw_jobs = adzuna_jobs + careers24_jobs
    logger.info(f"--- Extracted {len(all_raw_jobs)} total jobs. Starting Deduplication... ---")

    # 2. TRANSFORM & LOAD
    new_count = 0
    
    for job_data in all_raw_jobs:
        # Check if job exists (Deduplication)
        # We check Source + SourceID to be unique
        exists = Job.query.filter_by(
            source=job_data['source'], 
            source_job_id=job_data['source_job_id']
        ).first()
        
        if not exists:
            try:
                new_job = Job(**job_data)
                db.session.add(new_job)
                new_count += 1
            except Exception as e:
                logger.error(f"Failed to add job {job_data.get('title')}: {e}")

    # Commit all changes to Database
    try:
        db.session.commit()
        logger.info(f"--- Success! Committed {new_count} new jobs to the database. ---")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Database Commit Failed: {e}")

    return new_count

if __name__ == "__main__":
    # If running manually from terminal: python ingestion/pipeline.py
    # We need to create an app context to access the DB
    from app import create_app
    app = create_app()
    with app.app_context():
        run_etl()