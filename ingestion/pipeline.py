import logging
from datetime import datetime, timedelta
from app.models import db, Job
from ingestion.extractors.adzuna import fetch_adzuna_jobs
from ingestion.extractors.scraper import scrape_careers24

# Setup basic logging to see what's happening in Render logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_old_jobs(max_jobs=1500, max_days=30):
    """
    Data Retention Policy:
    Deletes jobs older than 30 days, or enforces a hard limit of 1500 rows
    to keep the free tier database fast and prevent memory crashes.
    """
    logger.info("--- Starting Database Cleanup ---")
    
    try:
        # 1. Delete jobs older than 30 days
        cutoff_date = datetime.utcnow() - timedelta(days=max_days)
        
        # Find them first so we can log how many we delete
        old_jobs = Job.query.filter(Job.posted_date < cutoff_date).all()
        deleted_by_date = len(old_jobs)
        
        for job in old_jobs:
            db.session.delete(job)
        
        # 2. Enforce the 1500 hard limit
        # Subtract the ones we just queued for deletion to get the real count
        total_jobs = Job.query.count() - deleted_by_date 
        deleted_by_limit = 0
        
        if total_jobs > max_jobs:
            excess_count = total_jobs - max_jobs
            # Find the IDs of the absolute oldest jobs remaining
            oldest_jobs = Job.query.order_by(Job.posted_date.asc()).limit(excess_count).all()
            for job in oldest_jobs:
                db.session.delete(job)
            deleted_by_limit = len(oldest_jobs)

        # Commit all deletions at once
        db.session.commit()
        
        total_deleted = deleted_by_date + deleted_by_limit
        if total_deleted > 0:
            logger.info(f"🧹 Cleanup finished: Deleted {total_deleted} old/excess jobs.")
        else:
            logger.info("🧹 Cleanup finished: Database is healthy, no jobs deleted.")
            
    except Exception as e:
        db.session.rollback()
        logger.error(f"!!! Cleanup Failed: {e}")


def run_etl():
    """
    Main function to run the ETL (Extract, Transform, Load) pipeline.
    """
    logger.info("--- Starting ETL Pipeline ---")

    # 1. EXTRACT
    adzuna_jobs = []
    careers24_jobs = []
    
    # We use try/except here so if one scraper fails, the other still runs
    try:
        adzuna_jobs = fetch_adzuna_jobs()
    except Exception as e:
        logger.error(f"Adzuna extraction failed: {e}")
        
    try:
        careers24_jobs = scrape_careers24()
    except Exception as e:
        logger.error(f"Careers24 extraction failed: {e}")
    
    # Combine results
    all_raw_jobs = adzuna_jobs + careers24_jobs
    logger.info(f"--- Extracted {len(all_raw_jobs)} total jobs. Starting Deduplication... ---")

    # 2. TRANSFORM & LOAD
    new_count = 0
    
    for job_data in all_raw_jobs:
        # Check if job exists (Deduplication by Source + SourceID)
        exists = Job.query.filter_by(
            source=job_data.get('source'), 
            source_job_id=job_data.get('source_job_id')
        ).first()
        
        if not exists:
            try:
                new_job = Job(**job_data)
                db.session.add(new_job)
                new_count += 1
            except Exception as e:
                logger.error(f"Failed to prep job {job_data.get('title', 'Unknown')}: {e}")

    # Commit all new jobs to Database
    try:
        db.session.commit()
        logger.info(f"--- Success! Committed {new_count} new jobs to the database. ---")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Database Commit Failed: {e}")

    # 3. CLEANUP (Run our new retention policy)
    cleanup_old_jobs(max_jobs=1500, max_days=30)

    return new_count

if __name__ == "__main__":
    # If running manually from terminal: python ingestion/pipeline.py
    from app import create_app
    app = create_app()
    with app.app_context():
        run_etl()


    