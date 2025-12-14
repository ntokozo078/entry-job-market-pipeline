# ingestion/pipeline.py
from app.models import db, Job
from ingestion.extractors.adzuna import fetch_adzuna_jobs
# from ingestion.extractors.scraper import scrape_site_b (Future)

def run_etl():
    """
    Main orchestration function.
    1. Extract data from all sources
    2. Transform/Normalize
    3. Load into DB (Upsert logic)
    """
    
    # --- STEP 1: EXTRACT ---
    print("Fetching jobs from Adzuna...")
    raw_jobs = fetch_adzuna_jobs() 
    # raw_jobs += scrape_site_b() # Add more sources here later

    # --- STEP 2: TRANSFORM & LOAD (The Upsert Logic) ---
    print(f"Processing {len(raw_jobs)} jobs...")
    
    new_count = 0
    updated_count = 0

    for job_data in raw_jobs:
        # Check if job already exists (Deduplication)
        existing_job = Job.query.filter_by(
            source=job_data['source'], 
            source_job_id=job_data['source_job_id']
        ).first()

        if existing_job:
            # UPDATE: Update last_seen_at so we know it's still active
            existing_job.last_seen_at = db.func.now()
            updated_count += 1
        else:
            # INSERT: Create new record
            new_job = Job(**job_data)
            db.session.add(new_job)
            new_count += 1
    
    # Commit changes to Postgres
    db.session.commit()
    
    print(f"Pipeline Finished. New: {new_count}, Updated: {updated_count}")