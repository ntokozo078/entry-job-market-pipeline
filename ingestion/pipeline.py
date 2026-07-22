import logging
from datetime import datetime, timedelta
from app.models import db, Job
from ingestion.extractors.adzuna import fetch_adzuna_jobs
from ingestion.extractors.scraper import scrape_careers24
from ingestion.extractors.remotive import fetch_remotive_jobs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retention Policy Constants
# ---------------------------------------------------------------------------
DISPLAY_MAX_DAYS = 150   # 5 months  — jobs older than this are marked inactive
DELETE_MAX_DAYS  = 180   # 6 months  — jobs older than this are deleted entirely
HARD_ROW_LIMIT   = 1500  # Maximum rows to keep on Render free tier


def deactivate_old_jobs(max_days: int = DISPLAY_MAX_DAYS) -> int:
    """
    Mark jobs older than `max_days` as inactive so they stop appearing
    in the UI, without immediately deleting them.
    Returns the count of jobs deactivated.
    """
    cutoff = datetime.utcnow() - timedelta(days=max_days)
    count = (
        Job.query
        .filter(Job.is_active == True)
        .filter(Job.posted_date < cutoff.date())
        .update({'is_active': False}, synchronize_session=False)
    )
    if count:
        db.session.commit()
        logger.info(f"🔕 Deactivated {count} jobs older than {max_days} days.")
    return count


def cleanup_old_jobs(max_days: int = DELETE_MAX_DAYS, max_rows: int = HARD_ROW_LIMIT) -> None:
    """
    Data Retention Policy:
    1. Deletes jobs older than `max_days` (6 months) to free storage.
    2. Enforces a hard cap of `max_rows` to keep Render's free-tier DB healthy.

    Commits date-based deletions FIRST so the subsequent count is accurate.
    """
    logger.info("--- Starting Database Cleanup ---")

    try:
        # 1. Delete jobs older than max_days (6 months)
        cutoff = datetime.utcnow() - timedelta(days=max_days)
        deleted_by_date = (
            Job.query
            .filter(Job.posted_date < cutoff.date())
            .delete(synchronize_session=False)
        )
        db.session.commit()

        # 2. Enforce the hard row limit on what remains
        remaining = Job.query.count()
        deleted_by_limit = 0

        if remaining > max_rows:
            excess = remaining - max_rows
            oldest_ids = [
                row.id for row in
                Job.query.order_by(Job.posted_date.asc()).limit(excess).all()
            ]
            deleted_by_limit = (
                Job.query.filter(Job.id.in_(oldest_ids))
                .delete(synchronize_session=False)
            )
            db.session.commit()

        total = deleted_by_date + deleted_by_limit
        if total:
            logger.info(f"🧹 Cleanup done: deleted {deleted_by_date} old + {deleted_by_limit} excess = {total} total.")
        else:
            logger.info("🧹 Cleanup done: database is healthy, nothing deleted.")

    except Exception as e:
        db.session.rollback()
        logger.error(f"!!! Cleanup failed: {e}")


def run_etl() -> int:
    """
    Main ETL (Extract, Transform, Load) pipeline.
    Sources: Adzuna API (SA + Global) · Careers24 scraper · Remotive.io API
    Returns the number of new jobs committed to the database.
    """
    logger.info("=== Starting ETL Pipeline ===")

    # ── 1. EXTRACT ──────────────────────────────────────────────────────────
    adzuna_jobs, careers24_jobs, remotive_jobs = [], [], []

    try:
        adzuna_jobs = fetch_adzuna_jobs()
    except Exception as e:
        logger.error(f"Adzuna extraction failed: {e}")

    try:
        careers24_jobs = scrape_careers24()
    except Exception as e:
        logger.error(f"Careers24 extraction failed: {e}")

    try:
        remotive_jobs = fetch_remotive_jobs()
    except Exception as e:
        logger.error(f"Remotive extraction failed: {e}")

    all_raw_jobs = adzuna_jobs + careers24_jobs + remotive_jobs
    logger.info(
        f"Extracted {len(adzuna_jobs)} Adzuna + "
        f"{len(careers24_jobs)} Careers24 + "
        f"{len(remotive_jobs)} Remotive = {len(all_raw_jobs)} total. "
        f"Starting deduplication..."
    )

    # ── 2. TRANSFORM & LOAD ─────────────────────────────────────────────────
    new_count = 0

    for job_data in all_raw_jobs:
        exists = Job.query.filter_by(
            source=job_data.get('source'),
            source_job_id=job_data.get('source_job_id'),
        ).first()

        if not exists:
            try:
                new_job = Job(**job_data)
                db.session.add(new_job)
                new_count += 1
            except Exception as e:
                logger.error(f"Failed to prepare job '{job_data.get('title', 'Unknown')}': {e}")

    try:
        db.session.commit()
        logger.info(f"✅ Committed {new_count} new jobs to the database.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Database commit failed: {e}")

    # ── 3. DEACTIVATE old jobs (5-month threshold) ──────────────────────────
    deactivate_old_jobs(max_days=DISPLAY_MAX_DAYS)

    # ── 4. DELETE very old jobs + enforce row limit (6-month threshold) ─────
    cleanup_old_jobs(max_days=DELETE_MAX_DAYS, max_rows=HARD_ROW_LIMIT)

    return new_count


if __name__ == "__main__":
    from app import create_app
    app = create_app()
    with app.app_context():
        run_etl()