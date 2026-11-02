import logging
from collections import Counter
from datetime import timedelta
from app.models import db, Job, utc_now
from ingestion.extractors.adzuna import fetch_adzuna_jobs
from ingestion.extractors.scraper import scrape_careers24
from ingestion.extractors.remotive import fetch_remotive_jobs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Retention Policy Constants
DISPLAY_MAX_DAYS = 150   # 5 months  — jobs older than this are marked inactive
DELETE_MAX_DAYS  = 180   # 6 months  — jobs older than this are deleted entirely
HARD_ROW_LIMIT   = 1500  # Maximum rows to keep on Render free tier
NOT_SEEN_MAX_DAYS = 30   # jobs missing from their source's results this long are marked inactive


def deactivate_unseen_jobs(sources: set, max_days: int = NOT_SEEN_MAX_DAYS) -> int:
    """
    Mark jobs inactive when their source has stopped returning them.
    Only `sources` are checked: pass the ones that returned jobs in this run,
    so an outage at one source doesn't hide all of its listings.
    Absence from search results is a signal, not proof the job has closed.
    Returns the count of jobs deactivated.
    """
    if not sources:
        return 0

    cutoff = utc_now() - timedelta(days=max_days)
    count = (
        Job.query
        .filter(Job.is_active == True)
        .filter(Job.source.in_(sources))
        .filter(Job.last_seen_at < cutoff)
        .update({'is_active': False}, synchronize_session=False)
    )
    if count:
        db.session.commit()
        logger.info(f"Deactivated {count} jobs not seen for {max_days} days.")
    return count


def deactivate_old_jobs(max_days: int = DISPLAY_MAX_DAYS) -> int:
    """
    Mark jobs older than `max_days` as inactive so they stop appearing
    in the UI, without immediately deleting them.
    Returns the count of jobs deactivated.
    """
    cutoff = utc_now() - timedelta(days=max_days)
    count = (
        Job.query
        .filter(Job.is_active == True)
        .filter(Job.posted_date < cutoff.date())
        .update({'is_active': False}, synchronize_session=False)
    )
    if count:
        db.session.commit()
        logger.info(f"Deactivated {count} jobs older than {max_days} days.")
    return count


def cleanup_old_jobs(max_days: int = DELETE_MAX_DAYS, max_rows: int = HARD_ROW_LIMIT) -> int:
    """
    Data Retention Policy:
    1. Deletes jobs older than `max_days` (6 months) to free storage.
    2. Enforces a hard cap of `max_rows` to keep Render's free-tier DB healthy.

    Commits date-based deletions FIRST so the subsequent count is accurate.
    Returns the count of jobs deleted.
    """
    logger.info("--- Starting Database Cleanup ---")

    try:
        # 1. Delete jobs older than max_days (6 months)
        cutoff = utc_now() - timedelta(days=max_days)
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
            logger.info(f"Cleanup done: deleted {deleted_by_date} old + {deleted_by_limit} excess = {total} total.")
        else:
            logger.info("Cleanup done: nothing deleted.")
        return total

    except Exception as e:
        db.session.rollback()
        logger.error(f"!!! Cleanup failed: {e}")
        return 0


def load_jobs(raw_jobs: list) -> tuple[int, int]:
    """
    Upsert on (source, source_job_id). New jobs are inserted. Existing jobs get
    their fields overwritten with the latest values and `last_seen_at` refreshed;
    `first_seen_at` is kept. Returns (new_count, updated_count).
    """
    now = utc_now()
    new_count = updated_count = 0

    for job_data in raw_jobs:
        existing = Job.query.filter_by(
            source=job_data.get('source'),
            source_job_id=job_data.get('source_job_id'),
        ).first()

        try:
            if existing:
                for field, value in job_data.items():
                    setattr(existing, field, value)
                existing.is_active = True
                existing.last_seen_at = now
                updated_count += 1
            else:
                db.session.add(Job(**job_data, first_seen_at=now, last_seen_at=now))
                new_count += 1
        except Exception as e:
            logger.error(f"Failed to prepare job '{job_data.get('title', 'Unknown')}': {e}")

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    logger.info(f"Committed {new_count} new and {updated_count} updated jobs.")
    return new_count, updated_count


def run_etl() -> dict:
    """
    Main ETL (Extract, Transform, Load) pipeline.
    Sources: Adzuna API (SA + Global) · Careers24 scraper · Remotive.io API
    Returns a summary of what the run extracted and changed.
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

    # ── 2. LOAD ─────────────────────────────────────────────────────────────
    new_count, updated_count = load_jobs(all_raw_jobs)
    extracted = Counter(job['source'] for job in all_raw_jobs)

    # ── 3. DEACTIVATE jobs the source stopped returning, then old jobs ──────
    deactivated_unseen = deactivate_unseen_jobs(set(extracted))
    deactivated_old = deactivate_old_jobs(max_days=DISPLAY_MAX_DAYS)

    # ── 4. DELETE very old jobs + enforce row limit (6-month threshold) ─────
    deleted = cleanup_old_jobs(max_days=DELETE_MAX_DAYS, max_rows=HARD_ROW_LIMIT)

    summary = {
        'extracted': dict(extracted),
        'new': new_count,
        'updated': updated_count,
        'deactivated_unseen': deactivated_unseen,
        'deactivated_old': deactivated_old,
        'deleted': deleted,
        'rows_after': Job.query.count(),
        'active_after': Job.query.filter_by(is_active=True).count(),
    }
    logger.info(f"Run summary: {summary}")
    return summary


if __name__ == "__main__":
    from app import create_app
    app = create_app()
    with app.app_context():
        run_etl()