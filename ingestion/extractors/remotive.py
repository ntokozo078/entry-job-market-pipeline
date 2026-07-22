"""
ingestion/extractors/remotive.py

Fetches remote tech jobs from Remotive.io — completely FREE, no API key required.
API docs: https://remotive.com/api/remote-jobs

Remotive focuses on remote-first companies globally, which is excellent for
junior/entry-level data and software roles that accept international candidates.
"""
import requests
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

REMOTIVE_API = "https://remotive.com/api/remote-jobs"

# Categories relevant to entry-level tech candidates
CATEGORIES = [
    'software-dev',
    'devops-sysadmin',
    'data',
]

# Keywords to accept (entry-level filter)
ENTRY_KEYWORDS = [
    'junior', 'intern', 'graduate', 'entry', 'trainee', 'associate',
    '0-2', 'no experience', 'entry level', 'new grad',
]

# Keywords that disqualify a role as too senior
SENIOR_KEYWORDS = [
    'senior', 'lead', 'manager', 'principal', 'staff', 'head of',
    'director', 'vp', 'architect', 'experienced', 'mid-level',
]


def fetch_remotive_jobs():
    """
    Pulls entry-level remote jobs from the Remotive API.
    Returns a list of job dicts matching our Job model schema.
    """
    logger.info("  - [REMOTIVE] Fetching remote entry-level tech jobs...")
    all_jobs = []
    seen_ids = set()

    for category in CATEGORIES:
        try:
            response = requests.get(
                REMOTIVE_API,
                params={'category': category, 'limit': 50},
                timeout=12,
            )
            response.raise_for_status()
            jobs_raw = response.json().get('jobs', [])
        except requests.exceptions.Timeout:
            logger.warning(f"Remotive timed out for category={category}")
            continue
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Remotive HTTP error {e.response.status_code} for category={category}")
            continue
        except Exception as e:
            logger.warning(f"Remotive request failed for category={category}: {e}")
            continue

        for item in jobs_raw:
            job_id = str(item.get('id', ''))
            if not job_id or job_id in seen_ids:
                continue

            title = item.get('title', '')
            description = item.get('description', '')
            combined = (title + ' ' + description).lower()

            # Apply entry-level + senior filters
            if any(k in combined for k in SENIOR_KEYWORDS):
                continue
            if not any(k in combined for k in ENTRY_KEYWORDS):
                continue

            job = normalize_remotive(item)
            if job:
                all_jobs.append(job)
                seen_ids.add(job_id)

    logger.info(f"  - Total Remotive Jobs Found: {len(all_jobs)}")
    return all_jobs


def parse_remotive_date(date_str: str) -> date:
    """Parse Remotive's publication date string to a date object."""
    if not date_str:
        return datetime.now().date()
    try:
        # Remotive returns e.g. "2026-07-15T14:00:00" or "2026-07-15"
        return datetime.strptime(date_str[:10], '%Y-%m-%d').date()
    except ValueError:
        return datetime.now().date()


def normalize_remotive(item) -> dict | None:
    """Normalize a Remotive API result into our Job model schema."""
    url = item.get('url', '')
    if not url:
        return None

    company = item.get('company_name', 'Unknown')
    # Remotive has candidate_required_location for where you can work from
    location_hint = item.get('candidate_required_location', 'Remote (Global)')
    if not location_hint:
        location_hint = 'Remote (Global)'

    return {
        'source': 'remotive',
        'source_job_id': str(item.get('id')),
        'title': item.get('title'),
        'company': company,
        'location': f"Remote — {location_hint}",
        'url': url,
        'description': item.get('description', '')[:500],  # cap description size
        'job_type': 'entry_level',
        'posted_date': parse_remotive_date(item.get('publication_date', '')),
        'is_active': True,
        'salary_min': None,
        'salary_max': None,
    }
