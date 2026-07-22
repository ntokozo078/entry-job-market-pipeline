import requests
import os
import time
import logging
from datetime import datetime, date
from ingestion.utils import is_title_outdated

logger = logging.getLogger(__name__)

ADZUNA_APP_ID = os.environ.get('ADZUNA_APP_ID')
ADZUNA_APP_KEY = os.environ.get('ADZUNA_APP_KEY')

# ---------------------------------------------------------------------------
# GHOST JOB FIX: Only fetch jobs posted in the last 7 days.
# Adzuna redirect_url links expire — fetching only fresh listings prevents
# "Cannot find page" errors when users click Apply.
# ---------------------------------------------------------------------------
MAX_DAYS_OLD_SA     = 7    # SA jobs: only truly fresh listings
MAX_DAYS_OLD_GLOBAL = 14   # Global: slightly wider window

# ---------------------------------------------------------------------------
# SA SEARCH TERMS — broad IT/IS/CS/ICT coverage + junior/graduate focus
# ---------------------------------------------------------------------------
SA_SEARCH_TERMS = [
    # ── Junior Developer roles (most requested) ──────────────────────────
    'junior developer',
    'junior software developer',
    'junior java developer',
    'junior python developer',
    'junior web developer',
    'junior frontend developer',
    'junior backend developer',
    'junior full stack developer',
    'junior .net developer',
    'junior php developer',
    'junior mobile developer',
    'junior android developer',
    'junior ios developer',
    'entry level developer',
    'entry level software engineer',

    # ── Graduate programmes — IT / ICT / CS / IS ─────────────────────────
    'it graduate',
    'ict graduate',
    'cs graduate',
    'information technology graduate',
    'computer science graduate',
    'information systems graduate',
    'information systems graduate programme',
    'it graduate programme',
    'ict graduate programme',
    'technology graduate programme',
    'software engineering graduate',
    'graduate software developer',
    'graduate it programme',
    'graduate trainee it',
    'graduate trainee technology',
    'bsc computer science graduate',
    'bcom information systems graduate',

    # ── Internships ───────────────────────────────────────────────────────
    'it internship',
    'software development internship',
    'software intern',
    'developer intern',
    'data intern',
    'technology internship',
    'ict internship',
    'computer science intern',
    'information systems intern',
    'web developer intern',
    'java intern',
    'python intern',

    # ── Data & Analytics ─────────────────────────────────────────────────
    'junior data analyst',
    'junior data engineer',
    'junior business intelligence',
    'graduate data analyst',
    'data analyst graduate',
    'data analytics graduate',
    'junior sql developer',
    'junior database administrator',
    'junior etl developer',

    # ── Cloud & DevOps ────────────────────────────────────────────────────
    'junior cloud engineer',
    'junior devops engineer',
    'cloud graduate',
    'aws graduate',
    'azure graduate',

    # ── Cybersecurity / InfoSec ───────────────────────────────────────────
    'junior cyber security',
    'junior security analyst',
    'cyber security graduate',
    'information security graduate',
    'graduate security analyst',

    # ── Business/Systems Analysis ─────────────────────────────────────────
    'junior business analyst',
    'junior systems analyst',
    'graduate business analyst',
    'it business analyst graduate',

    # ── QA & Testing ─────────────────────────────────────────────────────
    'junior qa engineer',
    'junior software tester',
    'graduate qa',

    # ── General tech / helpdesk ───────────────────────────────────────────
    'junior it technician',
    'it support graduate',
    'helpdesk graduate',
    'junior network engineer',
    'junior systems administrator',
    'graduate programme technology',
]

# ---------------------------------------------------------------------------
# GLOBAL SEARCH TERMS
# ---------------------------------------------------------------------------
GLOBAL_COUNTRIES = ['gb', 'us', 'au', 'de', 'nl', 'ca']
GLOBAL_SEARCH_TERMS = [
    'data engineer intern',
    'data engineer entry level',
    'junior data engineer',
    'associate data engineer',
    'sql developer intern',
    'junior sql developer',
    'etl developer intern',
    'junior etl developer',
    'analytics engineer intern',
    'data warehouse intern',
    'data internship',
    'data trainee',
    'junior software developer remote',
    'junior developer remote',
    'entry level data analyst remote',
]

ENTRY_LEVEL_KEYWORDS = [
    'intern', 'graduate', 'junior', 'entry', 'trainee',
    'apprentice', 'associate', '0-2 years', 'no experience',
    'grad', 'learnership',
]

SENIOR_KEYWORDS = [
    'senior', 'lead', 'manager', 'principal', 'head of',
    'mid-level', 'mid level', 'intermediate', 'experienced',
    '3 years', '4 years', '5 years', '5+', 'sr.', 'architect',
]

# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def fetch_adzuna_jobs():
    if not ADZUNA_APP_ID:
        logger.error("No Adzuna API keys found in environment. Skipping.")
        return []

    all_jobs = []
    seen_ids = set()
    # Increased cap now that retention is 5 months
    MAX_JOBS_PER_RUN = 100

    # ── Part A: South Africa (targeted junior/graduate search) ────────────
    logger.info("  - [SA] Fetching Junior/Graduate IT jobs...")
    for term in SA_SEARCH_TERMS:
        if len(all_jobs) >= MAX_JOBS_PER_RUN:
            break

        results = query_adzuna(country='za', what=term, max_days_old=MAX_DAYS_OLD_SA)
        for item in results:
            title = item.get('title', '')
            if is_title_outdated(title):
                continue

            if is_entry_level(item):
                job = normalize(item, 'adzuna_sa', 'South Africa')
                if job['source_job_id'] not in seen_ids:
                    all_jobs.append(job)
                    seen_ids.add(job['source_job_id'])

        time.sleep(0.2)

    # ── Part B: Global remote ─────────────────────────────────────────────
    logger.info("  - [GLOBAL] Fetching Data Engineering Internships/Entry...")
    for country in GLOBAL_COUNTRIES:
        if len(all_jobs) >= MAX_JOBS_PER_RUN:
            break
        for term in GLOBAL_SEARCH_TERMS:
            if len(all_jobs) >= MAX_JOBS_PER_RUN:
                break

            results = query_adzuna(country=country, what=term, max_days_old=MAX_DAYS_OLD_GLOBAL)
            for item in results:
                title = item.get('title', '')
                if is_title_outdated(title):
                    continue
                if is_entry_level(item):
                    is_remote = is_truly_remote(item)
                    location_tag = f"Remote ({country.upper()})" if is_remote else f"{country.upper()}"
                    job = normalize(item, f'adzuna_{country}', location_tag)
                    if job['source_job_id'] not in seen_ids:
                        all_jobs.append(job)
                        seen_ids.add(job['source_job_id'])
            time.sleep(0.2)

    logger.info(f"  - Total Adzuna Jobs Found: {len(all_jobs)}")
    return all_jobs


def query_adzuna(country, what, max_days_old=7):
    """Makes a single request to the Adzuna API and returns results."""
    try:
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
        params = {
            'app_id': ADZUNA_APP_ID,
            'app_key': ADZUNA_APP_KEY,
            'results_per_page': 10,
            'what': what,
            'content-type': 'application/json',
            'max_days_old': max_days_old,  # GHOST JOB FIX: only fresh listings
            'sort_by': 'date',
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get('results', [])
    except requests.exceptions.Timeout:
        logger.warning(f"Adzuna request timed out: country={country}, term={what}")
        return []
    except requests.exceptions.HTTPError as e:
        logger.warning(f"Adzuna HTTP {e.response.status_code}: country={country}, term={what}")
        return []
    except Exception as e:
        logger.warning(f"Adzuna request failed: country={country}, term={what}: {e}")
        return []


def is_entry_level(item):
    title = item.get('title', '').lower()
    description = item.get('description', '').lower()
    full_text = title + " " + description

    if any(k in full_text for k in SENIOR_KEYWORDS):
        return False
    if any(k in title for k in ENTRY_LEVEL_KEYWORDS):
        return True
    if any(k in description for k in ENTRY_LEVEL_KEYWORDS):
        return True
    return False


def is_truly_remote(item):
    text = (
        item.get('title', '') +
        item.get('description', '') +
        item.get('location', {}).get('display_name', '')
    ).lower()
    return 'remote' in text or 'work from home' in text or 'wfh' in text or 'anywhere' in text


def parse_adzuna_date(item) -> date:
    """
    Parse the actual posted date from the Adzuna API response.
    Falls back to today if the field is missing or malformed.
    Using the real date prevents ghost jobs from appearing "new".
    """
    created = item.get('created', '')
    if created:
        try:
            # Adzuna returns ISO 8601 e.g. "2026-07-20T10:00:00Z"
            return datetime.strptime(created[:10], '%Y-%m-%d').date()
        except ValueError:
            pass
    return datetime.now().date()


def normalize(item, source, location_override):
    """Normalize a raw Adzuna API result into our Job model dict."""
    return {
        'source': source,
        'source_job_id': str(item.get('id')),
        'title': item.get('title'),
        'company': item.get('company', {}).get('display_name', 'Unknown'),
        'location': location_override,
        'url': item.get('redirect_url'),
        'description': item.get('description'),
        'job_type': 'entry_level',
        'posted_date': parse_adzuna_date(item),   # ← REAL posted date now
        'is_active': True,
        'salary_min': item.get('salary_min'),
        'salary_max': item.get('salary_max'),
    }