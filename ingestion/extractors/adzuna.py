import requests
import os
import time
from datetime import datetime
from ingestion.utils import is_title_outdated

ADZUNA_APP_ID = os.environ.get('ADZUNA_APP_ID')
ADZUNA_APP_KEY = os.environ.get('ADZUNA_APP_KEY')

# --- CONFIGURATION ---
SA_SEARCH_TERMS = [
    'software developer', 'software engineer', 'web developer', 
    'frontend developer', 'backend developer', 'full stack developer',
    'mobile developer', 'java developer', 'python developer',
    'data analyst', 'data engineer', 'business intelligence', 
    'data scientist', 'database administrator', 'sql developer',
    'cloud engineer', 'devops engineer', 'system administrator',
    'network engineer', 'it support', 'technical support',
    'cyber security', 'security analyst', 'information security',
    'business analyst', 'it project manager', 'quality assurance',
    'software tester', 'scrum master',
    'graduate program', 'internship', 'it graduate', 'ict graduate'
]

GLOBAL_COUNTRIES = ['gb', 'us', 'au', 'de', 'nl', 'ca']
GLOBAL_SEARCH_TERMS = [
    'data engineer intern', 'data engineer entry level', 
    'junior data engineer', 'associate data engineer',
    'sql developer intern', 'junior sql developer',
    'etl developer intern', 'junior etl developer',
    'analytics engineer intern', 'data warehouse intern',
    'data internship', 'data trainee'
]

ENTRY_LEVEL_KEYWORDS = [
    'intern', 'graduate', 'junior', 'entry', 'trainee', 
    'apprentice', 'associate', '0-2 years', 'no experience'
]

SENIOR_KEYWORDS = [
    'senior', 'lead', 'manager', 'principal', 'head of', 
    'mid-level', 'mid level', 'intermediate', 'experienced',
    '3 years', '4 years', '5 years', '5+', 'sr.'
]

def fetch_adzuna_jobs():
    if not ADZUNA_APP_ID:
        print("Error: No API Keys found.")
        return []

    all_jobs = []
    seen_ids = set() 
    MAX_JOBS_PER_RUN = 50  # Circuit Breaker: Stop after finding this many to save memory

    # --- PART A: South Africa (Broad Search) ---
    print("  - [SA] Fetching Local IT/CS/IS/Security Jobs...")
    for term in SA_SEARCH_TERMS:
        if len(all_jobs) >= MAX_JOBS_PER_RUN: break # Stop if we have enough
        
        results = query_adzuna(country='za', what=term)
        for item in results:
            title = item.get('title', '')
            if is_title_outdated(title): continue
            
            if is_entry_level(item):
                job = normalize(item, 'adzuna_sa', 'South Africa')
                if job['source_job_id'] not in seen_ids:
                    all_jobs.append(job)
                    seen_ids.add(job['source_job_id'])
                    
        time.sleep(0.2) # Faster sleep to prevent timeout

    # --- PART B: Global ---
    print("  - [GLOBAL] Fetching Data Engineering Internships/Entry...")
    for country in GLOBAL_COUNTRIES:
        if len(all_jobs) >= MAX_JOBS_PER_RUN: break # Stop if we have enough
        
        for term in GLOBAL_SEARCH_TERMS:
            if len(all_jobs) >= MAX_JOBS_PER_RUN: break
            
            results = query_adzuna(country=country, what=term)
            for item in results:
                title = item.get('title', '')
                if is_title_outdated(title): continue

                if is_entry_level(item):
                    is_remote = is_truly_remote(item)
                    location_tag = f"Remote ({country.upper()})" if is_remote else f"{country.upper()}"
                    job = normalize(item, f'adzuna_{country}', location_tag)
                    
                    if job['source_job_id'] not in seen_ids:
                        all_jobs.append(job)
                        seen_ids.add(job['source_job_id'])
            time.sleep(0.2)

    print(f"  - Total Jobs Found: {len(all_jobs)}")
    return all_jobs

def query_adzuna(country, what):
    try:
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
        params = {
            'app_id': ADZUNA_APP_ID, 
            'app_key': ADZUNA_APP_KEY,
            'results_per_page': 10,  # Lowered from 50 to save massive memory
            'what': what, 
            'content-type': 'application/json',
            'max_days_old': 30,
            'sort_by': 'date'
        }
        return requests.get(url, params=params, timeout=10).json().get('results', [])
    except:
        return []

def is_entry_level(item):
    title = item.get('title', '').lower()
    description = item.get('description', '').lower()
    full_text = title + " " + description
    
    if any(k in full_text for k in SENIOR_KEYWORDS): return False
    if any(k in title for k in ENTRY_LEVEL_KEYWORDS): return True
    if any(k in description for k in ENTRY_LEVEL_KEYWORDS): return True
    return False

def is_truly_remote(item):
    text = (item.get('title', '') + item.get('description', '') + item.get('location', {}).get('display_name', '')).lower()
    return 'remote' in text or 'work from home' in text or 'wfh' in text or 'anywhere' in text

def normalize(item, source, location_override):
    return {
        'source': source,
        'source_job_id': str(item.get('id')),
        'title': item.get('title'),
        'company': item.get('company', {}).get('display_name', 'Unknown'),
        'location': location_override,
        'url': item.get('redirect_url'),
        'description': item.get('description'),
        'job_type': 'entry_level',
        'posted_date': datetime.now().date(),
        'is_active': True
    }