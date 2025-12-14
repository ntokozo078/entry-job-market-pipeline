import requests
import os
import time
from datetime import datetime
from ingestion.utils import is_title_outdated

ADZUNA_APP_ID = os.environ.get('ADZUNA_APP_ID')
ADZUNA_APP_KEY = os.environ.get('ADZUNA_APP_KEY')

# --- CONFIGURATION ---

# 1. SOUTH AFRICA SEARCH (Broad Tech Scope)
SA_SEARCH_TERMS = [
    # 1. DEVELOPMENT
    'software developer', 'software engineer', 'web developer', 
    'frontend developer', 'backend developer', 'full stack developer',
    'mobile developer', 'java developer', 'python developer',
    
    # 2. DATA
    'data analyst', 'data engineer', 'business intelligence', 
    'data scientist', 'database administrator', 'sql developer',
    
    # 3. INFRASTRUCTURE & CLOUD
    'cloud engineer', 'devops engineer', 'system administrator',
    'network engineer', 'it support', 'technical support',
    
    # 4. SECURITY
    'cyber security', 'security analyst', 'information security',
    
    # 5. BUSINESS & QA
    'business analyst', 'it project manager', 'quality assurance',
    'software tester', 'scrum master',
    
    # 6. GENERAL ENTRY LEVEL
    'graduate program', 'internship', 'it graduate', 'ict graduate'
]

# 2. GLOBAL SEARCH (Strictly Data Engineering & Related 0-2 Years)
# We search SPECIFIC combinations to find internships
GLOBAL_COUNTRIES = ['gb', 'us', 'au', 'de', 'nl', 'ca']
GLOBAL_SEARCH_TERMS = [
    # Explicit Data Engineering
    'data engineer intern', 'data engineer entry level', 
    'junior data engineer', 'associate data engineer',
    
    # SQL & ETL (Entry/Intern)
    'sql developer intern', 'junior sql developer',
    'etl developer intern', 'junior etl developer',
    
    # Data Warehousing & Analytics Engineering
    'analytics engineer intern', 'data warehouse intern',
    
    # General Data Internships (often overlap with DE)
    'data internship', 'data trainee'
]

# FILTERS (The Gatekeepers)
# We accept these keywords
ENTRY_LEVEL_KEYWORDS = [
    'intern', 'graduate', 'junior', 'entry', 'trainee', 
    'apprentice', 'associate', '0-2 years', 'no experience'
]

# We strictly REJECT these keywords to enforce "0-2 years"
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

    # --- PART A: South Africa (Broad Search) ---
    print("  - [SA] Fetching Local IT/CS/IS/Security Jobs...")
    for term in SA_SEARCH_TERMS:
        # Search SA specifically
        results = query_adzuna(country='za', what=term)
        
        for item in results:
            title = item.get('title', '')
            
            # 1. ZOMBIE CHECK
            if is_title_outdated(title):
                continue
            
            # 2. ENTRY LEVEL CHECK (0-2 Years)
            if is_entry_level(item):
                job = normalize(item, 'adzuna_sa', 'South Africa')
                
                if job['source_job_id'] not in seen_ids:
                    all_jobs.append(job)
                    seen_ids.add(job['source_job_id'])
                    
        time.sleep(1) 

    # --- PART B: Global (Remote/Relocation Data Engineering) ---
    print("  - [GLOBAL] Fetching Data Engineering Internships/Entry...")
    for country in GLOBAL_COUNTRIES:
        for term in GLOBAL_SEARCH_TERMS:
            
            # NOTE: We add "remote" to the query to prioritize WFH, 
            # but for internships, we sometimes want to see onsite too if they sponsor.
            # Let's search BROADLY for the role, then tag location.
            results = query_adzuna(country=country, what=term)
            
            for item in results:
                title = item.get('title', '')
                
                # 1. ZOMBIE CHECK
                if is_title_outdated(title):
                    continue

                # 2. ENTRY LEVEL CHECK
                if is_entry_level(item):
                    
                    # 3. REMOTE / HYBRID CHECK
                    # For global, we prefer remote, but if it's a specific "Internship", 
                    # we capture it even if onsite, because people might relocate.
                    is_remote = is_truly_remote(item)
                    location_tag = f"{country.upper()}"
                    if is_remote:
                        location_tag = f"Remote ({country.upper()})"
                    
                    job = normalize(item, f'adzuna_{country}', location_tag)
                    
                    if job['source_job_id'] not in seen_ids:
                        all_jobs.append(job)
                        seen_ids.add(job['source_job_id'])
            
            time.sleep(1)

    print(f"  - Total Jobs Found: {len(all_jobs)}")
    return all_jobs

def query_adzuna(country, what):
    try:
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
        params = {
            'app_id': ADZUNA_APP_ID, 
            'app_key': ADZUNA_APP_KEY,
            'results_per_page': 50, 
            'what': what, 
            'content-type': 'application/json',
            'max_days_old': 30,  # Strict freshness (30 days)
            'sort_by': 'date'
        }
        return requests.get(url, params=params).json().get('results', [])
    except:
        return []

def is_entry_level(item):
    """
    Strict filter for 0-2 years experience.
    """
    title = item.get('title', '').lower()
    description = item.get('description', '').lower()
    full_text = title + " " + description
    
    # 1. IMMEDIATE REJECTION (Senior Roles)
    if any(k in full_text for k in SENIOR_KEYWORDS):
        return False
        
    # 2. MUST HAVE "GREEN FLAG" (Intern, Junior, etc)
    # We prioritize the TITLE for the green flag to ensure relevance
    if any(k in title for k in ENTRY_LEVEL_KEYWORDS):
        return True
        
    # 3. Fallback: If title is neutral (e.g., "Data Engineer"), check description carefully
    if any(k in description for k in ENTRY_LEVEL_KEYWORDS):
        return True

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