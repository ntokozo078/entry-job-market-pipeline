# ingestion/extractors/scraper.py
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
from ingestion.utils import clean_text, parse_relative_date, is_date_valid

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

SEARCH_URLS = [
    "https://www.careers24.com/jobs/lc-south-africa/kw-software-developer/?sort=dateposted",
    "https://www.careers24.com/jobs/lc-south-africa/kw-data/?sort=dateposted",
    "https://www.careers24.com/jobs/lc-south-africa/kw-graduate/?sort=dateposted",
    "https://www.careers24.com/jobs/lc-south-africa/kw-intern/?sort=dateposted"
]

def scrape_careers24():
    print("  - Scraping Careers24 (Checking Dates)...")
    all_jobs = []
    seen_ids = set()

    for url in SEARCH_URLS:
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code != 200: continue
            soup = BeautifulSoup(response.text, 'html.parser')
            
            job_cards = soup.find_all('div', class_='job-card') 
            if not job_cards: job_cards = soup.select('.c24-job-card')

            for card in job_cards:
                try:
                    # --- DATE VALIDATION (CRITICAL FIX) ---
                    # 1. Check for "Closing Date" text specifically
                    # Some cards have: "Closing Date: 30 June 2017"
                    date_text = ""
                    closing_date_tag = card.find(string=lambda text: text and "closing date" in text.lower())
                    
                    if closing_date_tag:
                        # Extract the date part "30 June 2017"
                        clean_str = clean_text(closing_date_tag).lower().replace('closing date:', '').strip()
                        job_date = parse_relative_date(clean_str)
                        
                        # IF CLOSING DATE IS PAST -> SKIP
                        if job_date < datetime.utcnow().date():
                            continue 
                            
                    else:
                        # Fallback to "Posted Date"
                        date_tag = card.find('span', class_='job-card-date')
                        date_text = date_tag.text if date_tag else "Today"
                        job_date = parse_relative_date(date_text)
                        
                        # IF POSTED > 60 DAYS AGO -> SKIP
                        if not is_date_valid(job_date, max_age_days=60):
                            continue

                    # --- If we get here, the date is good. Extract the rest. ---
                    title_tag = card.find('h3') or card.find('span', class_='job-card-title')
                    title = clean_text(title_tag.text) if title_tag else "Unknown"
                    
                    # Filter Keywords
                    if 'senior' in title.lower() or 'lead' in title.lower(): continue

                    link_tag = card.find('a')
                    relative_link = link_tag['href'] if link_tag else ""
                    source_id = relative_link.split('-')[-1].replace('/', '')
                    
                    if source_id in seen_ids: continue
                    seen_ids.add(source_id)

                    job = {
                        'source': 'careers24',
                        'source_job_id': source_id,
                        'title': title,
                        'company': clean_text(card.find('span', class_='job-card-company').text) if card.find('span', class_='job-card-company') else "Unknown",
                        'location': clean_text(card.find('span', class_='job-card-location').text) if card.find('span', class_='job-card-location') else "SA",
                        'url': f"https://www.careers24.com{relative_link}",
                        'description': "Apply on Careers24",
                        'job_type': 'entry_level',
                        'posted_date': job_date,
                        'is_active': True
                    }
                    all_jobs.append(job)

                except Exception:
                    continue
            time.sleep(1)

        except Exception as e:
            print(f"Error: {e}")

    print(f"  - Total Valid Careers24 jobs: {len(all_jobs)}")
    return all_jobs