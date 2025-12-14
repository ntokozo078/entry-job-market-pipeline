# ingestion/utils.py
import re
from datetime import datetime, timedelta

def is_title_outdated(text):
    """
    Returns True if the title contains an old year (e.g., 'Graduate Programme 2017').
    Logic: If year found is more than 1 year in the past, it's outdated.
    """
    if not text:
        return False
        
    current_year = datetime.now().year
    cutoff_year = current_year - 1 # Allow last year (for late postings), but nothing older
    
    # Find all 4-digit numbers starting with "20" (e.g., 2012, 2023)
    found_years = re.findall(r'20\d{2}', text)
    
    for year_str in found_years:
        year = int(year_str)
        # If title says 2017 and we are in 2025 -> Reject
        if year < cutoff_year:
            return True
            
    return False

def clean_text(text):
    if not text: return None
    return re.sub(r'\s+', ' ', text).strip()

def parse_relative_date(date_text):
    """
    Parses '2 days ago', 'Today', '30 June 2017', etc.
    """
    if not date_text: return datetime.utcnow().date()
    
    text = date_text.lower()
    today = datetime.utcnow().date()
    
    # Handle "Today", "Yesterday", "Hours ago"
    if 'today' in text or 'hours' in text or 'minutes' in text: return today
    if 'yesterday' in text: return today - timedelta(days=1)
    
    # Handle explicit dates (e.g., "30 June 2017" or "2025-11-09")
    try:
        # Try parsing standard formats
        return datetime.strptime(date_text, '%d %B %Y').date()
    except ValueError:
        pass

    try:
        return datetime.strptime(date_text, '%Y-%m-%d').date()
    except ValueError:
        pass

    # Handle "30+ days ago"
    match = re.search(r'(\d+)', text)
    if match and 'ago' in text:
        days_ago = int(match.group(1))
        return today - timedelta(days=days_ago)

    return today

def is_date_valid(date_obj, max_age_days=60):
    """
    Returns False if the date is in the past (expired) OR too old.
    """
    if not date_obj: return False
    
    today = datetime.utcnow().date()
    
    # RULE 1: If it's in the future (e.g. Closing Date), it's VALID.
    if date_obj >= today:
        return True
        
    # RULE 2: If it's in the past, is it RECENT? (Posted within last 60 days)
    delta = today - date_obj
    if delta.days > max_age_days:
        return False # Too old (e.g. 2017)
        
    return True