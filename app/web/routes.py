# app/web/routes.py
from flask import Blueprint, render_template, request
from sqlalchemy import func
from app.models import db, Job

web_bp = Blueprint('web', __name__)

# --- EXISTING ROUTES (Keep these) ---
@web_bp.route('/')
def index():
    search_query = request.args.get('q', '')
    query = Job.query.filter(Job.is_active==True).filter(
        (Job.source == 'adzuna_sa') | (Job.source == 'careers24')
    )
    if search_query:
        query = query.filter(Job.title.ilike(f'%{search_query}%'))
    jobs = query.order_by(Job.posted_date.desc()).limit(50).all()
    return render_template('index.html', jobs=jobs, search_query=search_query, page_title="🇿🇦 SA Tech Jobs")

@web_bp.route('/global')
def global_jobs():
    search_query = request.args.get('q', '')
    query = Job.query.filter(Job.is_active==True).filter(
        (Job.source != 'adzuna_sa') & (Job.source != 'careers24')
    )
    if search_query:
        query = query.filter(Job.title.ilike(f'%{search_query}%'))
    jobs = query.order_by(Job.posted_date.desc()).limit(50).all()
    return render_template('index.html', jobs=jobs, search_query=search_query, page_title="🌍 Global Remote Data Jobs")

# --- NEW INTERACTIVE STATS ROUTE ---
@web_bp.route('/stats')
def stats():
    """
    Dashboard with data for Charts.js
    """
    # 1. High Level Numbers
    total_jobs = Job.query.count()
    active_jobs = Job.query.filter_by(is_active=True).count()
    
    # 2. Jobs by Source (for Pie Chart)
    # SQL: SELECT source, COUNT(*) FROM jobs GROUP BY source
    source_data = db.session.query(Job.source, func.count(Job.id))\
        .group_by(Job.source).all()
    
    # 3. Top Locations (for Bar Chart)
    # SQL: SELECT location, COUNT(*) ... ORDER BY count DESC LIMIT 5
    location_data = db.session.query(Job.location, func.count(Job.id))\
        .group_by(Job.location)\
        .order_by(func.count(Job.id).desc())\
        .limit(8).all()
    
    # 4. "Market Demand" (Python Keyword Search)
    # Since SQL 'LIKE' is slow for many keywords, we do a quick scan here (dataset is small)
    all_titles = [j.title.lower() for j in Job.query.filter_by(is_active=True).all()]
    
    skills_to_track = ['python', 'sql', 'java', 'aws', 'azure', 'react', 'data engineer', 'analyst']
    skill_counts = {}
    
    for skill in skills_to_track:
        count = sum(1 for t in all_titles if skill in t)
        if count > 0:
            skill_counts[skill.title()] = count

    return render_template(
        'stats.html',
        total_jobs=total_jobs,
        active_jobs=active_jobs,
        source_labels=[s[0] for s in source_data],
        source_values=[s[1] for s in source_data],
        loc_labels=[l[0] for l in location_data],
        loc_values=[l[1] for l in location_data],
        skill_labels=list(skill_counts.keys()),
        skill_values=list(skill_counts.values())
    )