# app/web/routes.py
import threading
from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app
from sqlalchemy import func
from app.models import db, Job
from ingestion.pipeline import run_etl  # <--- Import our new function

web_bp = Blueprint('web', __name__)

# --- HELPER FUNCTION FOR THREADING ---
def run_background_pipeline(app_context):
    """
    Runs the pipeline in a separate thread.
    We must pass the 'app_context' so the thread can find the Database.
    """
    with app_context:
        try:
            print(">>> Background Pipeline Triggered!")
            run_etl()
            print(">>> Background Pipeline Finished!")
        except Exception as e:
            print(f"!!! Background Pipeline Failed: {e}")

# --- ROUTES ---

@web_bp.route('/')
def index():
    """Home: SA Jobs ONLY"""
    search_query = request.args.get('q', '')
    
    # Filter: Source must be 'adzuna_sa' or 'careers24'
    query = Job.query.filter(Job.is_active==True).filter(
        (Job.source == 'adzuna_sa') | (Job.source == 'careers24')
    )
    
    if search_query:
        query = query.filter(Job.title.ilike(f'%{search_query}%'))
    
    jobs = query.order_by(Job.posted_date.desc()).limit(50).all()
    
    return render_template('index.html', jobs=jobs, search_query=search_query, page_title="🇿🇦 SA Tech Jobs")

@web_bp.route('/global')
def global_jobs():
    """Global: Remote Data Engineering ONLY"""
    search_query = request.args.get('q', '')
    
    # Filter: Source is NOT SA
    query = Job.query.filter(Job.is_active==True).filter(
        (Job.source != 'adzuna_sa') & (Job.source != 'careers24')
    )
    
    if search_query:
        query = query.filter(Job.title.ilike(f'%{search_query}%'))

    jobs = query.order_by(Job.posted_date.desc()).limit(50).all()
    
    return render_template('index.html', jobs=jobs, search_query=search_query, page_title="🌍 Global Remote Data Jobs")

@web_bp.route('/stats')
def stats():
    """Dashboard Metrics"""
    total_jobs = Job.query.count()
    active_jobs = Job.query.filter_by(is_active=True).count()
    
    source_data = db.session.query(Job.source, func.count(Job.id)).group_by(Job.source).all()
    
    location_data = db.session.query(Job.location, func.count(Job.id))\
        .group_by(Job.location)\
        .order_by(func.count(Job.id).desc())\
        .limit(8).all()
    
    # Keyword Skill Scan
    all_titles = [j.title.lower() for j in Job.query.filter_by(is_active=True).all()]
    skills_to_track = ['python', 'sql', 'java', 'aws', 'azure', 'react', 'data engineer', 'analyst', 'cyber', 'intern']
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

@web_bp.route('/refresh', methods=['POST'])
def refresh_data():
    """
    The Manual Button Trigger.
    Starts the ETL pipeline in a background thread.
    """
    # 1. Get the real app object to pass to the thread
    app = current_app._get_current_object()
    
    # 2. Start the thread
    thread = threading.Thread(target=run_background_pipeline, args=(app,))
    thread.start()
    
    # 3. Tell user it started
    flash("🔄 Pipeline started! It runs in the background. Refresh this page in 1 minute to see results.", "success")
    return redirect(url_for('web.index'))