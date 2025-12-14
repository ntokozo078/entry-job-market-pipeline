from flask import Blueprint, render_template, request, flash, redirect, url_for
from sqlalchemy import func
from app.models import db, Job
from ingestion.pipeline import run_etl  # <--- Import the pipeline function

web_bp = Blueprint('web', __name__)

# --- 1. STANDARD ROUTES (Home, Global, Stats) ---

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

# --- 2. THE FIXED REFRESH ROUTE (Better Way) ---

@web_bp.route('/refresh', methods=['POST'])
def refresh_data():
    """
    Runs the pipeline SYNCHRONOUSLY.
    The page will wait (spin) until it finishes, then report success/error.
    """
    print(">>> Manual Refresh Triggered (Sync Mode)")
    
    try:
        # 1. Run the pipeline directly. 
        # This blocks the code, preventing the 'Context Error' from before.
        new_jobs = run_etl()
        
        # 2. Give feedback based on result
        if new_jobs > 0:
            flash(f"✅ Success! Pipeline finished and found {new_jobs} new jobs.", "success")
        else:
            flash("⚠️ Pipeline finished successfully, but found 0 new jobs.", "warning")
            
    except Exception as e:
        # 3. Catch errors and show them to the user
        print(f"!!! Pipeline Failed: {e}")
        flash(f"❌ Error running pipeline: {str(e)}", "danger")

    return redirect(url_for('web.index'))