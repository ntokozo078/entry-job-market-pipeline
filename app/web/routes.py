import threading
from datetime import datetime, timedelta, date
from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify
from sqlalchemy import func
from app.models import db, Job
from ingestion.pipeline import run_etl, DISPLAY_MAX_DAYS

web_bp = Blueprint('web', __name__)

# Track pipeline state across requests
_pipeline_state = {
    'running': False,
    'last_run': None,
    'last_result': None,
}



def _active_cutoff() -> date:
    """Jobs older than this are considered inactive/ghost — not shown."""
    return (datetime.utcnow() - timedelta(days=DISPLAY_MAX_DAYS)).date()

def _get_category_counts(base_query):
    """Calculate job counts for the sidebar categories based on the current base query."""
    return {
        'all': base_query.count(),
        'junior_dev': base_query.filter(Job.title.ilike('%junior developer%')).count(),
        'graduate': base_query.filter(Job.title.ilike('%graduate%')).count(),
        'intern': base_query.filter(Job.title.ilike('%intern%')).count(),
        'data': base_query.filter(Job.title.ilike('%data%')).count(),
        'cyber': base_query.filter(Job.title.ilike('%cyber%')).count(),
        'cloud': base_query.filter(Job.title.ilike('%cloud%')).count(),
        'ict_grad': base_query.filter(Job.title.ilike('%ict%')).count(),
        'is_grad': base_query.filter(Job.title.ilike('%information systems%')).count(),
    }


# ---------------------------------------------------------------------------
# 1. Standard page routes
# ---------------------------------------------------------------------------

@web_bp.route('/')
def index():
    """Home: SA Jobs ONLY — last 5 months, no ghost jobs"""
    search_query = request.args.get('q', '')
    page = request.args.get('page', 1, type=int)
    per_page = 20
    cutoff = _active_cutoff()

    query = (
        Job.query
        .filter(Job.is_active == True)
        .filter(Job.posted_date >= cutoff)         # ← 5-month freshness filter
        .filter(
            (Job.source == 'adzuna_sa') | (Job.source == 'careers24')
        )
    )

    counts = _get_category_counts(query)

    if search_query:
        query = query.filter(Job.title.ilike(f'%{search_query}%'))

    pagination = query.order_by(Job.posted_date.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )

    return render_template(
        'index.html',
        jobs=pagination.items,
        pagination=pagination,
        search_query=search_query,
        page_title="🇿🇦 SA Tech Jobs",
        category_counts=counts,
    )


@web_bp.route('/global')
def global_jobs():
    """Global: Remote data/tech jobs — last 5 months"""
    search_query = request.args.get('q', '')
    page = request.args.get('page', 1, type=int)
    per_page = 20
    cutoff = _active_cutoff()

    query = (
        Job.query
        .filter(Job.is_active == True)
        .filter(Job.posted_date >= cutoff)         # ← 5-month freshness filter
        .filter(
            (Job.source != 'adzuna_sa') & (Job.source != 'careers24')
        )
    )

    counts = _get_category_counts(query)

    if search_query:
        query = query.filter(Job.title.ilike(f'%{search_query}%'))

    pagination = query.order_by(Job.posted_date.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )

    return render_template(
        'index.html',
        jobs=pagination.items,
        pagination=pagination,
        search_query=search_query,
        page_title="🌍 Global Remote Data Jobs",
        category_counts=counts,
    )


@web_bp.route('/stats')
def stats():
    """Analytics Dashboard — metrics for active + fresh jobs"""
    cutoff = _active_cutoff()
    total_jobs = Job.query.count()
    active_jobs = (
        Job.query
        .filter(Job.is_active == True)
        .filter(Job.posted_date >= cutoff)
        .count()
    )

    source_data = (
        db.session.query(Job.source, func.count(Job.id))
        .filter(Job.is_active == True)
        .filter(Job.posted_date >= cutoff)
        .group_by(Job.source)
        .all()
    )

    location_data = (
        db.session.query(Job.location, func.count(Job.id))
        .filter(Job.is_active == True)
        .filter(Job.posted_date >= cutoff)
        .group_by(Job.location)
        .order_by(func.count(Job.id).desc())
        .limit(8)
        .all()
    )

    # Skill demand via SQL LIKE (stays in DB, uses index)
    skills_to_track = [
        'python', 'sql', 'java', 'aws', 'azure', 'react',
        'data engineer', 'analyst', 'cyber', 'intern',
        'javascript', 'power bi', 'spark', 'docker', 'junior',
    ]
    skill_counts = {}
    for skill in skills_to_track:
        count = (
            db.session.query(func.count(Job.id))
            .filter(Job.is_active == True)
            .filter(Job.posted_date >= cutoff)
            .filter(Job.title.ilike(f'%{skill}%'))
            .scalar()
        )
        if count:
            skill_counts[skill.title()] = count

    # 14-day trend
    trend_data = (
        db.session.query(Job.posted_date, func.count(Job.id))
        .filter(Job.is_active == True)
        .filter(Job.posted_date >= (datetime.utcnow() - timedelta(days=14)).date())
        .group_by(Job.posted_date)
        .order_by(Job.posted_date.asc())
        .all()
    )

    last_run = _pipeline_state.get('last_run')
    last_run_str = last_run.strftime('%d %b %Y, %H:%M') if last_run else 'Not run yet'

    return render_template(
        'stats.html',
        total_jobs=total_jobs,
        active_jobs=active_jobs,
        source_labels=[s[0] for s in source_data],
        source_values=[s[1] for s in source_data],
        loc_labels=[l[0] for l in location_data],
        loc_values=[l[1] for l in location_data],
        skill_labels=list(skill_counts.keys()),
        skill_values=list(skill_counts.values()),
        trend_labels=[str(t[0]) for t in trend_data],
        trend_values=[t[1] for t in trend_data],
        last_run=last_run_str,
        pipeline_running=_pipeline_state['running'],
    )


# ---------------------------------------------------------------------------
# 2. Pipeline refresh (background thread)
# ---------------------------------------------------------------------------

def _run_pipeline_in_background(app):
    with app.app_context():
        try:
            new_jobs = run_etl()
            _pipeline_state['last_result'] = new_jobs
        except Exception as e:
            logger.error(f"Background pipeline error: {e}")
            _pipeline_state['last_result'] = f'error: {e}'
        finally:
            _pipeline_state['running'] = False
            _pipeline_state['last_run'] = datetime.utcnow()


@web_bp.route('/refresh', methods=['POST'])
def refresh_data():
    from flask import current_app
    import logging
    global logger
    logger = logging.getLogger(__name__)

    if _pipeline_state['running']:
        flash("⏳ Pipeline is already running. Please wait.", "warning")
        return redirect(url_for('web.index'))

    _pipeline_state['running'] = True
    thread = threading.Thread(
        target=_run_pipeline_in_background,
        args=(current_app._get_current_object(),),
        daemon=True,
    )
    thread.start()

    flash("🚀 Pipeline started! New jobs will appear in ~30 seconds. Page will update automatically.", "info")
    return redirect(url_for('web.index'))


@web_bp.route('/refresh/status')
def refresh_status():
    """Polled by the navbar JS to update the spinner state."""
    return jsonify({
        'running': _pipeline_state['running'],
        'last_run': _pipeline_state['last_run'].isoformat() if _pipeline_state['last_run'] else None,
        'last_result': _pipeline_state['last_result'],
    })


# ---------------------------------------------------------------------------
# 3. One-time cleanup of existing ghost jobs (on first visit)
# ---------------------------------------------------------------------------

import logging
logger = logging.getLogger(__name__)