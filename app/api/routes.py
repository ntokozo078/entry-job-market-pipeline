# app/api/routes.py
from datetime import datetime
from flask import Blueprint, jsonify, request
from app.models import db, Job

api_bp = Blueprint('api', __name__)


@api_bp.route('/jobs', methods=['GET'])
def get_jobs():
    """
    GET /api/jobs
    Query Params:
      - type     : Filter by keyword in title (e.g. ?type=intern)
      - location : Filter by location (e.g. ?location=durban)
      - source   : Filter by data source (e.g. ?source=adzuna_sa)
      - limit    : Max results to return (default 50, max 200)
    """
    job_type = request.args.get('type')
    location = request.args.get('location')
    source = request.args.get('source')
    limit = min(request.args.get('limit', 50, type=int), 200)

    query = Job.query.filter_by(is_active=True)

    if job_type:
        query = query.filter(Job.title.ilike(f'%{job_type}%'))

    if location:
        query = query.filter(Job.location.ilike(f'%{location}%'))

    if source:
        query = query.filter(Job.source == source)

    jobs = query.order_by(Job.posted_date.desc()).limit(limit).all()

    return jsonify({
        'count': len(jobs),
        'jobs': [job.to_dict() for job in jobs],
    })


@api_bp.route('/stats', methods=['GET'])
def get_stats():
    """
    GET /api/stats
    Returns aggregate counts about the current dataset.
    """
    from sqlalchemy import func

    total_jobs = Job.query.count()
    active_jobs = Job.query.filter_by(is_active=True).count()

    source_breakdown = dict(
        db.session.query(Job.source, func.count(Job.id))
        .group_by(Job.source)
        .all()
    )

    return jsonify({
        'total_jobs_scraped': total_jobs,
        'active_jobs_now': active_jobs,
        'by_source': source_breakdown,
        'generated_at': datetime.utcnow().isoformat(),
    })


@api_bp.route('/health', methods=['GET'])
def health_check():
    """
    GET /api/health
    Standard health-check endpoint. Returns 200 if the app and database
    are reachable, or 503 if the database connection fails.
    """
    try:
        # A lightweight DB probe — just fetch one row's ID
        db.session.execute(db.select(Job.id).limit(1))
        db_ok = True
    except Exception:
        db_ok = False

    status_code = 200 if db_ok else 503
    return jsonify({
        'status': 'ok' if db_ok else 'degraded',
        'db_connected': db_ok,
        'timestamp': datetime.utcnow().isoformat(),
    }), status_code