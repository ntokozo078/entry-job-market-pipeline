# app/api/routes.py
from flask import Blueprint, jsonify, request
from app.models import Job

api_bp = Blueprint('api', __name__)

@api_bp.route('/jobs', methods=['GET'])
def get_jobs():
    """
    GET /api/jobs
    Query Params: ?type=intern&location=durban
    """
    # 1. Get query parameters
    job_type = request.args.get('type')
    location = request.args.get('location')
    limit = request.args.get('limit', 50, type=int)

    # 2. Start Query
    query = Job.query.filter_by(is_active=True)

    # 3. Apply Filters
    if job_type:
        # Case-insensitive search (e.g. "Intern" finds "Internship")
        query = query.filter(Job.title.ilike(f'%{job_type}%'))
    
    if location:
        query = query.filter(Job.location.ilike(f'%{location}%'))

    # 4. Execute
    jobs = query.order_by(Job.posted_date.desc()).limit(limit).all()

    # 5. Return JSON
    return jsonify({
        'count': len(jobs),
        'jobs': [job.to_dict() for job in jobs]
    })

@api_bp.route('/stats', methods=['GET'])
def get_stats():
    """
    GET /api/stats
    Returns simple counts of the data.
    """
    total_jobs = Job.query.count()
    active_jobs = Job.query.filter_by(is_active=True).count()
    
    return jsonify({
        'total_jobs_scraped': total_jobs,
        'active_jobs_now': active_jobs
    })