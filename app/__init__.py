# app/__init__.py
"""
Application factory for JobTracker ZA.

Uses Flask Blueprints for a modular architecture:
  - `api`  → /api/*   (REST JSON endpoints)
  - `web`  → /*        (HTML pages)

Connection pooling is configured to handle Render's free-tier SSL drops.
"""
from datetime import date
from flask import Flask
from sqlalchemy import inspect, text
from app.config import Config, DEV_SECRET_KEY, is_production
from app.models import db


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    if is_production() and app.config['SECRET_KEY'] == DEV_SECRET_KEY:
        raise RuntimeError('SECRET_KEY must be set in production.')

    # Connection pooling — prevents stale connections on Render's free tier
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,   # Tests connection before every query
        'pool_recycle': 300,     # Refreshes connection every 5 minutes
    }

    # Initialize database ORM
    db.init_app(app)

    # Register Blueprints
    from app.api.routes import api_bp
    from app.web.routes import web_bp

    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(web_bp)

    # ── Jinja2 context processors ──────────────────────────────
    @app.context_processor
    def inject_globals():
        """Make `now` (today's date) available in all templates for relative timestamps."""
        return {'now': date.today()}

    return app


def init_db():
    """Create missing tables and add columns that older databases don't have. Needs an app context."""
    db.create_all()
    existing = {column['name'] for column in inspect(db.engine).get_columns('jobs')}
    for column in ('salary_min', 'salary_max'):
        if column not in existing:
            db.session.execute(text(f'ALTER TABLE jobs ADD COLUMN {column} FLOAT'))
    db.session.commit()