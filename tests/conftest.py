"""Shared fixtures. Every test gets its own temporary SQLite database and makes no network calls."""
from datetime import date

import pytest

from app import create_app, init_db
from app.config import Config
from app.models import db


@pytest.fixture
def app(tmp_path):
    class TestConfig(Config):
        TESTING = True
        SECRET_KEY = 'test-secret'
        REFRESH_TOKEN = 'test-refresh-token'
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{(tmp_path / 'test.db').as_posix()}"

    application = create_app(TestConfig)
    with application.app_context():
        init_db()
        yield application
        db.session.remove()
        db.engine.dispose()


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def make_job():
    def _make_job(source_job_id='1', source='adzuna_sa', **overrides):
        job = {
            'source': source,
            'source_job_id': source_job_id,
            'title': 'Junior Data Engineer',
            'company': 'Example Co',
            'location': 'South Africa',
            'url': f'https://example.com/jobs/{source_job_id}',
            'description': 'Graduate role',
            'job_type': 'entry_level',
            'posted_date': date.today(),
            'is_active': True,
        }
        job.update(overrides)
        return job
    return _make_job
