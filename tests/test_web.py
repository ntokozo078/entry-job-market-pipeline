"""Tests for the REST API, the HTML pages and the /refresh protection, using Flask's test client."""
from datetime import date, timedelta

import pytest

from app import create_app, init_db
from app.config import Config, DEV_SECRET_KEY, normalize_database_url
from app.models import db, Job
from app.web import routes as web_routes


@pytest.fixture
def seeded(app, make_job):
    db.session.add_all([
        Job(**make_job('1', title='Junior Python Developer', location='Durban')),
        Job(**make_job('2', title='Data Intern', location='Cape Town')),
        Job(**make_job('3', source='remotive', title='Junior Analyst', location='Remote')),
        Job(**make_job('4', title='Graduate Tester', is_active=False)),
    ])
    db.session.commit()


class TestApi:

    def test_jobs_lists_active_only(self, client, seeded):
        body = client.get('/api/jobs').get_json()
        assert body['count'] == 3
        assert 'Graduate Tester' not in [job['title'] for job in body['jobs']]

    @pytest.mark.parametrize('query, expected', [
        ('type=python', ['Junior Python Developer']),
        ('location=cape', ['Data Intern']),
        ('source=remotive', ['Junior Analyst']),
    ])
    def test_jobs_filters(self, client, seeded, query, expected):
        body = client.get(f'/api/jobs?{query}').get_json()
        assert [job['title'] for job in body['jobs']] == expected

    def test_jobs_limit_is_capped_at_200(self, client, make_job):
        db.session.add_all([Job(**make_job(str(i))) for i in range(205)])
        db.session.commit()
        assert client.get('/api/jobs?limit=1000').get_json()['count'] == 200
        assert client.get('/api/jobs?limit=5').get_json()['count'] == 5

    def test_stats_counts(self, client, seeded):
        body = client.get('/api/stats').get_json()
        assert body['total_jobs_scraped'] == 4
        assert body['active_jobs_now'] == 3
        assert body['by_source'] == {'adzuna_sa': 3, 'remotive': 1}

    def test_health_ok(self, client):
        response = client.get('/api/health')
        assert response.status_code == 200
        assert response.get_json()['db_connected'] is True


class TestPages:

    @pytest.mark.parametrize('path', ['/', '/global', '/stats', '/tracker'])
    def test_page_renders(self, client, seeded, path):
        assert client.get(path).status_code == 200

    def test_home_shows_only_sa_sources(self, client, seeded):
        page = client.get('/').get_data(as_text=True)
        assert 'Junior Python Developer' in page
        assert 'Junior Analyst' not in page

    def test_global_excludes_sa_sources(self, client, seeded):
        page = client.get('/global').get_data(as_text=True)
        assert 'Junior Analyst' in page
        assert 'Junior Python Developer' not in page

    def test_jobs_past_the_display_window_are_hidden(self, client, make_job):
        old = date.today() - timedelta(days=web_routes.DISPLAY_MAX_DAYS + 1)
        db.session.add(Job(**make_job('old', title='Old Junior Role', posted_date=old)))
        db.session.commit()
        assert 'Old Junior Role' not in client.get('/').get_data(as_text=True)

    def test_stats_reports_last_run_from_the_database(self, client, seeded):
        assert 'Not run yet' not in client.get('/stats').get_data(as_text=True)

    def test_stats_without_data_says_not_run(self, client):
        assert 'Not run yet' in client.get('/stats').get_data(as_text=True)

    def test_adzuna_jobs_carry_a_linked_attribution(self, client, seeded):
        assert 'href="https://www.adzuna.co.za/"' in client.get('/').get_data(as_text=True)

    def test_no_public_refresh_button(self, client, seeded):
        assert 'refresh-form' not in client.get('/').get_data(as_text=True)


class TestRefreshProtection:

    @pytest.fixture(autouse=True)
    def fake_pipeline(self, monkeypatch):
        self.runs = []
        monkeypatch.setattr(web_routes, 'run_etl', lambda: self.runs.append(1) or {'new': 0})
        monkeypatch.setitem(web_routes._pipeline_state, 'running', False)

    def test_missing_token_is_rejected(self, client):
        assert client.post('/refresh').status_code == 403
        assert self.runs == []

    def test_wrong_token_is_rejected(self, client):
        response = client.post('/refresh', headers={'X-Refresh-Token': 'wrong'})
        assert response.status_code == 403
        assert self.runs == []

    def test_closed_when_no_token_is_configured(self, app, client):
        app.config['REFRESH_TOKEN'] = None
        response = client.post('/refresh', headers={'X-Refresh-Token': ''})
        assert response.status_code == 403

    def test_non_ascii_token_is_rejected_cleanly(self, client):
        response = client.post('/refresh', headers={'X-Refresh-Token': 'tökén'.encode('utf-8')})
        assert response.status_code == 403

    def test_valid_token_starts_a_run(self, client, monkeypatch):
        # Run the work inline so the test doesn't race a background thread
        class InlineThread:
            def __init__(self, target, args, daemon):
                self.target, self.args = target, args

            def start(self):
                self.target(*self.args)

        monkeypatch.setattr(web_routes.threading, 'Thread', InlineThread)
        response = client.post('/refresh', headers={'X-Refresh-Token': 'test-refresh-token'})
        assert response.status_code == 202
        assert self.runs == [1]

    def test_second_run_is_refused_while_one_is_running(self, client, monkeypatch):
        monkeypatch.setitem(web_routes._pipeline_state, 'running', True)
        response = client.post('/refresh', headers={'X-Refresh-Token': 'test-refresh-token'})
        assert response.status_code == 409
        assert self.runs == []


class TestConfiguration:

    @pytest.mark.parametrize('url', [
        'postgres://u:p@host/db',
        'postgresql://u:p@host/db',
        'postgresql+psycopg2://u:p@host/db',
        'postgresql+psycopg://u:p@host/db',
    ])
    def test_postgres_urls_use_the_installed_driver(self, url):
        assert normalize_database_url(url) == 'postgresql+psycopg://u:p@host/db'

    def test_sqlite_url_is_unchanged(self):
        assert normalize_database_url('sqlite:///local.db') == 'sqlite:///local.db'

    @pytest.mark.parametrize('variable, value', [('RENDER', 'true'), ('FLASK_ENV', 'production')])
    def test_production_refuses_the_default_secret_key(self, monkeypatch, variable, value):
        class DefaultKey(Config):
            SECRET_KEY = DEV_SECRET_KEY

        monkeypatch.delenv('RENDER', raising=False)
        monkeypatch.delenv('FLASK_ENV', raising=False)
        monkeypatch.setenv(variable, value)
        with pytest.raises(RuntimeError, match='SECRET_KEY'):
            create_app(DefaultKey)

    def test_development_allows_the_default_secret_key(self, monkeypatch):
        class DefaultKey(Config):
            SECRET_KEY = DEV_SECRET_KEY

        monkeypatch.delenv('RENDER', raising=False)
        monkeypatch.setenv('FLASK_ENV', 'development')
        assert create_app(DefaultKey) is not None


class TestInitDb:

    def test_adds_missing_salary_columns_once(self, app):
        from sqlalchemy import inspect, text

        db.session.execute(text('ALTER TABLE jobs DROP COLUMN salary_min'))
        db.session.commit()
        init_db()
        init_db()
        columns = {column['name'] for column in inspect(db.engine).get_columns('jobs')}
        assert {'salary_min', 'salary_max'} <= columns
