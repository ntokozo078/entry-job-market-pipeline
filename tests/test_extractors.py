"""Tests for each extractor's filters, using stored sample responses and no network."""
import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion.extractors import adzuna, remotive, scraper
from ingestion.utils import utc_today

FIXTURES = Path(__file__).parent / 'fixtures'


def load_json(name):
    return json.loads((FIXTURES / name).read_text(encoding='utf-8'))


class TestAdzunaFilter:

    @pytest.mark.parametrize('title, description, expected', [
        ('Senior Data Engineer', '5 years experience required', False),
        ('Lead Software Engineer', 'Lead a team of 10', False),
        ('Solutions Architect', 'Junior friendly team', False),
        ('Junior Python Developer', 'Great role for new grads', True),
        ('Graduate Data Analyst', 'Join our graduate programme', True),
        ('Data Analyst', 'internship program for students', True),
        ('Python Developer', 'Must have strong skills', False),
        ('Junior Developer', 'At least 3 years of work', False),
    ])
    def test_is_entry_level(self, title, description, expected):
        assert adzuna.is_entry_level({'title': title, 'description': description}) is expected

    @pytest.mark.parametrize('location, description, expected', [
        ('London', 'Work from home role', True),
        ('Remote', 'Office hours', True),
        ('London', 'Office based', False),
    ])
    def test_is_truly_remote(self, location, description, expected):
        item = {'title': 'Junior Dev', 'description': description, 'location': {'display_name': location}}
        assert adzuna.is_truly_remote(item) is expected

    def test_parse_date_reads_the_real_posted_date(self):
        assert adzuna.parse_adzuna_date({'created': '2026-07-14T10:00:00Z'}) == date(2026, 7, 14)

    def test_parse_date_falls_back_to_today(self):
        assert adzuna.parse_adzuna_date({'created': 'not a date'}) == date.today()
        assert adzuna.parse_adzuna_date({}) == date.today()

    def test_normalize_maps_the_response(self):
        job = adzuna.normalize(load_json('adzuna_search.json')['results'][0], 'adzuna_sa', 'South Africa')
        assert job['source'] == 'adzuna_sa'
        assert job['source_job_id'] == '1001'
        assert job['company'] == 'Example Tech'
        assert job['salary_min'] == 240000
        assert job['posted_date'] == date(2026, 10, 30)


class TestFetchAdzunaJobs:

    @pytest.fixture(autouse=True)
    def no_delay(self, monkeypatch):
        monkeypatch.setattr(adzuna, 'ADZUNA_APP_ID', 'test-id')
        monkeypatch.setattr(adzuna.time, 'sleep', lambda seconds: None)

    def test_without_keys_returns_nothing(self, monkeypatch):
        monkeypatch.setattr(adzuna, 'ADZUNA_APP_ID', None)
        assert adzuna.fetch_adzuna_jobs() == []

    def test_sample_response_keeps_entry_level_and_current_jobs(self, monkeypatch):
        results = load_json('adzuna_search.json')['results']
        monkeypatch.setattr(adzuna, 'query_adzuna', lambda **kwargs: results if kwargs['country'] == 'za' else [])
        jobs = adzuna.fetch_adzuna_jobs()
        # Sample holds a junior role, a repeat of it, a senior role and a title from 2017
        assert [job['source_job_id'] for job in jobs] == ['1001']
        assert jobs[0]['location'] == 'South Africa'

    def test_sa_results_cannot_use_up_the_global_search(self, monkeypatch):
        def fake_query(country, what, max_days_old):
            return [
                {'id': f'{country}-{what}-{n}', 'title': 'Junior Developer', 'description': ''}
                for n in range(10)
            ]

        monkeypatch.setattr(adzuna, 'query_adzuna', fake_query)
        jobs = adzuna.fetch_adzuna_jobs()
        sources = [job['source'] for job in jobs]
        assert sources.count('adzuna_sa') == adzuna.MAX_SA_JOBS_PER_RUN
        for country in adzuna.GLOBAL_COUNTRIES:
            assert sources.count(f'adzuna_{country}') == adzuna.MAX_JOBS_PER_COUNTRY

    def test_global_location_marks_remote_roles(self, monkeypatch):
        items = [
            {'id': 'r', 'title': 'Junior Dev', 'description': 'Remote role', 'location': {}},
            {'id': 'o', 'title': 'Junior Dev', 'description': 'Office role', 'location': {}},
        ]
        monkeypatch.setattr(adzuna, 'query_adzuna', lambda **kwargs: items if kwargs['country'] == 'gb' else [])
        locations = {job['source_job_id']: job['location'] for job in adzuna.fetch_adzuna_jobs()}
        assert locations == {'r': 'Remote (GB)', 'o': 'GB'}


class TestRemotive:

    def fake_get(self, url, params, timeout):
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: load_json('remotive_jobs.json'))

    def test_sample_response_keeps_only_entry_level(self, monkeypatch):
        monkeypatch.setattr(remotive.requests, 'get', self.fake_get)
        jobs = remotive.fetch_remotive_jobs()
        assert [job['title'] for job in jobs] == ['Junior Data Analyst']
        assert jobs[0]['source'] == 'remotive'
        assert jobs[0]['location'] == 'Remote — Worldwide'
        assert jobs[0]['posted_date'] == date(2026, 10, 28)

    def test_senior_word_in_description_rejects_a_junior_title(self, monkeypatch):
        response = {'jobs': [{
            'id': 1, 'title': 'Junior Developer', 'description': 'Report to the team lead',
            'url': 'https://example.com/1',
        }]}
        monkeypatch.setattr(
            remotive.requests, 'get',
            lambda url, params, timeout: SimpleNamespace(raise_for_status=lambda: None, json=lambda: response),
        )
        assert remotive.fetch_remotive_jobs() == []

    def test_a_failing_category_returns_no_jobs_without_raising(self, monkeypatch):
        def failing_get(url, params, timeout):
            raise remotive.requests.exceptions.Timeout()

        monkeypatch.setattr(remotive.requests, 'get', failing_get)
        assert remotive.fetch_remotive_jobs() == []


class TestCareers24:

    @pytest.fixture
    def scraped(self, monkeypatch):
        recent = (utc_today() - timedelta(days=3)).strftime('%d %B %Y')
        expired = (utc_today() - timedelta(days=3)).strftime('%d %B %Y')
        old = (utc_today() - timedelta(days=90)).strftime('%d %B %Y')
        html = (
            (FIXTURES / 'careers24_search.html').read_text(encoding='utf-8')
            .replace('{{RECENT}}', recent)
            .replace('{{EXPIRED}}', expired)
            .replace('{{OLD}}', old)
        )
        monkeypatch.setattr(
            scraper.requests, 'get',
            lambda url, headers, timeout: SimpleNamespace(status_code=200, text=html),
        )
        monkeypatch.setattr(scraper.time, 'sleep', lambda seconds: None)
        return scraper.scrape_careers24()

    def test_keeps_only_valid_cards_once_across_search_pages(self, scraped):
        assert [job['title'] for job in scraped] == ['Graduate Software Developer']

    def test_maps_card_fields(self, scraped):
        job = scraped[0]
        assert job['source'] == 'careers24'
        assert job['source_job_id'] == '2001'
        assert job['company'] == 'Example Bank'
        assert job['location'] == 'Durban'
        assert job['url'] == 'https://www.careers24.com/jobs/adverts/graduate-software-developer-2001/'

    def test_description_is_a_placeholder(self, scraped):
        # The search page has no description, so seniority is checked on the title only
        assert scraped[0]['description'] == 'Apply on Careers24'

    def test_blocked_page_returns_no_jobs(self, monkeypatch):
        monkeypatch.setattr(
            scraper.requests, 'get',
            lambda url, headers, timeout: SimpleNamespace(status_code=403, text=''),
        )
        monkeypatch.setattr(scraper.time, 'sleep', lambda seconds: None)
        assert scraper.scrape_careers24() == []
