"""Tests for loading, deactivation and retention against a temporary SQLite database."""
from datetime import date, datetime, timedelta

import pytest

from app.models import db, Job, utc_now
from ingestion import pipeline


@pytest.fixture
def stub_sources(monkeypatch):
    """Replaces the three extractors. Set a value to a list of jobs or to an exception to raise."""
    results = {'adzuna': [], 'careers24': [], 'remotive': []}

    def stub(name):
        def extractor():
            if isinstance(results[name], Exception):
                raise results[name]
            return results[name]
        return extractor

    monkeypatch.setattr(pipeline, 'fetch_adzuna_jobs', stub('adzuna'))
    monkeypatch.setattr(pipeline, 'scrape_careers24', stub('careers24'))
    monkeypatch.setattr(pipeline, 'fetch_remotive_jobs', stub('remotive'))
    return results


def add_job(make_job, source_job_id='1', **overrides):
    job = Job(**make_job(source_job_id, **overrides))
    db.session.add(job)
    db.session.commit()
    return job


class TestLoad:

    def test_first_run_inserts_every_job(self, app, stub_sources, make_job):
        stub_sources['adzuna'] = [make_job('1'), make_job('2')]
        stub_sources['remotive'] = [make_job('1', source='remotive')]
        summary = pipeline.run_etl()
        assert summary['new'] == 3
        assert summary['updated'] == 0
        assert summary['extracted'] == {'adzuna_sa': 2, 'remotive': 1}
        assert Job.query.count() == 3

    def test_same_id_from_another_source_is_a_separate_job(self, app, stub_sources, make_job):
        stub_sources['adzuna'] = [make_job('1'), make_job('1', source='adzuna_gb')]
        assert pipeline.run_etl()['new'] == 2

    def test_second_run_updates_instead_of_duplicating(self, app, stub_sources, make_job):
        stub_sources['adzuna'] = [make_job('1')]
        pipeline.run_etl()
        summary = pipeline.run_etl()
        assert summary['new'] == 0
        assert summary['updated'] == 1
        assert Job.query.count() == 1

    def test_upsert_refreshes_fields_and_last_seen_but_keeps_first_seen(self, app, stub_sources, make_job):
        first_seen = datetime(2026, 1, 1)
        add_job(make_job, '1', title='Old Title', first_seen_at=first_seen, last_seen_at=first_seen)
        stub_sources['adzuna'] = [make_job('1', title='New Title')]
        pipeline.run_etl()
        job = Job.query.one()
        assert job.title == 'New Title'
        assert job.first_seen_at == first_seen
        assert job.last_seen_at > first_seen

    def test_a_returning_job_is_reactivated(self, app, stub_sources, make_job):
        add_job(make_job, '1', is_active=False)
        stub_sources['adzuna'] = [make_job('1')]
        pipeline.run_etl()
        assert Job.query.one().is_active is True

    def test_failed_commit_raises(self, app, stub_sources, make_job, monkeypatch):
        stub_sources['adzuna'] = [make_job('1')]

        def failing_commit():
            raise RuntimeError('database unavailable')

        monkeypatch.setattr(db.session, 'commit', failing_commit)
        with pytest.raises(RuntimeError):
            pipeline.run_etl()


class TestSourceFailures:

    def test_one_failing_source_does_not_stop_the_others(self, app, stub_sources, make_job):
        stub_sources['adzuna'] = RuntimeError('API down')
        stub_sources['careers24'] = [make_job('1', source='careers24')]
        stub_sources['remotive'] = [make_job('2', source='remotive')]
        summary = pipeline.run_etl()
        assert summary['extracted'] == {'careers24': 1, 'remotive': 1}


class TestDeactivateUnseen:

    def stale(self):
        return utc_now() - timedelta(days=pipeline.NOT_SEEN_MAX_DAYS + 1)

    def test_job_the_source_stopped_returning_goes_inactive(self, app, stub_sources, make_job):
        add_job(make_job, 'gone', last_seen_at=self.stale())
        stub_sources['adzuna'] = [make_job('current')]
        summary = pipeline.run_etl()
        assert summary['deactivated_unseen'] == 1
        assert db.session.get(Job, Job.query.filter_by(source_job_id='gone').one().id).is_active is False
        assert Job.query.filter_by(source_job_id='current').one().is_active is True

    def test_recently_seen_job_stays_active(self, app, stub_sources, make_job):
        add_job(make_job, 'recent', last_seen_at=utc_now() - timedelta(days=1))
        stub_sources['adzuna'] = [make_job('current')]
        assert pipeline.run_etl()['deactivated_unseen'] == 0

    def test_source_with_no_results_keeps_its_jobs(self, app, stub_sources, make_job):
        add_job(make_job, 'stale', source='careers24', last_seen_at=self.stale())
        stub_sources['careers24'] = RuntimeError('site blocked')
        stub_sources['adzuna'] = [make_job('current')]
        pipeline.run_etl()
        assert Job.query.filter_by(source_job_id='stale').one().is_active is True


class TestRetention:

    def days_ago(self, days):
        return date.today() - timedelta(days=days)

    def test_old_jobs_are_deactivated(self, app, make_job):
        add_job(make_job, 'old', posted_date=self.days_ago(pipeline.DISPLAY_MAX_DAYS + 5))
        add_job(make_job, 'fresh')
        assert pipeline.deactivate_old_jobs() == 1
        assert Job.query.filter_by(source_job_id='old').one().is_active is False
        assert Job.query.filter_by(source_job_id='fresh').one().is_active is True

    def test_very_old_jobs_are_deleted(self, app, make_job):
        add_job(make_job, 'ancient', posted_date=self.days_ago(pipeline.DELETE_MAX_DAYS + 5))
        add_job(make_job, 'fresh')
        assert pipeline.cleanup_old_jobs() == 1
        assert [job.source_job_id for job in Job.query.all()] == ['fresh']

    def test_row_cap_removes_the_oldest_jobs(self, app, make_job):
        for age in range(5):
            add_job(make_job, f'age-{age}', posted_date=self.days_ago(age))
        assert pipeline.cleanup_old_jobs(max_rows=3) == 2
        assert {job.source_job_id for job in Job.query.all()} == {'age-0', 'age-1', 'age-2'}

    def test_nothing_to_clean_deletes_nothing(self, app, make_job):
        add_job(make_job, 'fresh')
        assert pipeline.cleanup_old_jobs() == 0
