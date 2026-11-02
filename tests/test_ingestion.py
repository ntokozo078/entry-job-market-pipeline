"""
tests/test_ingestion.py

Unit tests for the ingestion layer utilities.
Run with: python -m pytest
"""
from datetime import date, timedelta
from ingestion.utils import is_title_outdated, clean_text, parse_relative_date, is_date_valid, utc_today


# ── is_title_outdated ──────────────────────────────────────────────────────

class TestIsTitleOutdated:

    def test_returns_false_for_current_year_title(self):
        current_year = date.today().year
        assert is_title_outdated(f"Graduate Programme {current_year}") is False

    def test_returns_true_for_old_year(self):
        assert is_title_outdated("Software Engineer 2019") is True
        assert is_title_outdated("IT Graduate 2017") is True

    def test_returns_false_for_title_with_no_year(self):
        assert is_title_outdated("Junior Python Developer") is False

    def test_returns_false_for_none_input(self):
        assert is_title_outdated(None) is False

    def test_returns_false_for_empty_string(self):
        assert is_title_outdated("") is False

    def test_returns_false_for_last_year(self):
        last_year = date.today().year - 1
        assert is_title_outdated(f"Internship {last_year}") is False


# ── clean_text ─────────────────────────────────────────────────────────────

class TestCleanText:

    def test_strips_whitespace(self):
        assert clean_text("  Hello World  ") == "Hello World"

    def test_collapses_internal_spaces(self):
        assert clean_text("Python   Developer") == "Python Developer"

    def test_returns_none_for_none_input(self):
        assert clean_text(None) is None

    def test_handles_newlines_and_tabs(self):
        assert clean_text("Data\n\tEngineer") == "Data Engineer"


# ── parse_relative_date ────────────────────────────────────────────────────

class TestParseRelativeDate:

    def test_today(self):
        assert parse_relative_date("today") == utc_today()

    def test_yesterday(self):
        assert parse_relative_date("yesterday") == utc_today() - timedelta(days=1)

    def test_days_ago(self):
        result = parse_relative_date("5 days ago")
        assert result == utc_today() - timedelta(days=5)

    def test_none_defaults_to_today(self):
        assert parse_relative_date(None) == utc_today()

    def test_explicit_date(self):
        assert parse_relative_date("30 June 2017") == date(2017, 6, 30)


# ── is_date_valid ──────────────────────────────────────────────────────────

class TestIsDateValid:

    def test_today_is_valid(self):
        assert is_date_valid(utc_today()) is True

    def test_future_date_is_valid(self):
        future = utc_today() + timedelta(days=10)
        assert is_date_valid(future) is True

    def test_recent_past_is_valid(self):
        recent = utc_today() - timedelta(days=30)
        assert is_date_valid(recent) is True

    def test_old_date_is_invalid(self):
        old = utc_today() - timedelta(days=90)
        assert is_date_valid(old, max_age_days=60) is False

    def test_none_is_invalid(self):
        assert is_date_valid(None) is False
