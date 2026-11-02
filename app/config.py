# app/config.py
import os
import re
from dotenv import load_dotenv

load_dotenv() # Load local .env file if it exists

DEV_SECRET_KEY = 'dev-key-you-should-change'


def normalize_database_url(uri):
    """
    Point any Postgres URL at psycopg 3, the only driver in requirements.txt.
    Render's URL starts with postgres://, which SQLAlchemy doesn't accept.
    """
    return re.sub(r'^postgres(ql)?(\+\w+)?://', 'postgresql+psycopg://', uri)


def is_production():
    # Render sets RENDER on every service it runs
    return 'RENDER' in os.environ or os.environ.get('FLASK_ENV') == 'production'


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', DEV_SECRET_KEY)

    # Sent in the X-Refresh-Token header of POST /refresh. Unset means the endpoint is closed.
    REFRESH_TOKEN = os.environ.get('REFRESH_TOKEN')

    # DATABASE CONFIGURATION
    # Render provides 'DATABASE_URL'. If not found, use local SQLite.
    SQLALCHEMY_DATABASE_URI = normalize_database_url(
        os.environ.get('DATABASE_URL', 'sqlite:///local_jobs.db')
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
