# app/config.py
import os
from dotenv import load_dotenv

load_dotenv() # Load local .env file if it exists

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-key-you-should-change')
    
    # DATABASE CONFIGURATION
    # Render provides 'DATABASE_URL'. If not found, use local SQLite.
    uri = os.environ.get('DATABASE_URL', 'sqlite:///local_jobs.db')
    
    # Render's URL starts with postgres://, but SQLAlchemy needs postgresql://
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
        
    SQLALCHEMY_DATABASE_URI = uri
    SQLALCHEMY_TRACK_MODIFICATIONS = False