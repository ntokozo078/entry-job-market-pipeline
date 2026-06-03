# Inside your app setup (likely app/__init__.py)
import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    
    # 1. Your existing database URL setup
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
    
    # 2. ADD THIS NEW BLOCK:
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        "pool_pre_ping": True,  # Tests the connection before every query
        "pool_recycle": 300     # Refreshes the connection every 5 minutes
    }
    
    db.init_app(app)
    return app