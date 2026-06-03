# app/__init__.py
from flask import Flask
from app.config import Config
from app.models import db

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # --- ADDED: Connection pooling to prevent Render SSL drops ---
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        "pool_pre_ping": True,  # Tests the connection before every query
        "pool_recycle": 300     # Refreshes the connection every 5 minutes
    }

    # Initialize the database with the app
    db.init_app(app)

    # Register Blueprints (The routes)
    from app.api.routes import api_bp
    from app.web.routes import web_bp
    
    app.register_blueprint(api_bp, url_prefix='/api') 
    app.register_blueprint(web_bp)                    

    return app