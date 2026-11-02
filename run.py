# run.py
import logging
from app import create_app, init_db

app = create_app()

# Render has no shell step on the free tier, so the app prepares its schema on startup.
# A failure is logged rather than raised so /api/health can still report the database as down.
with app.app_context():
    try:
        init_db()
    except Exception:
        logging.getLogger(__name__).exception('Database setup failed')

if __name__ == '__main__':
    app.run(debug=True)
