# run.py
from app import create_app, db
from app.models import Job

app = create_app()

# This is a helper command. 
# On Render, you won't have bash, so you need the app to create tables for you.
# We use a special context processor or just run it on startup (simplest for MVP).
with app.app_context():
    try:
        db.create_all() # Creates the tables in Postgres if they don't exist
    except Exception as e:
        print(f"Schema create_all error (safe to ignore if tables exist): {e}")

    # Automatic quick migrations for new columns
    from sqlalchemy import text
    try:
        db.session.execute(text('ALTER TABLE jobs ADD COLUMN salary_min FLOAT;'))
        db.session.commit()
    except Exception:
        db.session.rollback()

    try:
        db.session.execute(text('ALTER TABLE jobs ADD COLUMN salary_max FLOAT;'))
        db.session.commit()
    except Exception:
        db.session.rollback()

if __name__ == '__main__':
    app.run(debug=True)