# run.py
from app import create_app, db
from app.models import Job

app = create_app()

# This is a helper command. 
# On Render, you won't have bash, so you need the app to create tables for you.
# We use a special context processor or just run it on startup (simplest for MVP).
with app.app_context():
    db.create_all() # Creates the tables in Postgres if they don't exist

if __name__ == '__main__':
    app.run(debug=True)