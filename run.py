import sys
import os

# Add src to the system path to allow imports from src
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from app import app, db

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
