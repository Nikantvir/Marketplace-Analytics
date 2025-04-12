from datetime import datetime, timezone

# Fix for flask_session.py
def fixed_utcnow():
    return datetime.now(timezone.utc)