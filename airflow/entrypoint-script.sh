#!/bin/bash
# Fix for Flask-Session using a compatible approach
python -c "
from datetime import datetime, timezone
from flask_session.sessions import MongoDBSessionInterface

# Patch MongoDBSessionInterface.open_session directly
original_open_session = MongoDBSessionInterface.open_session

def patched_open_session(self, app, request):
    rv = original_open_session(self, app, request)
    if hasattr(self, 'collection') and hasattr(self.collection, 'find_one'):
        sid = self.key_prefix + request.cookies.get(app.session_cookie_name, '')
        saved_session = self.collection.find_one({'sid': sid})
        if saved_session and hasattr(saved_session, 'expiry'):
            if not saved_session.expiry.tzinfo:
                saved_session.expiry = saved_session.expiry.replace(tzinfo=timezone.utc)
    return rv

MongoDBSessionInterface.open_session = patched_open_session
print('Flask-Session MongoDB interface patched successfully.')
"

# Run airflow command
exec airflow $@