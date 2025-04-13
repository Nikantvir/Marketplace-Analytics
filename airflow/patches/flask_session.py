#!/bin/bash
# Applying a compatible patch for flask_session 0.5.0
python -c "
import sys
from datetime import datetime, timezone
from flask_session.sessions import SessionInterface

# Check if we're dealing with a MongoDB session interface
orig_open_session = SessionInterface.open_session

def patched_open_session(self, app, request):
    rv = orig_open_session(self, app, request)
    if hasattr(self, 'collection') and hasattr(self.collection, 'find_one'):
        # Fix MongoDB session expiry check
        sid = self.key_prefix + request.cookies.get(app.session_cookie_name, '')
        if hasattr(self, 'collection') and hasattr(self.collection, 'find_one'):
            saved_session = self.collection.find_one({'sid': sid})
            if saved_session and hasattr(saved_session, 'expiry'):
                if not saved_session.expiry.tzinfo:
                    saved_session.expiry = saved_session.expiry.replace(tzinfo=timezone.utc)
    return rv

SessionInterface.open_session = patched_open_session
print('Flask-Session patched successfully.')
"

# Run airflow command
exec airflow $@