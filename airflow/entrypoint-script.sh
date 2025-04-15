#!/bin/bash
# Apply patch for flask_session timezone issue
python -c "
import sys
from datetime import datetime, timezone
import importlib
import types

import sys
sys.path.insert(0, '/opt/airflow/patches')
import flask_session_patch
print('Flask-Session patched for timezone handling.')

# Import flask_session module
from flask_session import sessions

# Create a patched version of open_session method
def patched_open_session(self, app, request):
    saved_session = None
    sid = request.cookies.get(app.session_cookie_name)
    
    # For various session types
    if hasattr(self, 'collection') and sid:  # MongoDB
        saved_session = self.collection.find_one({'sid': self.key_prefix + sid})
    elif hasattr(self, 'redis') and sid:  # Redis
        saved_session = self.serializer.loads(self.redis.get(self.key_prefix + sid) or b'')
    
    # Handle the expiry time by ensuring both times have timezone info
    if saved_session and hasattr(saved_session, 'expiry') and saved_session.expiry:
        # Convert expiry time to TZ-aware if it's naive
        if not saved_session.expiry.tzinfo:
            saved_session.expiry = saved_session.expiry.replace(tzinfo=timezone.utc)
            
        # Use TZ-aware utcnow for comparison
        if saved_session.expiry <= datetime.now(timezone.utc):
            return self.session_class()
    
    # Create a new session if needed
    if not saved_session:
        return self.session_class()
    
    return self.session_class(saved_session)

# Apply the patch to the relevant class(es)
sessions.SessionInterface.open_session = patched_open_session
print('Flask-Session patched successfully for timezone handling.')
"

# Execute the original airflow command
exec airflow $@