# flask_session_patch.py
from datetime import datetime, timezone
import types
from flask_session.sessions import SessionInterface

# Original method reference
original_open_session = SessionInterface.open_session

# Create patched method
def patched_open_session(self, app, request):
    # Call original method to get the session
    rv = original_open_session(self, app, request)
    
    # Fix any timezone issues in the session
    if hasattr(rv, 'expiry') and rv.expiry and not rv.expiry.tzinfo:
        rv.expiry = rv.expiry.replace(tzinfo=timezone.utc)
    
    return rv

# Apply the patch
SessionInterface.open_session = patched_open_session