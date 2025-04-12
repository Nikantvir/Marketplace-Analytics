#!/bin/bash
# Применяем патч для flask_session
python -c "
import sys
from datetime import datetime, timezone
from flask_session.sessions import open_session as orig_open_session

def patched_open_session(self, app, request):
    rv = orig_open_session(self, app, request)
    if hasattr(self, 'collection') and hasattr(self.collection, 'find_one'):
        # Fix MongoDB session expiry check
        if saved_session := self.collection.find_one({'sid': self.key_prefix + request.cookies.get(app.session_cookie_name, '')}):
            if hasattr(saved_session, 'expiry'):
                if not saved_session.expiry.tzinfo:
                    saved_session.expiry = saved_session.expiry.replace(tzinfo=timezone.utc)
    return rv

from flask_session.sessions import SessionInterface
SessionInterface.open_session = patched_open_session
print('Flask-Session patched successfully.')
"

# Запуск команды airflow
exec airflow $@