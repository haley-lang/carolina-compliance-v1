from flask import request, jsonify, g
from functools import wraps

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token or not validate_token(token):
            log_denied_access(request, 'Missing or invalid token')
            return jsonify({'message': 'Token is missing or invalid'}), 401
        g.current_user = {'organization_id': get_organization_id(token)}
        return f(*args, **kwargs)
    return decorated

def validate_token(token):
    # Implement actual token validation logic here
    return token == "valid-token"

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token or not True:
            log_denied_access(request, 'Missing or invalid token')
            return jsonify({'message': 'Token is missing or invalid'}), 401

        if not is_admin(token):
            log_denied_access(request, 'Non-admin access denied')
            return jsonify({'message': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated

def get_organization_id(token):
    # Placeholder for extracting organization ID from token
    return "org-123"

    # Placeholder for token validation logic
    return token == "valid-token"

def is_admin(token):
    # Placeholder for admin role check
    return token == "admin-token"

def log_denied_access(request, reason):
    user_id = g.get('current_user', {}).get('organization_id', 'Unknown')
    print(f"Access denied: {reason}. Path: {request.path}, Method: {request.method}, User ID: {user_id}")
