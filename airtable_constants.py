"""
Airtable field name and table name constants — Carolina Compliance Solutions

Single source of truth for all Airtable field names used across modules.
Import from here instead of hardcoding strings in individual modules.
"""

# ── Table names ──────────────────────────────────────────────────────────────
TBL_VENDORS = "Vendors"
TBL_INSURANCE_POLICIES = "Insurance Policies"
TBL_EMAIL_QUEUE = "Email Queue"
TBL_CLIENTS = "Clients"
TBL_VENDOR_CLIENT_ASSIGNMENTS = "Vendor Client Assignments"
TBL_CLIENT_REQUIREMENTS = "Client Requirements"
TBL_INCOMING_EXTRACTIONS = "Incoming Extractions"
TBL_INCOMING_DOCUMENTS = "Incoming Documents"
TBL_INSURANCE_CERTIFICATES = "Insurance Certificates"

# ── Vendors table ────────────────────────────────────────────────────────────
V_VENDOR_NAME = "Vendor Name"
V_VENDOR_EMAIL = "Vendor Email"
V_COMPLIANCE_STATUS = "Compliance Status"
V_SEND_REQUEST = "Send Request"

# ── Insurance Policies table ─────────────────────────────────────────────────
P_VENDOR_LINK = "Vendor Link"
P_POLICY_TYPE = "Policy Type"
P_POLICY_NUMBER = "Policy Number"
P_EXPIRATION_DATE = "Expiration Date"
P_EFFECTIVE_DATE = "Effective Date"
P_EXPIRATION_STATUS = "Expiration Status"
P_STATUS = "Status"
P_CARRIER = "Carrier"
P_COVERAGE_LIMITS = "Coverage Limits"
P_LAST_REMINDER_THRESHOLD = "Last Reminder Threshold"

# ── Email Queue table ────────────────────────────────────────────────────────
EQ_PRIMARY_EMAIL = "Primary Email"
EQ_SUBJECT = "Subject"
EQ_BODY = "Body"
EQ_CC_EMAILS = "CC Emails"
EQ_EMAIL_TYPE = "Email Type"
EQ_EMAIL_STATUS = "Email Status"
EQ_REMINDER_STATUS = "Reminder Status"
EQ_RECORD_STATUS = "Record Status"
EQ_SEND_AFTER = "Send After"
EQ_SENT_AT = "Sent At"
EQ_CREATED_AT = "Created At"
EQ_VENDOR_LINK = "Vendor"
EQ_CLIENT_LINK = "Client"

# ── Clients table ────────────────────────────────────────────────────────────
C_CLIENT_NAME = "Client Name"
C_CLIENT_LINK = "Client Link"
C_REQUIREMENTS_STATUS = "Requirements Status"
C_CERTIFICATE_HOLDER = "Certificate Holder"

# ── Vendor Client Assignments table ──────────────────────────────────────────
A_VENDOR_LINK = "Vendor Link"
A_CLIENT_LINK = "Client Link"
A_ACTIVE = "Active"
A_COMPLIANCE_STATUS = "Compliance Status"

# ── Incoming Extractions table ───────────────────────────────────────────────
IE_PROCESSING_STATUS = "Processing Status"
IE_NAMED_INSURED = "Named Insured"
IE_MATCH_STATUS = "Match Status"
IE_MATCHED_VENDOR = "Matched Vendor"

# ── Common status values ─────────────────────────────────────────────────────
STATUS_QUEUED = "Queued"
STATUS_PENDING = "Pending"
STATUS_ACTIVE = "Active"
STATUS_SENT = "Sent"
STATUS_FAILED = "Failed"

# Initial Request email type
EMAIL_TYPE_INITIAL_REQUEST = "Initial Request"
EMAIL_TYPE_DEFICIENCY_REQUEST = "Deficiency Request"
