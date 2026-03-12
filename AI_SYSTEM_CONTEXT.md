You are helping me build a software system called Carolina Compliance Solutions.

This system automates Certificate of Insurance (COI) compliance management for contractor clients and their subcontractors.

The system ingests COIs, extracts insurance data using AI, stores structured records in Airtable, and automatically evaluates vendor compliance with client insurance requirements.

The system is designed to evolve into a scalable vendor compliance platform capable of serving many contractor clients.

Your role is to help design, improve, and expand the system without breaking the existing pipeline.

Always prioritize:

- minimal refactors
- stable modules
- modular architecture
- safe automation
- scalability for many contractor clients
- secure data handling
- clarity of Airtable data relationships


SYSTEM ARCHITECTURE

Language:
Python 3.9

Core libraries:
pyairtable
pdf2image
OpenAI API
imaplib
logging

Environment variables:
AIRTABLE_API_KEY
AIRTABLE_BASE_ID
OPENAI_API_KEY


Folder structure:

uploads/
extracted/
venv/


Key scripts:

email_monitor.py
extractor.py
airtable_importer.py
processor.py
compliance_checker.py
module_6_task_creator.py
module_7a_client_setup_wizard.py
module_7b_requirement_validator.py
run_pipeline.py


PIPELINE FLOW

Module 1 – Email Intake

Reads vendor emails and downloads attachments to uploads/.

Typical inputs include:
COI PDFs
Insurance endorsements
policy documents


Module 2 – COI Extractor

Converts PDFs to images using Poppler.

Images are sent to OpenAI GPT-4o to extract structured insurance data including:

insured name
policy type
carrier
policy number
effective date
expiration date
coverage limits
certificate holder
additional insured indicators
waiver indicators
primary noncontributory indicators

Output JSON is saved to:

extracted/


Module 3 – Airtable Importer

Creates records in:

Incoming Extractions

This table stores raw AI extraction results.


Module 4 – COI Processor

Matches extracted data to vendors and creates records in:

Insurance Policies
Insurance Certificates


Module 5 – Compliance Checker

Evaluates expiration dates and writes:

Vendors → Expiration Status

Examples:
Active
Expiring Soon
Expired


Module 6 – Renewal Task Creator

Creates follow-up tasks in the Tasks table for expiring policies.

These tasks drive vendor follow-up workflows.


Module 7A – Client Setup Wizard

Creates:

Clients
Client Requirements

This allows onboarding of new contractor clients.

Each client defines insurance requirements for vendors.


Module 7B – Requirement Validator

Evaluates vendor policies against client insurance requirements.

Writes results to:

Vendors → Compliance Status

Example statuses:

Compliant
Non-Compliant
Missing Policy
Below Minimum Limit
Missing Endorsement


AIRTABLE DATA MODEL

Tables:

Clients
Client Requirements
Vendor Requirement Overrides
Vendors
Insurance Policies
Insurance Certificates
Incoming Extractions
Tasks


Important fields:

Vendors

Client Link
Compliance Status
Expiration Status
Vendor Email
Vendor Name


Client Requirements

Client Link
Policy Type
Required
Minimum Limit
Additional Insured Required
Waiver Required
Primary Noncontributory Required


Insurance Policies

Vendor Link
Policy Type
Carrier
Policy Number
Effective Date
Expiration Date
Coverage Limits


Insurance Certificates

Vendor Link
Certificate Holder
Certificate Date
Source File


Tasks

Vendor Link
Task Type
Due Date
Status


CURRENT SYSTEM STATUS

The entire pipeline runs successfully end-to-end.

Current command:

python run_pipeline.py


System capabilities already include:

COI ingestion via email
AI extraction of insurance data
automated policy creation
policy expiration monitoring
client requirement validation
renewal task creation


LONG TERM PRODUCT VISION

Carolina Compliance Solutions will evolve from a COI processor into a vendor compliance platform.

Future capabilities may include:

Vendor onboarding automation

Vendor portal for uploading insurance documents

Client dashboards showing:
compliant vendors
non-compliant vendors
expiring policies
missing documents

Automated vendor reminders

Agent-friendly workflows allowing insurance agents to submit certificates directly

Email ingestion of COIs sent directly by insurance agents

Compliance rule engine evaluating:

coverage limits
expiration dates
additional insured requirements
waiver of subrogation requirements
primary/noncontributory requirements

Audit trail of compliance decisions.

The platform may eventually expand to support additional compliance documents:

W-9 forms
licenses
safety certifications
contracts


SECURITY PRINCIPLES

The system handles sensitive insurance data.

Design must ensure:

no public storage of uploaded documents
secure handling of API keys
audit logs of automation actions
vendor data isolation by client
minimal exposure of vendor information across clients


SCALABILITY GOALS

The system must support:

many contractor clients
thousands of vendors
automated COI ingestion
large volumes of policy documents
continuous compliance evaluation


Design should prioritize:

modular automation scripts
idempotent pipeline runs
clear Airtable data relationships
minimal manual intervention


UPCOMING PRIORITIES

1. Simplify client onboarding
2. Build client dashboards
3. Automate vendor reminder emails
4. Improve vendor-client linking
5. Prepare the system to scale across many contractor clients


DEVELOPMENT RULES

Before suggesting code:

Always describe:

1. the problem
2. the proposed architecture
3. required Airtable changes
4. new scripts or modules
5. how the change integrates with the pipeline

Only after that generate code.


FIRST TASK

Recommend the best next feature to build that improves scalability and usability for contractor clients.

Explain:

why the feature is important
how it integrates into the pipeline
what Airtable changes are required
what scripts or modules should be created