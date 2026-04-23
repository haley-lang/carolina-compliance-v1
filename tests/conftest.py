"""
Pytest configuration and fixtures for Carolina Compliance Solutions COI extraction pipeline.
Mocks all external API interactions (Airtable, Anthropic Claude) to ensure tests never make real API calls.
"""

import sys
import os
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Bootstrap mock stubs for packages that may not be installed in the test environment
import tests._bootstrap  # noqa: F401, E402

from unittest.mock import Mock, MagicMock, patch
import json
from datetime import datetime, timedelta

import pytest


# ============================================================================
# CONFIG FIXTURES
# ============================================================================

@pytest.fixture(autouse=True)
def mock_config():
    """
    Auto-use fixture that patches config module env vars.
    Ensures all config loads use test values, never real credentials.
    """
    with patch.dict('os.environ', {
        'AIRTABLE_API_KEY': 'test_airtable_key_12345',
        'AIRTABLE_BASE_ID': 'appTestBase123456',
        'EMAIL_ADDRESS': 'test@carolina-compliance.local',
        'EMAIL_PASSWORD': 'test_email_password',
    }):
        yield


@pytest.fixture
def mock_airtable_api(mock_table_factory):
    """
    Patches pyairtable.Api to return mock tables.
    All Airtable interactions go through these mocks.
    """
    with patch('pyairtable.Api') as mock_api_class:
        mock_api_instance = Mock()
        mock_api_class.return_value = mock_api_instance

        # Setup mock tables for all key tables in the system
        table_map = {
            'tblIncomingExtractions123': mock_table_factory('tblIncomingExtractions123'),
            'tblVendors123': mock_table_factory('tblVendors123'),
            'tblpPcmm5ANE0bMNB': mock_table_factory('tblpPcmm5ANE0bMNB'),  # Insurance Policies
            'tblClients123': mock_table_factory('tblClients123'),
            'tblCOIRequests123': mock_table_factory('tblCOIRequests123'),
            'tblClientRequirements123': mock_table_factory('tblClientRequirements123'),
            'tblInsuranceCertificates123': mock_table_factory('tblInsuranceCertificates123'),
            'tblEmailQueue123': mock_table_factory('tblEmailQueue123'),
            'tblTemplates123': mock_table_factory('tblTemplates123'),
            'tblVendorClientAssignments123': mock_table_factory('tblVendorClientAssignments123'),
        }

        mock_api_instance.table.side_effect = lambda table_id: table_map.get(
            table_id,
            mock_table_factory(table_id)
        )

        yield mock_api_instance


@pytest.fixture
def mock_anthropic_client():
    """
    Patches anthropic.Anthropic client to return mock responses.
    All Claude API calls are mocked.
    """
    with patch('anthropic.Anthropic') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock the messages.create method
        mock_response = Mock()
        mock_response.content = [Mock(text='{"document_type": "COI", "status": "mock_response"}')]
        mock_client.messages.create.return_value = mock_response

        yield mock_client


# ============================================================================
# MOCK TABLE FACTORY
# ============================================================================

@pytest.fixture
def mock_table_factory():
    """
    Factory fixture that creates mock pyairtable Table objects.
    Each mock table supports: .all(), .first(), .create(), .update(), .get()
    """
    def create_mock_table(table_id):
        mock_table = Mock()
        mock_table.table_id = table_id

        # In-memory storage for mock records
        mock_table._records = {}
        mock_table._next_id_counter = 1

        def mock_all(fields=None, formula=None, page_size=100, **kwargs):
            """Mock the .all() method"""
            return list(mock_table._records.values())

        def mock_first(formula=None, fields=None, **kwargs):
            """Mock the .first() method"""
            records = list(mock_table._records.values())
            return records[0] if records else None

        def mock_create(fields, **kwargs):
            """Mock the .create() method"""
            rec_id = f'rec{table_id[-10:]}_{mock_table._next_id_counter}'
            mock_table._next_id_counter += 1
            record = {
                'id': rec_id,
                'fields': fields,
                'createdTime': datetime.now().isoformat(),
            }
            mock_table._records[rec_id] = record
            return record

        def mock_update(record_id, fields, **kwargs):
            """Mock the .update() method"""
            if record_id in mock_table._records:
                mock_table._records[record_id]['fields'].update(fields)
            return mock_table._records.get(record_id)

        def mock_get(record_id, **kwargs):
            """Mock the .get() method"""
            return mock_table._records.get(record_id)

        def mock_batch_create(records, **kwargs):
            """Mock the .batch_create() method"""
            results = []
            for fields in records:
                results.append(mock_create(fields))
            return results

        def mock_batch_update(records, **kwargs):
            """Mock the .batch_update() method"""
            results = []
            for record in records:
                rec_id = record.get('id')
                fields = record.get('fields', {})
                if rec_id in mock_table._records:
                    results.append(mock_update(rec_id, fields))
            return results

        mock_table.all = mock_all
        mock_table.first = mock_first
        mock_table.create = mock_create
        mock_table.update = mock_update
        mock_table.get = mock_get
        mock_table.batch_create = mock_batch_create
        mock_table.batch_update = mock_batch_update

        return mock_table

    return create_mock_table


# ============================================================================
# SAMPLE DATA FIXTURES - AIRTABLE RECORDS
# ============================================================================

@pytest.fixture
def sample_extraction_fields():
    """
    Returns a realistic Incoming Extraction record dict (Airtable fields format).
    """
    return {
        'name': 'COI_ABC_Contractors_20250115',
        'file_name': 'coi_abc_contractors_2025.pdf',
        'raw_json': json.dumps({
            "document_type": "COI",
            "named_insured": "ABC Contractors LLC",
            "certificate_holder": "Smith General Contractors",
            "certificate_date": "01/15/2025",
            "contact_emails": ["insurance@abccontractors.com"],
            "policies": [
                {
                    "policy_type": "Commercial General Liability",
                    "policy_number": "GL-2025-001234",
                    "carrier": "Hartford Fire Insurance",
                    "effective_date": "01/01/2025",
                    "expiration_date": "01/01/2026",
                    "coverage_limits": "EACH OCCURRENCE $1,000,000, GENERAL AGGREGATE $2,000,000"
                },
                {
                    "policy_type": "Workers Compensation",
                    "policy_number": "WC-2025-005678",
                    "carrier": "State Compensation Insurance Fund",
                    "effective_date": "01/01/2025",
                    "expiration_date": "01/01/2026",
                    "coverage_limits": "E.L. EACH ACCIDENT $1,000,000"
                },
                {
                    "policy_type": "Automobile Liability",
                    "policy_number": "AUTO-2025-009012",
                    "carrier": "Progressive Commercial",
                    "effective_date": "01/01/2025",
                    "expiration_date": "01/01/2026",
                    "coverage_limits": "COMBINED SINGLE LIMIT $1,000,000"
                }
            ]
        }),
        'status': 'pending',
        'extraction_date': datetime.now().isoformat(),
        'vendor_name': 'ABC Contractors LLC',
        'client_name': 'Smith General Contractors',
    }


@pytest.fixture
def sample_vendor_record():
    """
    Returns a realistic vendor record (construction industry example).
    """
    return {
        'name': 'ABC Contractors LLC',
        'email': 'insurance@abccontractors.com',
        'phone': '555-0101',
        'address': '123 Construction Ave, Charlotte, NC 28202',
        'industry': 'General Contracting',
        'coi_required': True,
        'last_coi_date': '2025-01-15',
        'coi_expiration_date': '2026-01-15',
        'status': 'active',
    }


@pytest.fixture
def sample_policy_record():
    """
    Returns a realistic insurance policy record.
    """
    return {
        'policy_type': 'Commercial General Liability',
        'policy_number': 'GL-2025-001234',
        'carrier': 'Hartford Fire Insurance',
        'vendor_name': 'ABC Contractors LLC',
        'effective_date': '2025-01-01',
        'expiration_date': '2026-01-01',
        'coverage_limits': 'EACH OCCURRENCE $1,000,000, GENERAL AGGREGATE $2,000,000',
        'limit_amount': 1000000,
        'deductible': 5000,
        'status': 'active',
    }


@pytest.fixture
def sample_client_record():
    """
    Returns a realistic client record (construction industry example).
    """
    return {
        'name': 'Smith General Contractors',
        'type': 'General Contractor',
        'contact_email': 'contracts@smithcontractors.com',
        'contact_phone': '555-0102',
        'address': '456 Project Blvd, Charlotte, NC 28203',
        'city': 'Charlotte',
        'state': 'NC',
        'zip_code': '28203',
        'status': 'active',
        'coi_required': True,
    }


@pytest.fixture
def sample_requirement_record():
    """
    Returns a realistic client requirement record.
    """
    return {
        'client_name': 'Smith General Contractors',
        'requirement_type': 'Commercial General Liability',
        'required_coverage_limit': 1000000,
        'certificate_holder_required': True,
        'additional_insured_required': True,
        'waiver_of_subrogation': True,
        'status': 'active',
        'notes': 'Required for all vendors on projects',
    }


@pytest.fixture
def sample_certificate_record():
    """
    Returns a realistic insurance certificate record.
    """
    return {
        'vendor_name': 'ABC Contractors LLC',
        'certificate_holder': 'Smith General Contractors',
        'certificate_date': '2025-01-15',
        'certificate_number': 'ACORD-20250115-001',
        'document_status': 'received',
        'extracted': True,
        'compliance_status': 'compliant',
        'expiration_date': '2026-01-15',
        'file_name': 'ACORD_ABC_Contractors_20250115.pdf',
    }


# ============================================================================
# SAMPLE DATA FIXTURES - RAW JSON FROM CLAUDE
# ============================================================================

@pytest.fixture
def sample_raw_json_coi():
    """
    Returns a realistic Raw JSON string from Claude extraction for a COI.
    Matches the structure returned by the extractor module when processing a COI.
    """
    return json.dumps({
        "document_type": "COI",
        "named_insured": "ABC Contractors LLC",
        "certificate_holder": "Smith General Contractors",
        "certificate_date": "01/15/2025",
        "contact_emails": ["insurance@abccontractors.com"],
        "policies": [
            {
                "policy_type": "Commercial General Liability",
                "policy_number": "GL-2025-001234",
                "carrier": "Hartford Fire Insurance",
                "effective_date": "01/01/2025",
                "expiration_date": "01/01/2026",
                "coverage_limits": "EACH OCCURRENCE $1,000,000, GENERAL AGGREGATE $2,000,000"
            },
            {
                "policy_type": "Workers Compensation",
                "policy_number": "WC-2025-005678",
                "carrier": "State Compensation Insurance Fund",
                "effective_date": "01/01/2025",
                "expiration_date": "01/01/2026",
                "coverage_limits": "E.L. EACH ACCIDENT $1,000,000"
            },
            {
                "policy_type": "Automobile Liability",
                "policy_number": "AUTO-2025-009012",
                "carrier": "Progressive Commercial",
                "effective_date": "01/01/2025",
                "expiration_date": "01/01/2026",
                "coverage_limits": "COMBINED SINGLE LIMIT $1,000,000"
            }
        ]
    })


@pytest.fixture
def sample_raw_json_cancellation():
    """
    Returns a Raw JSON string from Claude extraction for a cancellation notice.
    """
    return json.dumps({
        "document_type": "Cancellation",
        "named_insured": "ABC Contractors LLC",
        "certificate_holder": "Smith General Contractors",
        "cancellation_date": "02/15/2025",
        "effective_date": "03/15/2025",
        "policies": [
            {
                "policy_type": "Commercial General Liability",
                "policy_number": "GL-2025-001234",
                "carrier": "Hartford Fire Insurance",
                "cancellation_reason": "Non-payment"
            }
        ]
    })


# ============================================================================
# HELPER FIXTURES
# ============================================================================

@pytest.fixture
def sample_record_with_id(sample_extraction_fields):
    """
    Returns a complete Airtable record dict with id and metadata.
    """
    return {
        'id': 'recIncomingExtraction123',
        'fields': sample_extraction_fields,
        'createdTime': datetime.now().isoformat(),
    }
