import unittest
from unittest.mock import patch, MagicMock
import logging
from module_9_vendor_notification_scheduler import log_vendor_notifications

class TestVendorNotificationScheduler(unittest.TestCase):

    @patch('module_9_vendor_notification_scheduler.Table')
    def test_log_vendor_notifications(self, MockTable):
        # Mock the Airtable Table responses
        mock_table_instance = MockTable.return_value
        mock_table_instance.all.side_effect = [
            # Mock response for get_expiring_policies
            [
                {'fields': {'Vendor': 'vendor_1', 'Expiration Status': 'Expiring in 30 Days'}},
                {'fields': {'Vendor': 'vendor_2', 'Expiration Status': 'Expired'}}
            ],
            # Mock response for get_non_compliant_assignments
            [
                {'fields': {'Vendor': 'vendor_1', 'Compliance Status': 'Missing Coverage'}},
                {'fields': {'Vendor': 'vendor_3', 'Compliance Status': 'Needs Review'}}
            ]
        ]
        mock_table_instance.get.side_effect = lambda vendor_id: {
            'vendor_1': {'fields': {'Name': 'Vendor One'}},
            'vendor_2': {'fields': {'Name': 'Vendor Two'}},
            'vendor_3': {'fields': {'Name': 'Vendor Three'}}
        }.get(vendor_id, {'fields': {'Name': 'Unknown Vendor'}})

        # Capture the logging output
        with self.assertLogs(level='INFO') as log:
            log_vendor_notifications()

        # Verify the log messages
        self.assertIn('INFO:root:Vendor needs expiration notice: Vendor One — Expiring in 30 Days', log.output)
        self.assertIn('INFO:root:Vendor needs expiration notice: Vendor Two — Expired', log.output)
        self.assertIn('INFO:root:Vendor needs compliance notice: Vendor One — Missing Coverage', log.output)
        self.assertIn('INFO:root:Vendor needs compliance notice: Vendor Three — Needs Review', log.output)

if __name__ == '__main__':
    unittest.main()