import unittest
from unittest.mock import MagicMock
from module_9_vendor_notification_scheduler import log_vendor_notifications


class TestVendorNotificationScheduler(unittest.TestCase):

    def test_log_vendor_notifications(self):
        api = MagicMock()

        insurance_policies_table = MagicMock()
        insurance_policies_table.all.return_value = [
            {'fields': {'Vendor': ['vendor_1'], 'Expiration Status': 'Expiring in 30 Days'}},
            {'fields': {'Vendor': ['vendor_2'], 'Expiration Status': 'Expired'}},
        ]

        vendors_table = MagicMock()
        vendors_table.get.side_effect = lambda vendor_id: {
            'vendor_1': {'fields': {'Vendor Name': 'Vendor One'}},
            'vendor_2': {'fields': {'Vendor Name': 'Vendor Two'}},
        }.get(vendor_id, {'fields': {'Vendor Name': 'Unknown Vendor'}})

        def table_side_effect(_base_id, table_name):
            if table_name == 'Insurance Policies':
                return insurance_policies_table
            if table_name == 'Vendors':
                return vendors_table
            return MagicMock()

        api.table.side_effect = table_side_effect

        # Capture the logging output
        with self.assertLogs(level='INFO') as log:
            log_vendor_notifications(api)

        insurance_policies_table.all.assert_called_once_with(
            formula="OR({Expiration Status} = 'Expiring in 30 Days', {Expiration Status} = 'Expired')"
        )

        # Verify the log messages
        self.assertIn('INFO:root:Vendor needs expiration notice: Vendor One — Expiring in 30 Days', log.output)
        self.assertIn('INFO:root:Vendor needs expiration notice: Vendor Two — Expired', log.output)
        self.assertIn('INFO:root:Compliance/requirement-based reminder triggers are disabled.', log.output)

if __name__ == '__main__':
    unittest.main()