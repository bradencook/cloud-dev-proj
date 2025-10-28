# THIS FILE CREATED WITH HELP FROM ChatGPT

import unittest
from unittest.mock import MagicMock, patch
import json
import consumer


class TestConsumer(unittest.TestCase):

    def setUp(self):
        # Example widget object like the request queue provides
        self.test_widget = {
            "id": "123",
            "owner": "John Doe"
        }

    # --- Test S3 Request Retrieval ---
    def test_get_next_request_returns_widget(self):
        mock_client = MagicMock()

        # Mock list_objects_v2 returning a key
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "req1"}]
        }

        # Mock S3 obj retrieval
        widget_bytes = json.dumps(self.test_widget).encode("utf-8")
        mock_client.get_object.return_value = {"Body": MagicMock(read=lambda: widget_bytes)}

        widget = consumer.get_next_request(mock_client, "mybucket")

        self.assertEqual(widget, self.test_widget)
        mock_client.delete_object.assert_called_once()

    def test_get_next_request_none(self):
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}  # no Contents means no request

        widget = consumer.get_next_request(mock_client, "mybucket")
        self.assertIsNone(widget)

    # --- Test DynamoDB Update ---
    @patch("consumer.log")
    def test_update_dynamodb(self, mock_log):
        mock_table = MagicMock()

        consumer.update_dynamodb(mock_table, self.test_widget)

        mock_table.put_item.assert_called_once_with(Item=self.test_widget)
        mock_log.assert_called_once()

    # --- Test S3 Update ---
    @patch("consumer.log")
    def test_update_s3(self, mock_log):
        mock_client = MagicMock()

        consumer.update_s3(mock_client, "dest-bucket", self.test_widget)

        # Owner should become "john-doe" in key path
        expected_key = "widgets/john-doe/123"
        mock_client.put_object.assert_called_once()
        args, kwargs = mock_client.put_object.call_args
        self.assertEqual(kwargs["Key"], expected_key)
        self.assertIn(b"123", kwargs["Body"])  # JSON body contains ID

        mock_log.assert_called_once()

    # --- Test CLI Parsing ---
    def test_cl_parse_requires_bucket_and_one_option(self):
        test_args = ["-rb", "req-bucket", "-wb", "widget-bucket"]

        with patch("argparse._sys.argv", ["consumer.py"] + test_args):
            args = consumer.cl_parse()
            self.assertEqual(args.request_bucket, "req-bucket")
            self.assertEqual(args.widget_bucket, "widget-bucket")
            self.assertIsNone(args.dynamodb_widget_table)

class TestConsumerMainLoop(unittest.TestCase):

    def setUp(self):
        self.create_request = {
            "type": "create",
            "widgetId": "123",
            "requestId": "abc",
            "owner": "Jane Doe",
            "otherAttributes": [
                {"name": "color", "value": "red"}
            ]
        }

        self.skip_request = {
            "type": "delete",
            "widgetId": "999",
            "requestId": "zzz",
            "owner": "X",
            "otherAttributes": []
        }

    @patch("consumer.update_s3")
    def test_process_next_widget_stores_create(self, mock_update_s3):
        mock_client = MagicMock()
        body = json.dumps(self.create_request).encode("utf-8")

        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "req1"}]
        }
        mock_client.get_object.return_value = {"Body": MagicMock(read=lambda: body)}

        result = consumer.process_next_widget(
            mock_client, "req-bucket", table=None, widget_bucket="out-bucket"
        )

        self.assertIsInstance(result, dict)
        self.assertEqual(result["id"], "123")
        mock_update_s3.assert_called_once()

    @patch("consumer.log")
    def test_process_next_widget_skips_noncreate(self, mock_log):
        mock_client = MagicMock()
        body = json.dumps(self.skip_request).encode("utf-8")

        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "req2"}]
        }
        mock_client.get_object.return_value = {"Body": MagicMock(read=lambda: body)}

        result = consumer.process_next_widget(
            mock_client, "req-bucket", table=None, widget_bucket="out-bucket"
        )

        self.assertEqual(result, "skipped")
        mock_log.assert_called_once()

    def test_process_next_widget_none_when_empty(self):
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}

        result = consumer.process_next_widget(
            mock_client, "req-bucket", table=None, widget_bucket=None
        )

        self.assertIsNone(result)
        
if __name__ == "__main__":
    unittest.main()
