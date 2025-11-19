# THIS FILE CREATED WITH HELP FROM ChatGPT

import unittest
from unittest.mock import MagicMock, patch
import json
import consumer


class TestS3RequestSource(unittest.TestCase):

    def setUp(self):
        self.widget = {"id": "123", "owner": "John Doe"}

    def test_s3_get_next_returns_widget(self):
        mock_client = MagicMock()

        # Mock S3 to return 1 object
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "req1"}]
        }

        # Mock the body bytes
        body_bytes = json.dumps(self.widget).encode("utf-8")
        mock_client.get_object.return_value = {
            "Body": MagicMock(read=lambda: body_bytes)
        }

        source = consumer.S3RequestSource(mock_client, "bucket")
        result = source.get_next()

        self.assertEqual(result, self.widget)
        mock_client.delete_object.assert_called_once()

    def test_s3_get_next_none(self):
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}

        source = consumer.S3RequestSource(mock_client, "bucket")
        result = source.get_next()

        self.assertIsNone(result)


class TestSQSRequestSource(unittest.TestCase):

    def setUp(self):
        self.widget = {"type": "create", "widgetId": "123"}

    def test_sqs_get_next_returns_one_and_caches(self):
        sqs_client = MagicMock()

        # Return 2 messages to simulate batch retrieval
        sqs_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(self.widget),
                    "ReceiptHandle": "rh1",
                },
                {
                    "Body": json.dumps(self.widget),
                    "ReceiptHandle": "rh2",
                },
            ]
        }

        src = consumer.SQSRequestSource(sqs_client, "queue-url")

        first = src.get_next()
        self.assertEqual(first["widgetId"], "123")
        self.assertEqual(first["_receipt_handle"], "rh1")

        # Only one receive_message call expected
        sqs_client.receive_message.assert_called_once()

        second = src.get_next()
        self.assertEqual(second["_receipt_handle"], "rh2")

    def test_sqs_get_next_none(self):
        sqs_client = MagicMock()

        sqs_client.receive_message.return_value = {"Messages": []}

        src = consumer.SQSRequestSource(sqs_client, "queue-url")
        self.assertIsNone(src.get_next())

    def test_sqs_delete_processed(self):
        sqs_client = MagicMock()
        src = consumer.SQSRequestSource(sqs_client, "queue-url")

        src.delete_processed("abc123")
        sqs_client.delete_message.assert_called_once_with(
            QueueUrl="queue-url",
            ReceiptHandle="abc123"
        )


class TestStorageBackends(unittest.TestCase):

    def setUp(self):
        self.widget = {"id": "123", "owner": "John Doe"}

    @patch("consumer.log")
    def test_update_dynamodb(self, mock_log):
        mock_table = MagicMock()
        consumer.update_dynamodb(mock_table, self.widget)
        mock_table.put_item.assert_called_once_with(Item=self.widget)
        mock_log.assert_called_once()

    @patch("consumer.log")
    def test_update_s3(self, mock_log):
        mock_client = MagicMock()
        consumer.update_s3(mock_client, "out-bucket", self.widget)

        args, kwargs = mock_client.put_object.call_args
        self.assertIn("widgets/john-doe/123", kwargs["Key"])
        self.assertIn(b"123", kwargs["Body"])
        mock_log.assert_called_once()


class TestProcessWidget(unittest.TestCase):

    def setUp(self):
        self.create_req = {
            "type": "create",
            "widgetId": "123",
            "requestId": "r1",
            "owner": "Jane Doe",
            "otherAttributes": [{"name": "color", "value": "red"}],
        }

        self.update_req = {
            "type": "update",
            "widgetId": "123",
            "requestId": "r2",
            "owner": "Jane Doe",
            "otherAttributes": [{"name": "size", "value": "large"}],
        }

        self.delete_req = {
            "type": "delete",
            "widgetId": "123",
            "requestId": "r3",
            "owner": "Jane Doe",
            "otherAttributes": [],
        }

    def test_process_create(self):
        storage = {
            "update": MagicMock(),
            "delete": MagicMock(),
        }
        out = consumer.process_widget(self.create_req, storage)
        storage["update"].assert_called_once()
        self.assertEqual(out["id"], "123")
        self.assertEqual(out["color"], "red")

    def test_process_update(self):
        storage = {
            "update": MagicMock(),
            "delete": MagicMock(),
        }
        out = consumer.process_widget(self.update_req, storage)
        storage["update"].assert_called_once()
        self.assertEqual(out["size"], "large")

    def test_process_delete(self):
        storage = {
            "update": MagicMock(),
            "delete": MagicMock(),
        }
        out = consumer.process_widget(self.delete_req, storage)
        storage["delete"].assert_called_once_with("123")
        self.assertEqual(out, {"deleted": "123"})


class TestCLI(unittest.TestCase):

    def test_cl_parse_s3(self):
        args = ["consumer.py", "-rb", "req-bucket", "-wb", "out-bucket"]
        with patch("argparse._sys.argv", args):
            parsed = consumer.cl_parse()
            self.assertEqual(parsed.request_bucket, "req-bucket")
            self.assertEqual(parsed.widget_bucket, "out-bucket")
            self.assertIsNone(parsed.sqs_queue)

    def test_cl_parse_sqs(self):
        args = ["consumer.py", "-sq", "myqueue", "-dwt", "TableA"]
        with patch("argparse._sys.argv", args):
            parsed = consumer.cl_parse()
            self.assertEqual(parsed.sqs_queue, "myqueue")
            self.assertEqual(parsed.dynamodb_widget_table, "TableA")
            self.assertIsNone(parsed.request_bucket)


if __name__ == "__main__":
    unittest.main()
