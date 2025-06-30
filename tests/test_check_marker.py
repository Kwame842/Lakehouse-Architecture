import check_marker_lambda

def test_marker_check_false(monkeypatch):
    class FakeS3Client:
        def head_object(self, Bucket, Key):
            raise check_marker_lambda.s3.exceptions.ClientError({}, 'head_object')

    # Replace boto3 client with our fake one
    monkeypatch.setattr(check_marker_lambda, "s3", FakeS3Client())
    
    result = check_marker_lambda.lambda_handler({"dataset": "products"}, {})
    assert result["already_processed"] is False
