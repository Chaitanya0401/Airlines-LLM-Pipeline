import os
from dotenv import load_dotenv

load_dotenv()

def test_env_variables_exist():
    assert os.getenv("AVIATIONSTACK_API_KEY") is not None
    assert os.getenv("S3_BUCKET_NAME") is not None
    assert os.getenv("AWS_REGION") is not None
