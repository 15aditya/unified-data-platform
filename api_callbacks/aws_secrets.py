import boto3
import json

def get_secret(secret_name: str, region_name: str = "eu-central-1") -> dict:
    """
    Fetch a secret from AWS Secrets Manager and return as dict.
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise RuntimeError(f"Error fetching secret {secret_name}: {e}")

    # Secrets Manager returns a string
    if "SecretString" in response:
        return json.loads(response["SecretString"])
    else:
        return json.loads(response["SecretBinary"].decode("utf-8"))
