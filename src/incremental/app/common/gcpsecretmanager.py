import json
from google.cloud import secretmanager


def get_secret(request: str) -> str:
    """
    function to get secret from secret manager
    @param request: str.
    @return: result: str.
    """
    # instantiates Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request)

    # access the secret version
    secret_payload = response.payload.data.decode("UTF-8")

    result = secret_parser(secret_payload)

    return result


def secret_parser(secret_payload: object) -> str:
    """
    function to get secret value from the secret request parameter
    @param secret_payload: object.
    @return: result: list.
    """
    result = str(secret_payload)
    return result


def secret_to_json(secret_payload: str) -> json:
    """
    function to convert secret payload into JSON
    @param secret_payload: str.
    @return: result: json.
    """
    result = json.loads(secret_payload)
    return result
