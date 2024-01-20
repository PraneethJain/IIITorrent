import functions_framework
from google.cloud import storage


@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    
    request_json = request.get_json(silent=True)
    magnet_link = request_json["magnet"]
    print(magnet_link)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("iiitorrent")
    blob = bucket.blob("ok.txt")
    blob.upload_from_string("random text pls word")

    return "test!"

