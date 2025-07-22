from process_cae_retriever import parse_event
from process_cae_retriever import run_cae_retriever as main_function


def lambda_handler(event, context):
    """
    lambda_handler - lambda function
    """
    kwargs = parse_event(event, main_function)

    res = main_function(**kwargs)

    return {
        "statusCode": 200, 
        "body": {
            "result": res   
        }
    }


if __name__ == "__main__":
    event = {
        "dem": "s3://saferplaces.co/packages/safer_rain/CLSA_LiDAR/CLSA_LiDAR.tif",
        "debug": "false"
    }

    kwargs = parse_event(event, main_function)
    res = main_function(**kwargs)
    print(res)
