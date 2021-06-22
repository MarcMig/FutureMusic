import asyncio
import logging
import boto3
from botocore.exceptions import ClientError
import requests
import urllib.parse
from secret_credentials import access_key_ID, secret_access_ID
import pandas as pd


def multipart_stream(
    track_url,
    artist_id,
    track_id,
    artist_name,
    track_name,
    client,
    endpoint,  # "eu-west-3",
    folder="track-files",
    bucket="sound-scraping",
    verbose=True,
):

    payload = {"query": ""}  # need this?
    file_name = f"{int(track_id)}.wav"
    file_key = folder + "/" + file_name
    s3 = client
    session = requests.Session()
    response = session.post(track_url, data=payload, stream=True)
    print(response)

    with response as part:
        part.raw.decode_content = True
        conf = boto3.s3.transfer.TransferConfig(
            multipart_threshold=10000, max_concurrency=10
        )
        s3.upload_fileobj(
            part.raw,
            bucket,
            file_key,
            Config=conf,
            ExtraArgs={
                "Metadata": {
                    "artist_name": artist_name,
                    "track_name": track_name,
                    "artist_id": str(int(artist_id)),
                    "track_id": str(track_id)
                    #
                }
            },
        )
        # add metadata
        print("Uploaded object")


# def create_presigned_post(
#     bucket_name, object_name, fields=None, conditions=None, expiration=3600
# ):
#     """Generate a presigned URL S3 POST request to upload a file

#     :param bucket_name: string
#     :param object_name: string
#     :param fields: Dictionary of prefilled form fields
#     :param conditions: List of conditions to include in the policy
#     :param expiration: Time in seconds for the presigned URL to remain valid
#     :return: Dictionary with the following keys:
#         url: URL to post to
#         fields: Dictionary of form fields and values to submit with the POST
#     :return: None if error.
#     """

#     # Generate a presigned S3 POST URL
#     s3_client = boto3.client("s3")
#     try:
#         response = s3_client.generate_presigned_post(
#             bucket_name,
#             object_name,
#             Fields=fields,
#             Conditions=conditions,
#             ExpiresIn=expiration,
#         )
#     except ClientError as e:
#         logging.error(e)
#         return None

#     # The response contains the presigned URL and required fields
#     return response


# def post_request(file_name, bucket_name):
#     # Generate a presigned S3 POST URL
#     response = create_presigned_post(bucket_name, file_name)
#     if response is None:
#         exit(1)

#     # Demonstrate how another Python program can use the presigned URL to upload a file
#     with open(file_name, "rb") as f:
#         files = {"file": (file_name, f)}
#         http_response = requests.post(
#             response["url"], data=response["fields"], files=files
#         )
#     # If successful, returns HTTP status code 204
#     logging.info(f"File upload HTTP status code: {http_response.status_code}")


# async def read_stream(download_url):
#     # creating a read stream from the remote wav file
#     download_url = urllib.parse.urlsplit(download_url)
#     reader, writer = await asyncio.open_connection(
#         download_url.hostname, 8888
#     )  # how to make it listen to particular link/request?
#     data = await reader.read()
#     writer.write(data)
#     await writer.drain()
#     writer.close()  # close the connection?


# async def send_stream(track_id):
#     s3_url = post_request(f"track_files/{track_id}.wav", "sound-scraping")
#     server = await asyncio.start_server(read_stream, s3_url)


# for url in ...:
#     asyncio.run(stream(url, track_id))


def streamer(n="all", start=0, verbose=True):

    session = boto3.Session(
        aws_access_key_id=access_key_ID, aws_secret_access_key=secret_access_ID
    )
    ## setting up AWS S3 client
    s3_client = session.client("s3")

    s3_endpoint = "s3.eu-west-3.amazonaws.com"

    song_urls = pd.read_csv("track_urls_10.csv")

    if n != "all":
        song_urls = song_urls.iloc[
            :n,
        ]

    if start != 0:
        song_urls = song_urls.iloc[
            start:,
        ]

    for i in tqdm(range(len(song_urls))):
        a_name = song_urls["artist_name"][i]
        a_id = song_urls["artist_id"][i]
        t_name = song_urls["track_name"][i]
        t_id = song_urls["track_id"][i]
        t_url = song_urls["download_url"][i]

        if verbose:
            print(a_name, ": ", t_name)

        multipart_stream(
            track_url=t_url,
            artist_id=a_id,
            track_id=t_id,
            artist_name=a_name,
            track_name=t_name,
            client=s3_client,
            endpoint=s3_endpoint,
        )

    if verbose:
        print("Downloads completed!")


# HOW TO MAKE IT LAMBDA?
if __name__ == "__main__":
    # asyncio.run(streamer(n=1))
    # asyncio.run(streamer(start=1))
    streamer()
