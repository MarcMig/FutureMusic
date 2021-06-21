import os
import boto3
import requests
from urllib.parse import urlparse
from time import sleep
from secret_credentials import access_key_ID, secret_access_ID
import pandas as pd
from boto3 import resource
from smart_open import open
from boto3.s3.transfer import TransferConfig


# to later update metadata with e.g. comments: https://stackoverflow.com/questions/4754383/how-to-change-metadata-on-an-object-in-amazon-s3#4754439


def s3_save_track(
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

    ## getting .wav data, storing it into handle file object
    # with open(file_name, 'wb+') as handle:
    response = requests.get(track_url, stream=True)
    print(response, response.status_code)
    if response.status_code == 200:

        raw_data = response.content
        url_parser = urlparse(
            track_url
        )  # try something else than urlparse? download locally?
        file_name_server = os.path.basename(url_parser.path)
        file_name = f"{int(track_id)}.wav"
        file_key = folder + "/" + file_name
        s3_url = "s3://" + bucket + "/" + file_key
        # client.create_multipart_upload(Bucket=bucket, Key=file_key)
        # try:
        # Write the raw data as bytes in new file_name in the server
        with open(
            s3_url,
            "wb",
            transport_params={
                # "client": client,
                "multipart_upload": False
                # "client_kwargs": {
                #     "S3.Client.create_multipart_upload": {
                #         "Bucket": bucket,
                #         "Key": file_key,
                #         "Metadata": {
                #             "artist_name": artist_name,
                #             "track_name": track_name,
                #             "artist_id": str(artist_id),
                #             "track_id": str(track_id)
                #             #
                #         },
                #     }
                # },
            },
        ) as new_file:

            for chunk in response.iter_content(chunk_size=1024 * 1024):
                # writing one chunk at a time to wav file
                if chunk:
                    new_file.write(chunk)

            if verbose:
                print("Read response file, wrote data")

            # open the server file as read mode and upload it in bucket
            # data = open(f"{track_id}.wav", "rb")

            # client.Bucket(bucket).put_object(
            #     Key=file_key,
            #     Body=data,
            #     Metadata={
            #         "artist_name": artist_name,
            #         "track_name": track_name,
            #         "artist_id": str(artist_id),
            #         "track_id": str(track_id)
            #         #
            #     }
            #     #
            # )
            # data.close()
            new_file.close()

        # Format the return URL of upload file in S3 Bucket
        file_url = "https://%s.%s/%s" % (bucket, endpoint, file_key)

        if verbose:
            print("Attachment Successfully save in S3 Bucket, url: %s " % (file_url))

        return file_url

        # except Exception as e:
        #     print("Error in file upload %s." % (str(e)))

        # finally:
        # Close and remove file from Server

        # sleep(1)
        # os.remove(file_name)
    # else:
    #     print(response.status_code)


def downloader(n="all", verbose=True):

    ## setting up AWS S3 client
    s3_client = boto3.client(
        "s3", aws_access_key_id=access_key_ID, aws_secret_access_key=secret_access_ID
    )

    s3_endpoint = "s3.eu-west-3.amazonaws.com"

    song_urls = pd.read_csv("track_urls_10.csv")

    if n != "all":
        song_urls = song_urls.iloc[
            :n,
        ]

    for i in range(len(song_urls)):
        a_name = song_urls["artist_name"][i]
        a_id = song_urls["artist_id"][i]
        t_name = song_urls["track_name"][i]
        t_id = song_urls["track_id"][i]
        t_url = song_urls["download_url"][i]

        if verbose:
            print(a_name, ": ", t_name)

        s3_save_track(
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
    downloader(n=1)
