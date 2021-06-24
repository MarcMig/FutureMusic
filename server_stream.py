import asyncio
import logging
import boto3
from botocore.exceptions import ClientError
from botocore.vendored.six import BytesIO
import requests
import urllib.parse
from secret_credentials import access_key_ID, secret_access_ID
import pandas as pd
import aiohttp


async def create_presigned_post(
    bucket_name, object_name, s3_client, fields=None, conditions=None, expiration=3600
):
    try:  # need aiobotocore? add metadata?
        response = await s3_client.generate_presigned_post(
            bucket_name,
            object_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=expiration,
        )
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL and required fields
    return response


class Streamer:
    def __init__(
        self,
        track_id,
        download_url,
        client,
        bucket="sound-scraping",
        folder="track-files",
    ):

        self.file_name = f"{int(track_id)}.wav"
        self.file_key = folder + "/" + self.file_name
        self.bucket = bucket
        self.download_url = download_url
        self.IP = "127.0.0.1"
        self.port = 8888
        self.next_IP = "52.95.156.0"  # use url.hostname instead?
        self.next_port = 24
        self.client = client

    async def server_callback(self, reader, writer):
        # callback called whenever a client connection is established, receives reader and writer. is it just client_method()?
        payload = {"query": ""}

        # sending post request to url link to start stream. do we even need to send request or is reader.read() enough?
        async with aiohttp.ClientSession(auto_decompress=False) as session:
            await session.post(self.download_url, data=payload)

        data = await reader.read()  # need to decode?
        # message = data.decode()

        writer.write(data)
        await writer.drain()

        writer.close()  # close the connection?
        await writer.wait_closed()  # need?

    async def run(self):
        self.next_reader, self.next_writer = await asyncio.open_connection(
            self.next_IP, self.next_port
        )

        # server part coroutine accepts the connection from download server
        server_coro = asyncio.create_task(self.server_init())

        # client part coroutine opens a connection to presigned_url server
        client_coro = asyncio.create_task(self.client_method())
        await client_coro
        await server_coro

    async def server_init(self):
        server = await asyncio.start_server(
            self.server_callback, self.IP, self.port
        )  # use upload_url.hostname instead?

        addr = server.sockets[0].getsockname()
        print(f"Serving on {addr}")

        async with server:
            await server.serve_forever()

    async def client_method(self):
        try:
            data = await self.next_reader.read()
            presigned_response = create_presigned_post(
                self.bucket, self.file_key, self.client
            )
            upload_url = presigned_response["url"]
            upload_url = urllib.parse.urlsplit(upload_url)
            print(upload_url.hostname)

            files = {
                "file": (self.file_key, data)
            }  # use put instead? response = requests.put(url, data=object_text)
            async with aiohttp.ClientSession(auto_decompress=False) as session:
                http_response = await session.post(
                    presigned_response["url"],
                    data=presigned_response["fields"],
                    files=files,
                )  # data = response['fields']? or data=data?

            # If successful, returns HTTP status code 204
            logging.info(f"File upload HTTP status code: {http_response.status_code}")

        except ConnectionError as e:
            print(e)


async def stream(i, tracks_df, client, verbose=True):
    a_name = tracks_df["artist_name"][i]
    a_id = tracks_df["artist_id"][i]
    t_name = tracks_df["track_name"][i]
    t_id = tracks_df["track_id"][i]
    t_url = tracks_df["download_url"][i]

    if verbose:
        print(a_name, ": ", t_name)

    server = Streamer(t_id, t_url, client)
    server.run()


async def main(n="all", start=0, verbose=True):

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

    await asyncio.gather(
        *[stream(n, song_urls, s3_client) for n in range(len(song_urls))]
    )

    if verbose:
        print("Downloads completed!")


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(streamer(start=1))
    # streamer()
