import requests
import base64
import hmac
from hashlib import sha1
from datetime import datetime
import io
import xmltodict
import os
import logging
from typing import Union

# Logging setup
log = logging.getLogger("miniS3")
log.addHandler(logging.NullHandler())

class Client:
    def __init__(self, access_key: str, secret_key: str, region: str, server: str = None, encryption="AES256") -> None:
        self.region = region
        self.server = server or f"https://s3-{self.region}.amazonaws.com"
        self.access_key = access_key
        self.secret_key = secret_key
        self.date_format = "%a, %d %b %Y %H:%M:%S +0000"
        self.encryption = encryption

    def make_signed_request(self, method: str, url: str, key: str) -> Union[requests.Response, None]:
        date = datetime.utcnow().strftime(self.date_format)
        signature = self.create_aws_signature(date, key, method)
        headers = {"Authorization": signature, "Date": date}
        
        try:
            response = requests.request(method, url, headers=headers, stream=True)
            if response.status_code != 200:
                log.error(f"Failed to perform {method} request to {key}")
                log.error(response.text)
                return None
            return response
        except Exception as error:
            log.error(f"Failed to perform {method} request to {key}")
            log.error(error)
            return None

    def list_objects(self, Bucket: str, Prefix: str) -> list:
        url = f"{self.server}/{Bucket}/?list-type=2&prefix={Prefix}"
        key = f"{Bucket}/"
        response = self.make_signed_request("GET", url, key)
        
        if response:
            data = self.get_bucket_keys(response.text, Prefix)
        else:
            data = []
        return data

    def get_object(self, Bucket: str, Key: str) -> bool:
        url = f"{self.server}/{Bucket}/{Key}"
        key = f"{Bucket}/{Key}"
        response = self.make_signed_request("GET", url, key)
        
        if response:
            exists = True
        else:
            exists = False
        return exists

    def download_file(self, Bucket: str, Key: str, Filename: str) -> str:
        url, key = self.build_vars(Key, Bucket)
        date = datetime.utcnow().strftime(self.date_format)
        signature = self.create_aws_signature(date, key, "GET")
        headers = {"Authorization": signature, "Date": date}

        try:
            response = requests.get(url=url, headers=headers, stream=True)
            if response.status_code == 200:
                self.create_download_folders(Filename)
                with open(Filename, "wb") as file_handle:
                    for chunk in response.iter_content(chunk_size=128):
                        file_handle.write(chunk)
            else:
                Filename = ""
                log.error(f"Something went wrong downloading {key}")
                log.error(response.text)
        except Exception as error:
            Filename = ""
            log.error(f"Something went wrong downloading {key}")
            log.error(error)
        return Filename

    # Other methods...

    def create_aws_signature(self, date, key, method) -> str:
        string_to_sign = f"{method}\n\n\n{date}\n/{key}".encode("UTF-8")
        signature = base64.encodebytes(hmac.new(self.secret_key.encode("UTF-8"), string_to_sign, sha1).digest()).strip()
        return f"AWS {self.access_key}:{signature.decode()}"

    def build_vars(self, file_name: str, bucket_name) -> (str, str):
        s3_url = f"{self.server}/{bucket_name}/{file_name}"
        s3_key = f"{bucket_name}/{file_name}"
        return s3_url, s3_key
