# from ..manager import RemoteManager

import os
import io

import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload

class Drive:

    def __init__(self):

        self._creds, self._project = google.auth.default()
        print(self._project)
        self._build = build('drive', 'v3', credentials=self._creds)

    def _ls(self, artefact):

        while True:
            response = self._build.files().list().execute()
            print(dir(response))

            for file in response.get('files', []):
                # Process change
                print(F'Found file: {file.get("name")}, {file.get("id")}')

            break

    def _put(self, bytes_: bytes, destination: str):

        metadata = {
            'name': destination
        }
        ioBytes = io.BytesIO(bytes_)
        media = MediaIoBaseUpload(ioBytes, mimetype='text/plain')
        file = self._build.files().create(body=metadata, media_body=media).execute()
        print(f'File ID: {file.get("id")}')


if __name__ == "__main__":

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\kieran\Downloads\default-384021-1b912c94a251.json"

    manager = Drive()

    manager._put(b'here are some content', '/tmp/file.txt')
    manager._ls('/myfiles')