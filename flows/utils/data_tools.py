import os
import time
import tarfile
from typing import Union, Dict
from metaflow import S3
from metaflow.metaflow_config import DATATOOLS_S3ROOT


class DirectorySyncManager:
    def __init__(self, root, s3_root=None, run=None):
        self.root = os.path.relpath(path=root, start=os.getcwd())
        self.s3_root = s3_root
        self.run = run

    def _get_tar_bytes(self):
        "Zip from the root of the directory and return the bytes of the tar file."
        with tarfile.open(f"{self.root}.tar.gz", "w:gz") as tar:
            tar.add(self.root, arcname=os.path.basename(self.root))
        with open(f"{self.root}.tar.gz", "rb") as f:
            tar_bytes = f.read()
        return tar_bytes

    def _get_s3_client(self):
        "Return an S3 object based on the run or s3_root."
        if self.run:
            return S3(run=self.run)
        elif self.s3_root:
            return S3(s3root=self.s3_root)
        else:
            return S3(s3root=os.path.join(DATATOOLS_S3ROOT, self.root))

    def _upload_to_s3(self, tar_bytes):
        "Push the tar file to S3."
        s3 = self._get_s3_client()
        if s3 is None:
            return None
        else:
            self.s3_path = s3.put(f"{self.root}.tar.gz", tar_bytes)
        s3.close()

    def _download_from_s3(
        self, all_nodes: bool = False
    ) -> Union[bytes, Dict[str, bytes]]:
        "Pull the tar file(s) from S3."
        s3 = self._get_s3_client()
        candidate_paths = s3.list_paths()
        if all_nodes:
            tar_balls = {}
            for s3obj in candidate_paths:
                if self.root in s3obj.key:
                    obj = s3.get(s3obj.key)
                    tar_balls[obj.key] = obj.blob
            s3.close()
            return tar_balls
        else:
            tar_bytes = s3.get(f"{self.root}.tar.gz").blob
        s3.close()
        return tar_bytes

    def _extract_tar(self, tar_bytes, path=None):
        """
        Extract the tar file to the root of the directory.
        If `path` is specified, assumed to be a file path and extract to that location.
        The use case for path is
        """
        if path:
            with open(path, "wb") as f:
                f.write(tar_bytes)
            with tarfile.open(path, "r:gz") as tar:
                tar.extractall(path=path.replace(".tar.gz", ""))
            os.remove(path)
        else:
            with open(f"{self.root}.tar.gz", "wb") as f:
                f.write(tar_bytes)
            with tarfile.open(f"{self.root}.tar.gz", "r:gz") as tar:
                tar.extractall(path=os.path.dirname(self.root))
            os.remove(f"{self.root}.tar.gz")

    def download(self):
        tar_bytes = self._download_from_s3()
        self._extract_tar(tar_bytes)

    def upload(self):
        tar_bytes = self._get_tar_bytes()
        self.s3_path = self._upload_to_s3(tar_bytes)
