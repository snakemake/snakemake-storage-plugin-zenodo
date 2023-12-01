from collections import namedtuple
import os
import requests
from snakemake_interface_common.exceptions import WorkflowError


JSON_MIME = "application/json"


ZenFileInfo = namedtuple(
    "ZenFileInfo", ["filename", "checksum", "filesize", "download"]
)


class ZENHelper:
    def __init__(self, settings, deposition=None, is_record=True):
        try:
            self._access_token = settings.access_token
        except KeyError:
            raise WorkflowError(
                "Zenodo personal access token must be passed in as 'access_token' "
                "argument.\n"
                "Separate registration and access token is needed for Zenodo sandbox "
                "environment at https://sandbox.zenodo.org."
            )

        self.restricted_access_token = settings.restricted_access_token
        self._restricted_access_cookies = None

        self._sandbox = settings.sandbox

        if self._sandbox:
            self._baseurl = "https://sandbox.zenodo.org"
        else:
            self._baseurl = "https://zenodo.org"

        self.is_new_deposition = not is_record
        self.deposition = deposition
        self._bucket = None

    def _api_request(
        self,
        url,
        method="GET",
        data=None,
        headers={},
        files=None,
        json=False,
        restricted_access=True,
    ):
        # Create a session with a hook to raise error on bad request.
        session = requests.Session()
        session.hooks = {"response": lambda r, *args, **kwargs: r.raise_for_status()}
        session.headers["Authorization"] = f"Bearer {self._access_token}"
        session.headers.update(headers)

        cookies = self.restricted_access_cookies if restricted_access else None

        # Run query.
        r = session.request(
            method=method, url=url, data=data, files=files, cookies=cookies
        )
        if json:
            msg = r.json()
            return msg
        else:
            return r

    @property
    def bucket(self):
        if self._bucket is None:
            resp = self._api_request(
                self._baseurl + f"/api/deposit/depositions/{self.deposition}",
                headers={"Content-Type": JSON_MIME},
                json=True,
            )
            self._bucket = resp["links"]["bucket"]
        return self._bucket

    def get_files(self):
        if self.is_new_deposition:
            return self.get_files_own_deposition()
        else:
            return self.get_files_record()

    def get_files_own_deposition(self):
        files = self._api_request(
            self._baseurl + f"/api/deposit/depositions/{self.deposition}/files",
            headers={"Content-Type": JSON_MIME},
            json=True,
        )
        return {
            os.path.basename(f["filename"]): ZenFileInfo(
                f["filename"], f["checksum"], int(f["filesize"]), f["links"]["download"]
            )
            for f in files
        }

    def get_files_record(self):
        resp = self._api_request(
            self._baseurl + f"/api/records/{self.deposition}",
            headers={"Content-Type": JSON_MIME},
            json=True,
        )
        if "files" not in resp:
            raise WorkflowError(
                "No files found in zenodo deposition "
                f"https://zenodo.org/record/{self.deposition}. "
                "Either the depositon is empty or access is restricted. Please check "
                "in your browser."
            )
        files = resp["files"]

        def get_checksum(f):
            checksum = f["checksum"]
            if checksum.startswith("md5:"):
                return checksum[4:]
            else:
                raise ValueError(
                    "Unsupported checksum (currently only md5 support is "
                    f"implemented for Zenodo): {checksum}"
                )

        return {
            f["key"]: ZenFileInfo(
                f["key"], get_checksum(f), int(f["size"]), f["links"]["self"]
            )
            for f in files
        }

    @property
    def restricted_access_cookies(self):
        """Retrieve cookies necessary for restricted access.

        Inspired by https://gist.github.com/slint/d47fe5628916d14b8d0b987ac45aeb66
        """
        if self.restricted_access_token and self._restricted_access_cookies is None:
            url = (
                self._baseurl
                + f"/record/{self.deposition}?token={self.restricted_access_token}"
            )
            resp = self._api_request(url, restricted_access=False)
            if "session" in resp.cookies:
                self._restricted_access_cookies = resp.cookies
            else:
                raise WorkflowError(
                    "Failure to retrieve session cookie with given restricted "
                    "access token. "
                    f"Is the token valid? Please check by opening {url} manually in "
                    "your browser."
                )
        return self._restricted_access_cookies
