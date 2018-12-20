import os
import json
import requests


class NomadException(Exception):
    pass


class Nomad:
    def __init__(self, host="127.0.0.1", port=4646, timeout=5):
        if host != "127.0.0.1" or port != 4646:
            self._address = f"http://{host}:{port}"
        else:
            self._address = os.getenv("NOMAD_ADDR", f"http://{host}:{port}")
        self._timeout = timeout
        self._session = requests.Session()

    def dispatch_job(self, job_id, **kwargs):
        url = f"{self._address}/v1/job/{job_id}/dispatch"
        try:
            rsp = self._session.post(url=url, json=kwargs, timeout=self._timeout)
        except Exception as e:
            raise NomadException from e
        print(rsp.text)
        return rsp.json()

    def __getattr__(self, attr):
        method, *endpoints = attr.split("_")

        def fn(id=None, **kwargs):
            url = f'{self._address}/v1/{"/".join(endpoints)}'
            if id is not None:
                url += f"/{id}"
            try:
                if method == "get":
                    rsp = self._session.request(
                        method=method, url=url, params=kwargs, timeout=self._timeout
                    )
                else:
                    rsp = self._session.request(
                        method=method, url=url, json=kwargs, timeout=self._timeout
                    )
            except Exception as e:
                raise NomadException from e
            print(rsp.text)
            return rsp.json()

        return fn
