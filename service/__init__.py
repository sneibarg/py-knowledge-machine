import requests
from requests import Session


def get_session(max_retries=0) -> Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=max_retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
