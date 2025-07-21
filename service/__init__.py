import requests


def get_session(max_retries=0):
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=max_retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
