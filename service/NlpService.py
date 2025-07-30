import json
import logging
import time
import requests

from tenacity import stop_after_attempt, retry, wait_exponential, retry_if_exception_type
from service import get_session

nlp_openie_relations = "/openie-relations"
nlp_relations = "/relations"
nlp_ner = "/ner"
nlp_sentiment = "/sentiment"
nlp_coref = "/coref"
nlp_tokenize = "/tokenize"


class NlpService:
    def __init__(self, api_url: str, parent_logger: logging.Logger):
        self.api_url = api_url
        self.logger = parent_logger
        self.session = get_session(max_retries=3)

    def _nlp_post_request(self, url: str, data: str) -> dict:
        response = None
        headers = {'Content-Type': 'application/json'}
        try:
            response = self.session.post(
                url,
                data=data.encode('utf-8', errors='replace'),
                headers=headers,
                timeout=(120, 360)
            )
            response.raise_for_status()
            return response.json()
        except json.JSONDecodeError as jde:
            self.logger.error(f"Invalid JSON from NLP API: {jde}")
            raise ValueError("NLP API returned malformed JSON") from jde
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout error contacting {url}: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error contacting {url}: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                self.logger.error(f"Rate limit exceeded for {url}: {e}")
            elif response.status_code >= 500:
                self.logger.error(f"Server error from {url} (status {response.status_code}): {e}")
                raise
            else:
                self.logger.error(f"HTTP error from {url} (status {response.status_code}): {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error in NLP request to {url}: {e}")
            return {}
        finally:
            time.sleep(0.1)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError))
    )
    def stanford_relations(self, data: str, openie=False) -> dict:
        if not isinstance(data, str):
            self.logger.error(f"Invalid input type for stanford_relations: {type(data)}")
            return {}
        if openie:
            url = str(self.api_url + nlp_openie_relations)
        else:
            url = str(self.api_url + nlp_relations)
        return self._nlp_post_request(url, data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError))
    )
    def stanford_ner(self, data: str) -> dict:
        if not isinstance(data, str):
            self.logger.error(f"Invalid input type for stanford_ner: {type(data)}")
            return {}
        return self._nlp_post_request(str(self.api_url + nlp_ner), data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError))
    )
    def stanford_sentiment(self, data: str) -> dict:
        if not isinstance(data, str):
            self.logger.error(f"Invalid input type for stanford_sentiment: {type(data)}")
            return {}
        return self._nlp_post_request(str(self.api_url + nlp_sentiment), data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError))
    )
    def stanford_coref(self, data: str) -> dict:
        if not isinstance(data, str):
            self.logger.error(f"Invalid input type for stanford_coref: {type(data)}")
            return {}
        return self._nlp_post_request(str(self.api_url + nlp_coref), data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError))
    )
    def stanford_tokenize(self, data: str) -> dict:
        if not isinstance(data, str):
            self.logger.error(f"Invalid input type for stanford_tokenize: {type(data)}")
            return {}
        return self._nlp_post_request(str(self.api_url + nlp_tokenize), data)
