import json
import time
import requests

from typing import Optional
from tenacity import stop_after_attempt, wait_exponential, retry_if_exception_type, retry
from service import get_session


class OllamaService:
    def __init__(self, api_url, parent_logger):
        self.api_url = api_url
        self.logger = parent_logger
        self.session = get_session(max_retries=3)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError))
    )
    def one_shot(self, model: str, text: str, base_prompt: str) -> Optional[str]:
        response = None
        safe_text = text.encode('utf-8', errors='replace').decode('utf-8') if text else ""
        full_prompt = f"<s>[INST] {base_prompt} {safe_text} [/INST]"
        payload = {
            "model": model,
            "prompt": full_prompt,
            "stream": False
        }

        try:
            start_time = time.time()
            response = self.session.post(
                self.api_url,
                json=payload,
                timeout=(120, 3600)
            )
            response.raise_for_status()
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"REST call to {self.api_url} took {duration:.3f} seconds")
            try:
                json_response = response.json()
                if 'response' not in json_response:
                    self.logger.error(f"Missing 'response' key in JSON from {self.api_url}")
                    return None
                return json_response['response']
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON response from {self.api_url}: {e}")
                return None
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout error contacting {self.api_url}: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error contacting {self.api_url}: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                self.logger.error(f"Rate limit exceeded for {self.api_url}: {e}")
            elif response.status_code >= 500:
                self.logger.error(f"Server error from {self.api_url} (status {response.status_code}): {e}")
                raise
            else:
                self.logger.error(f"HTTP error from {self.api_url} (status {response.status_code}): {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in one_shot: {e}")
            return None
        finally:
            time.sleep(0.1)
