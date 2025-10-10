import requests
import urllib.parse
import time

from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from processor.opencyc import cb_handle_query, update_cyc_payload, cb_handle_create, cb_handle_assert, cb_continue_query, cb_handle_specify


def is_invalid_constant_references(soup):
    return True if soup.find('h2').text == "Invalid Constant References in New Inference" else False


def invalid_constant_references(soup) -> str:
    constants = soup.find('pre')
    if constants is not None and "ARG2" in constants.text:
        return constants.text


def parse_term_slots(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Parse term slots from noflow tables in the soup."""
    slots = []
    for table in soup.find_all('table', noflow=' noflow'):
        assert_sents = table.find_all('span', class_='assert-sent')
        if not assert_sents:
            continue
        slot_name = table.find('a', target='cyc-main').text.strip()
        line = " ".join(sent.text.strip().replace("\n", "") for sent in assert_sents)
        slots.append({slot_name: line})
    return slots


def parse_all_assertions(soup: BeautifulSoup) -> str:
    """Parse assertions into a formatted string, grouped by sections."""
    output = []
    predicate_strong = soup.find('strong', string=lambda t: t and 'Predicate :' in t)
    if predicate_strong:
        term_strong = predicate_strong.find_next_sibling('strong')
        if term_strong:
            term = term_strong.find('a').text.strip()
            output.append(f"Predicate: {term}\n")
    sections: Dict[str, List[str]] = {}
    current_section = "On the term"
    sections[current_section] = []

    for elem in soup.find_all():
        if elem.name == 'strong' and 'via' in elem.text:
            current_section = elem.text.strip()
            sections[current_section] = []
        elif elem.name == 'table' and elem.get('noflow') == ' noflow':
            pred, value = _extract_pred_value_from_table(elem)
            if pred and value:
                sections[current_section].append(f"{pred}: {value}")
        elif elem.name == 'span' and 'assertion' in elem.get('class', []):
            sentence = _extract_assertion_sentence(elem)
            if sentence:
                sections[current_section].append(f"({sentence})")
        elif elem.name == 'a' and 'query' in elem.text.lower():
            query_text = elem.text.strip()
            sections[current_section].append(f"{query_text} [LitQ]")

    for sec, items in sections.items():
        if items:
            output.extend([f"\n{sec}:"] + items)
    return '\n'.join(output)


def _extract_pred_value_from_table(table) -> tuple[str, str]:
    """Helper to extract predicate and value from a noflow table."""
    strong_td = table.find('td', valign='top')
    if not strong_td:
        return '', ''
    pred = strong_td.find('strong').find('a').text.strip() if strong_td.find('strong') else ''
    value_td = strong_td.find_next_sibling('td')
    if not value_td:
        return pred, ''

    value = ''
    assert_sent = value_td.find('span', class_='assert-sent')
    if assert_sent:
        value_a = assert_sent.find('a', recursive=False)
        if value_a and value_a.find_next_sibling() is None:
            next_a = value_a.find_next('a')
            value = next_a.text.strip() if next_a else ''
        else:
            nobr = value_td.find('nobr')
            if nobr:
                value = nobr.text.strip()
            string_span = assert_sent.find('span', class_='string')
            if string_span:
                value = string_span.text.strip()
    return pred, value


def _extract_assertion_sentence(elem) -> str:
    """Helper to extract sentence from assertion span."""
    cons_span = elem.find('span', class_='cons')
    if not cons_span:
        return ''
    return cons_span.text.strip().replace('(', '').replace(')', '').replace('\n', ' ')


def _extract_input_value(soup: BeautifulSoup, name: str) -> Optional[str]:
    """Helper to extract value from input tag by name."""
    tag = soup.find('input', {'name': name})
    return tag['value'] if tag else None


def _parse_answers_from_soup(soup: BeautifulSoup) -> Dict[str, str]:
    """Parse answer rows from the answers table."""
    answers = {}
    table = soup.find('table', border='0', cellpadding='2', cellspacing='2')
    if not table:
        return answers
    rows = table.find_all('tr')[1:]  # Skip header
    for row in rows:
        cells = row.find_all('td')
        if len(cells) < 2:
            continue
        explain = cells[0].get_text(strip=True).replace('*', '')
        binding = cells[1].get_text(strip=True)
        answers[explain] = binding
    return answers


def _print_query_results(sentence: str, mt: str, status: str, answers: Dict[str, str]) -> None:
    """Print formatted query results."""
    output = f"Query: {sentence}\nMt: {mt}\nStatus: {status}\n\nAnswers ({len(answers)}):\n"
    for explain, binding in sorted(answers.items()):
        output += f"{explain}: {binding}\n"
    print(output)


class CycLService:
    def __init__(self, host: str = 'localhost:3602'):
        self.base_url = f"http://{host}/cgi-bin/"
        self.session = requests.Session()

    def _get_uniquifier_code(self, url: str) -> str:
        """Fetch uniquifier code from a page."""
        response = self.session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        input_tag = soup.find('input', {'name': 'uniquifier-code'})
        if not input_tag:
            raise ValueError("Uniquifier code not found in the page.")
        return input_tag['value']

    def create_constant(self, name: str) -> Dict[str, any]:
        """Create a new constant."""
        create_url = self.base_url + "cg?cb-create"
        uniquifier = self._get_uniquifier_code(create_url)
        payload = update_cyc_payload(cb_handle_create, uniquifier=uniquifier, name=name)
        response = self.session.post(self.base_url + "cg", data=payload)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.find('title').text if soup.find('title') else ''
        if "Constant Create operation completed" not in title:
            raise ValueError("Constant creation failed.")

        recent_constants = [a.text.strip() for a in soup.find_all('a', href=lambda h: h and 'cb-cf' in h)]
        return {
            'status': 'success',
            'response_text': response.text,
            'recent_constants': recent_constants
        }

    def assert_sentence(self, sentence: str, **kwargs) -> Dict[str, any]:
        """Assert a CycL sentence."""
        assert_url = self.base_url + "cg?cb-assert"
        uniquifier = self._get_uniquifier_code(assert_url)
        payload = update_cyc_payload(cb_handle_assert.copy(), sentence=sentence, uniquifier=uniquifier, **kwargs)
        response = self.session.post(self.base_url + "cg", data=payload)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.find('title').text if soup.find('title') else ''
        if "EL Sentence Assert operation was added to queue" not in title:
            raise ValueError("Sentence assertion failed.")

        recent_assertions = [span.get_text(strip=True) for span in soup.find_all('span', class_='assertion')]
        return {
            'status': 'success',
            'response_text': response.text,
            'recent_assertions': recent_assertions
        }

    def query_sentence(self, sentence: str, mt_monad: Optional[str] = None, pretty_print: bool = False, **kwargs) -> Dict[str, any]:
        """
        Execute a CycL query and fetch all answers, continuing if necessary.

        Args:
            sentence: The CycL query sentence.
            mt_monad: The micro theory to use.
            pretty_print: If True, print the results.
            **kwargs: Additional parameters for the query payload.

        Returns:
            dict: Query results including status, answers, mt, and el_query.
        """
        query_url = self.base_url + "cg?cb-query"
        uniquifier = self._get_uniquifier_code(query_url)
        payload = update_cyc_payload(cb_handle_query, sentence, mt_monad, uniquifier, **kwargs)
        query_response = self.session.post(self.base_url + "cg", data=payload)
        query_response.raise_for_status()

        soup = BeautifulSoup(query_response.text, 'html.parser')
        if is_invalid_constant_references(soup):
            constants = invalid_constant_references(soup)
            raise ValueError(f"Invalid Constant References in New Inference: {str(constants)}")

        focal_problem_store = _extract_input_value(soup, 'focal-problem-store')
        focal_inference = _extract_input_value(soup, 'focal-inference')
        if not focal_problem_store or not focal_inference:
            print(f"SOUP={str(soup)}")
            raise ValueError("Failed to extract problem store or inference ID.")

        answers, status, mt, el_query = self._poll_for_all_answers(
            focal_problem_store, focal_inference, uniquifier, payload
        )

        if pretty_print:
            _print_query_results(sentence, mt, status, answers)

        return {
            'status': status,
            'query_response_text': query_response.text,
            'answers': answers,
            'mt': mt,
            'el_query': el_query
        }

    def _poll_for_all_answers(self, problem_store: str, inference: str, uniquifier: str, payload: Dict) -> tuple[Dict[str, str], str, str, str]:
        """Poll for query answers until exhausted."""
        answer_dict: Dict[str, str] = {}
        max_attempts = 10
        attempt = 0
        last_answer_count = 0
        status = 'Unknown'
        mt = payload.get('mt-monad', 'Unknown')
        el_query = payload.get('sentence', 'Unknown')

        while attempt < max_attempts:
            all_answers_soup, new_status, new_mt, new_el_query = self._fetch_answers_page(problem_store, inference)
            status = new_status
            mt = new_mt or mt
            el_query = new_el_query or el_query

            new_answers = _parse_answers_from_soup(all_answers_soup)
            for explain, binding in new_answers.items():
                if explain not in answer_dict:
                    answer_dict[explain] = binding

            current_count = len(answer_dict)
            if 'Exhaust Total' in status and current_count == last_answer_count:
                break

            last_answer_count = current_count
            attempt += 1

            if 'Suspended' in status and payload.get('radio-CONTINUABLE?_') == '1':
                self._continue_inference(problem_store, inference, uniquifier)
                time.sleep(1)

        return answer_dict, status, mt, el_query

    def _fetch_answers_page(self, problem_store: str, inference: str) -> tuple[BeautifulSoup, str, Optional[str], Optional[str]]:
        """Fetch and parse the all-inference-answers page."""
        url = self.base_url + f"cg?cb-all-inference-answers&{problem_store}&{inference}"
        response = self.session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        mt_strong = soup.find('strong', string='Mt :')
        mt = mt_strong.find_next_sibling('span').get_text(strip=True) if mt_strong else None

        query_strong = soup.find('strong', string='EL Query :')
        el_query = query_strong.find_next_sibling('span').get_text(strip=True) if query_strong else None

        status_strong = soup.find('strong', string='Status :')
        status = status_strong.next_sibling.strip() if status_strong else 'Unknown'

        return soup, status, mt, el_query

    def _continue_inference(self, problem_store: str, inference: str, uniquifier: str) -> None:
        """Continue a suspended inference."""
        url = self.base_url + "cg"
        payload = cb_continue_query.copy()
        payload['focal-problem-store'] = problem_store
        payload['focal-inference'] = inference
        payload['uniquifier-code'] = uniquifier
        response = self.session.post(url, data=payload)
        if response.status_code != 200:
            print(f"Warning: Failed to continue inference.")

    def get_all_inference_answers(self, problem_store: str, inference: str) -> Dict[str, any]:
        """Get all answers for an inference and print them."""
        soup, status, mt, el_query = self._fetch_answers_page(problem_store, inference)
        answers_list = list(_parse_answers_from_soup(soup).items())
        pretty_output = f"Mt: {mt}\nEL Query: {el_query}\nStatus: {status}\n\nAnswers ({len(answers_list)}):\n"
        for explain, binding in answers_list:
            pretty_output += f"{explain.replace('*', 'New: ')}: {binding}\n"
        print(pretty_output)
        return {
            'mt': mt,
            'el_query': el_query,
            'status': status,
            'answers': [{'explain': explain, 'binding': binding} for explain, binding in answers_list],
            'pretty_output': pretty_output
        }

    def search_terms(self, term: str) -> List[str]:
        """Search for multiple terms matching a query."""
        if term.startswith("#$"):
            raise ValueError("Term starts with '#$' - use search_term for a specific term")
        params = cb_handle_specify.copy()
        params['query'] = term
        response = self.session.get(self.base_url + 'cg', params=params)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        terms = []
        collect = False
        tds = soup.find_all('td', attrs={'valign': 'top'})
        for td in tds:
            if not td.text.strip():
                continue
            for child in td.children:
                if isinstance(child, str):
                    continue
                if child.name == 'hr':
                    collect = True
                    continue
                if collect and child.name in ['a', 'span']:
                    term_text = child.get_text().strip()
                    if term_text:
                        terms.append(term_text)
        return terms

    def search_term(self, term: str) -> List[Dict[str, str]]:
        """Search for details on a specific term."""
        params = cb_handle_specify.copy()
        params['query'] = term
        main_response = self.session.get(self.base_url + 'cg', params=params)
        main_response.raise_for_status()
        soup = BeautifulSoup(main_response.text, 'html.parser')

        frames = soup.find_all('frame')
        if len(frames) != 2:
            raise ValueError("Unexpected frameset structure.")
        content_src = frames[1]['src']
        content_url = self.base_url + content_src
        content_response = self.session.get(content_url)
        content_response.raise_for_status()

        return parse_term_slots(BeautifulSoup(content_response.text, 'html.parser'))

    def alpha_paging(self) -> List[str]:
        """Fetch all terms via alpha paging."""
        start_time = time.time()
        all_terms = []
        page_count = 0
        start: Optional[str] = None
        while True:
            terms, next_start = self._fetch_alpha_index(start)
            if terms is None:
                print("Failed to fetch page.")
                break
            # Avoid duplicates from overlapping pages
            if all_terms and terms and terms[0] == all_terms[-1]:
                terms = terms[1:]
            page_count += 1
            all_terms.extend(terms)
            print(f"Page {page_count}: Fetched {len(terms)} terms")
            print("Terms:")
            for term in terms:
                print(f" - {term}")
            print("")
            if next_start is None or not terms:
                break
            start = next_start
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Total pages: {page_count}")
        print(f"Total terms: {len(all_terms)}")
        print(f"Time taken: {total_time:.2f} seconds")
        return all_terms

    def _fetch_alpha_index(self, start: Optional[str] = None) -> tuple[Optional[List[str]], Optional[str]]:
        """Fetch a single alpha index page."""
        path = "cg?cb-alpha-top" if start is None else f"cg?cb-alpha-pagedn|{urllib.parse.quote(start)}"
        url = self.base_url + path
        response = self.session.get(url)
        if not response.ok:
            return None, None
        soup = BeautifulSoup(response.text, 'html.parser')
        terms_table = next(
            (t for t in soup.find_all('table', noflow=' noflow', border='0', cellpadding='0', cellspacing='0') if
             'nowrap' not in t.attrs), None)
        if not terms_table:
            return [], None

        terms = []
        for tr in terms_table.find_all('tr', valign='middle'):
            td = tr.find('td', nowrap=' nowrap')
            if not td:
                continue
            a = td.find('a')
            if a and a.get('href', '').startswith('cg?cb-cf&'):
                terms.append(a.text.strip())

        page_down_a = soup.find('a', string=lambda t: t and 'Page Down' in t)
        next_start = page_down_a['href'].split('|')[1] if page_down_a and '|' in page_down_a['href'] else None
        return terms, next_start
