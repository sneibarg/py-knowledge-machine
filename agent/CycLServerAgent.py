import requests
import urllib.parse
import time

from agent import cb_handle_query, update_payload, cb_handle_create, cb_handle_assert
from bs4 import BeautifulSoup


class CycLServerAgent:
    def __init__(self, host='dragon:3602'):
        self.base_url = f"http://{host}/cgi-bin/"
        self.session = requests.Session()

    def _get_uniquifier_code(self, url):
        response = self.session.get(url)
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch page for uniquifier code: {response.status_code}")
        soup = BeautifulSoup(response.text, 'html.parser')
        input_tag = soup.find('input', {'name': 'uniquifier-code'})
        if input_tag:
            return input_tag['value']
        raise ValueError("Uniquifier code not found in the page.")

    def create_constant(self, name):
        create_url = self.base_url + "cg?cb-create"
        uniquifier = self._get_uniquifier_code(create_url)
        payload = update_payload(cb_handle_create, uniquifier=uniquifier, name=name)
        create_response = self.session.post(self.base_url + "cg", data=payload)
        if create_response.status_code != 200:
            raise ValueError("Failed to create constant.")
        soup = BeautifulSoup(create_response.text, 'html.parser')
        title = soup.find('title').text if soup.find('title') else ''
        if "Constant Create operation completed" not in title:
            raise ValueError("Constant creation failed.")
        recent_constants = [a.text.strip() for a in soup.find_all('a', href=lambda h: h and 'cb-cf' in h)]
        return {
            'status': 'success',
            'response_text': create_response.text,
            'recent_constants': recent_constants
        }

    def assert_sentence(self, sentence, **kwargs):
        assert_url = self.base_url + "cg?cb-assert"
        uniquifier = self._get_uniquifier_code(assert_url)
        payload = update_payload(cb_handle_assert.copy(), sentence=sentence, uniquifier=uniquifier, **kwargs)
        assert_response = self.session.post(self.base_url + "cg", data=payload)
        if assert_response.status_code != 200:
            raise ValueError("Failed to assert sentence.")
        soup = BeautifulSoup(assert_response.text, 'html.parser')
        title = soup.find('title').text if soup.find('title') else ''
        if "EL Sentence Assert operation was added to queue" not in title:
            raise ValueError("Sentence assertion failed.")
        recent_assertions = [span.get_text(strip=True) for span in soup.find_all('span', class_='assertion')]
        return {
            'status': 'success',
            'response_text': assert_response.text,
            'recent_assertions': recent_assertions
        }

    def query_sentence(self, sentence, mt_monad=None, **kwargs):
        """
        Execute a CycL query and fetch all answers, continuing if necessary.

        Args:
            sentence (str): The CycL query sentence.
            mt_monad (str, optional): The microtheory to use. Defaults to default in cyc_query_post_data.
            **kwargs: Additional parameters to override in post_data.

        Returns:
            dict: Query results including status, responses, answers, and pretty-printed output.
        """
        query_url = self.base_url + "cg?cb-query"
        uniquifier = self._get_uniquifier_code(query_url)
        payload = update_payload(cb_handle_query, sentence, mt_monad, uniquifier, **kwargs)
        query_response = self.session.post(self.base_url + "cg", data=payload)
        if query_response.status_code != 200:
            raise ValueError("Failed to start query.")

        soup = BeautifulSoup(query_response.text, 'html.parser')
        focal_problem_store = soup.find('input', {'name': 'focal-problem-store'})['value'] if soup.find('input', {
            'name': 'focal-problem-store'}) else None
        focal_inference = soup.find('input', {'name': 'focal-inference'})['value'] if soup.find('input', {
            'name': 'focal-inference'}) else None

        if not focal_problem_store or not focal_inference:
            raise ValueError("Failed to extract problem store or inference ID.")

        answer_dict = {}
        all_answers = []
        max_attempts = 10
        attempt = 0
        last_answer_count = 0
        status = 'Unknown'

        while attempt < max_attempts:
            all_answers_url = self.base_url + f"cg?cb-all-inference-answers&{focal_problem_store}&{focal_inference}"
            all_answers_response = self.session.get(all_answers_url)
            if all_answers_response.status_code != 200:
                raise ValueError("Failed to fetch all inference answers page.")

            all_answers_soup = BeautifulSoup(all_answers_response.text, 'html.parser')
            mt_strong = all_answers_soup.find('strong', string='Mt :')
            mt = mt_strong.find_next_sibling('span').get_text(strip=True) if mt_strong else payload['mt-monad']
            query_strong = all_answers_soup.find('strong', string='EL Query :')
            el_query = query_strong.find_next_sibling('span').get_text(strip=True) if query_strong else sentence
            status_strong = all_answers_soup.find('strong', string='Status :')
            status = status_strong.next_sibling.strip() if status_strong else 'Unknown'

            answers_table = all_answers_soup.find('table', border='0', cellpadding='2', cellspacing='2')
            if answers_table:
                rows = answers_table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        explain = cells[0].get_text(strip=True).replace('*', '')
                        binding = cells[1].get_text(strip=True)
                        if explain not in answer_dict:
                            all_answers.append({'explain': explain, 'binding': binding})
                        answer_dict[explain] = binding

            current_answer_count = len(all_answers)
            if 'Exhaust Total' in status and current_answer_count == last_answer_count:
                break

            last_answer_count = current_answer_count
            attempt += 1

            if 'Suspended' in status and payload.get('radio-CONTINUABLE?_') == '1':
                continue_url = self.base_url + "cg"
                continue_data = {
                    'cb-handle-query': '',
                    'continue': 'Continue the Focal Inference',
                    'focal-problem-store': focal_problem_store,
                    'focal-inference': focal_inference,
                    'uniquifier-code': uniquifier
                }
                continue_response = self.session.post(continue_url, data=continue_data)
                if continue_response.status_code != 200:
                    print(f"Warning: Failed to continue inference on attempt {attempt}")
                    break
                time.sleep(1)

        seen = set()
        unique_answers = []
        for ans in all_answers:
            if ans['binding'] not in seen:
                seen.add(ans['binding'])
                unique_answers.append(ans)

        pretty_output = f"Query: {sentence}\nMt: {mt}\nStatus: {status}\n\nAnswers ({len(answer_dict.items())}):\n"
        for ans in sorted(answer_dict.items()):
            if ans in answer_dict:
                pretty_output += f"{ans}: {answer_dict[ans]}\n"

        print(pretty_output)

        return {
            'status': status,
            'query_response_text': query_response.text,
            'all_answers_response_text': all_answers_response.text,
            'answers': answer_dict,
            'pretty_output': pretty_output,
            'mt': mt,
            'el_query': el_query
        }

    def get_all_inference_answers(self, problem_store, inference):
        all_answers_url = self.base_url + f"cg?cb-all-inference-answers&{problem_store}&{inference}"
        all_answers_response = self.session.get(all_answers_url)
        if all_answers_response.status_code != 200:
            raise ValueError("Failed to fetch all inference answers page.")

        soup = BeautifulSoup(all_answers_response.text, 'html.parser')

        mt_strong = soup.find('strong', string='Mt :')
        mt = mt_strong.find_next_sibling('span').get_text(strip=True) if mt_strong else 'Unknown'

        query_strong = soup.find('strong', string='EL Query :')
        el_query = query_strong.find_next_sibling('span').get_text(strip=True) if query_strong else 'Unknown'

        status_strong = soup.find('strong', string='Status :')
        status = status_strong.next_sibling.strip() if status_strong else 'Unknown'

        answers = []
        answers_div = soup.find('div', id='inference-answers')
        if answers_div:
            rows = answers_div.find_all('tr')[1:]
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    explain = cells[0].get_text(strip=True).replace('*', 'New: ')
                    binding = cells[1].get_text(strip=True)
                    answers.append({'explain': explain, 'binding': binding})

        pretty_output = f"Mt: {mt}\nEL Query: {el_query}\nStatus: {status}\n\nAnswers ({len(answers)}):\n"
        for ans in answers:
            pretty_output += f"{ans['explain']}: {ans['binding']}\n"

        print(pretty_output)

        return {
            'mt': mt,
            'el_query': el_query,
            'status': status,
            'answers': answers,
            'pretty_output': pretty_output
        }

    def search_term(self, term, mode='default'):
        search_params = {
            'cb-handle-specify': '',
            'handler': 'cb-cf',
            'arg-done': 'T',
            'query': term,
            'uniquifier-code': '335'
        }
        main_response = self.session.get(self.base_url + 'cg', params=search_params)
        if main_response.status_code != 200:
            raise ValueError("Failed to fetch main page.")
        soup = BeautifulSoup(main_response.text, 'html.parser')
        frames = soup.find_all('frame')
        if len(frames) != 2:
            raise ValueError("Unexpected frameset structure.")
        index_src = frames[0]['src']
        content_src = frames[1]['src']
        constant_id = index_src.split('&')[1]
        content_url = self.base_url + (
            content_src if mode == 'default' else f"cg?cb-inferred-gaf-arg-assertions&{constant_id}")
        content_response = self.session.get(content_url)
        if not content_response.ok:
            raise ValueError("Failed to fetch content frame.")
        content_soup = BeautifulSoup(content_response.text, 'html.parser')
        return content_soup.get_text(separator='\n\n', strip=True) if mode == 'default' else self._parse_all_assertions(
            content_soup)

    def _parse_all_assertions(self, soup):
        output = []
        predicate_strong = soup.find('strong', string=lambda t: t and 'Predicate :' in t)
        if predicate_strong:
            term_strong = predicate_strong.find_next_sibling('strong')
            if term_strong:
                term = term_strong.find('a').text.strip()
                output.append(f"Predicate: {term}\n")
        current_section = "On the term"
        sections = {current_section: []}
        on_term_strong = soup.find('strong', string='on  the term')
        if on_term_strong:
            output.append(f"{current_section}:")
        for elem in soup.find_all():
            if elem.name == 'strong' and 'via' in elem.text:
                current_section = elem.text.strip()
                sections[current_section] = []
            elif elem.name == 'table' and elem.get('noflow') == ' noflow':
                strong_td = elem.find('td', valign='top')
                if strong_td:
                    pred = strong_td.find('strong').find('a').text.strip() if strong_td.find('strong') else ''
                    value_td = strong_td.find_next_sibling('td')
                    if value_td:
                        value = ''
                        assert_sent = value_td.find('span', class_='assert-sent')
                        if assert_sent:
                            value_a = assert_sent.find('a', recursive=False)
                            if value_a and value_a.find_next_sibling() is None:
                                value = value_a.find_next('a').text.strip() if value_a.find_next('a') else ''
                            else:
                                nobr = value_td.find('nobr')
                                if nobr:
                                    value = nobr.text.strip()
                                string_span = assert_sent.find('span', class_='string')
                                if string_span:
                                    value = string_span.text.strip()
                        if pred and value:
                            sections[current_section].append(f"{pred}: {value}")
            elif elem.name == 'span' and 'assertion' in elem.get('class', []):
                cons_span = elem.find('span', class_='cons')
                if cons_span:
                    sentence = cons_span.text.strip().replace('(', '').replace(')', '').replace('\n', ' ')
                    sections[current_section].append(f"({sentence})")
            elif elem.name == 'a' and 'query' in elem.text.lower():
                query_text = elem.text.strip()
                sections[current_section].append(f"{query_text} [LitQ]")
        for sec, items in sections.items():
            if items:
                output.append(f"\n{sec}:")
                for item in items:
                    output.append(item)
        return '\n'.join(output)

    def alpha_paging(self):
        start_time = time.time()
        all_terms = []
        page_count = 0
        start = None
        while True:
            terms, next_start = self._fetch_alpha_index(start)
            if terms is None:
                print("Failed to fetch page.")
                break
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

    def _fetch_alpha_index(self, start=None):
        url = self.base_url + (
            "cg?cb-alpha-top" if start is None else f"cg?cb-alpha-pagedn|{urllib.parse.quote(start)}")
        response = self.session.get(url)
        if not response.ok:
            return None, None
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table',
                               attrs={'noflow': ' noflow', 'border': '0', 'cellpadding': '0', 'cellspacing': '0'})
        terms_table = None
        for table in tables:
            if 'nowrap' not in table.attrs:
                terms_table = table
                break
        terms = []
        if terms_table:
            for tr in terms_table.find_all('tr', attrs={'valign': 'middle'}):
                td = tr.find('td', attrs={'nowrap': ' nowrap'})
                if td:
                    a = td.find('a')
                    if a and a.get('href').startswith('cg?cb-cf&'):
                        terms.append(a.text.strip())
        page_down_a = soup.find('a', string=lambda t: t and 'Page Down' in t)
        next_start = None
        if page_down_a:
            href = page_down_a['href']
            if '|' in href:
                next_start = href.split('|')[1]
        return terms, next_start
