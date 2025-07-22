import urllib
import requests
from datetime import time
from bs4 import BeautifulSoup
from bs4.element import AttributeValueList
from typing import Tuple, List, Union


class CycLAgent:
    def __init__(self, host='localhost:3602'):
        self.base_url = f"http://{host}/cgi-bin/"
        self.session = requests.Session()  # For persistent connections

    def _get_uniquifier_code(self, url) -> Union[str, AttributeValueList]:
        response = self.session.get(url)
        if response.status_code != 200:
            raise ValueError("Failed to fetch page for uniquifier code.")
        soup = BeautifulSoup(response.text, 'html.parser')
        input_tag = soup.find('input', {'name': 'uniquifier-code'})
        if input_tag:
            return input_tag['value']
        raise ValueError("Uniquifier code not found in the page.")

    def create_constant(self, name) -> dict:
        create_url = self.base_url + "cg?cb-create"
        uniquifier = self._get_uniquifier_code(create_url)
        post_data = {
            'cb-handle-create': 'T',
            'new-name': name,
            'uniquifier-code': uniquifier
        }
        create_response = self.session.post(self.base_url + "cg", data=post_data)
        if create_response.status_code != 200:
            raise ValueError("Failed to create constant.")

        soup = BeautifulSoup(create_response.text, 'html.parser')
        title = soup.find('title').text if soup.find('title') else ''
        if "Constant Create operation completed" not in title:
            raise ValueError("Constant creation failed.")

        recent_constants = []
        for a in soup.find_all('a', href=lambda h: h and 'cb-cf' in h):
            recent_constants.append(a.text.strip())

        return {
            'status': 'success',
            'response_text': create_response.text,
            'recent_constants': recent_constants
        }

    def assert_sentence(self, sentence, mt='BaseKB', strength=':default', assertion_queue=':local',
                        mt_time_dimension_specified='na', mt_time_interval='Always-TimeInterval',
                        mt_time_parameter='Null-TimeParameter') -> dict:

        assert_url = self.base_url + "cg?cb-assert"
        uniquifier = self._get_uniquifier_code(assert_url)
        post_data = {
            'cb-handle-assert': 'T',
            'assert': 'Assert Sentence',
            'assertion-queue': assertion_queue,
            'strength': strength,
            'mt-monad': mt,
            'mt-time-dimension-specified': mt_time_dimension_specified,
            'mt-time-interval': mt_time_interval,
            'mt-time-parameter': mt_time_parameter,
            'sentence': sentence,
            'uniquifier-code': uniquifier
        }
        assert_response = self.session.post(self.base_url + "cg", data=post_data)
        if assert_response.status_code != 200:
            raise ValueError("Failed to assert sentence.")

        soup = BeautifulSoup(assert_response.text, 'html.parser')
        title = soup.find('title').text if soup.find('title') else ''
        if "EL Sentence Assert operation was added to queue" not in title:
            raise ValueError("Sentence assertion failed.")

        recent_assertions = []
        for span in soup.find_all('span', class_='assertion'):
            recent_assertions.append(span.get_text(strip=True))

        return {
            'status': 'success',
            'response_text': assert_response.text,
            'recent_assertions': recent_assertions
        }

    def search_term(self, term, mode='default') -> str:
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

        if mode == 'default':
            content_url = self.base_url + content_src
        else:
            content_url = self.base_url + f"cg?cb-inferred-gaf-arg-assertions&{constant_id}"

        content_response = self.session.get(content_url)
        if not content_response.ok:
            raise ValueError("Failed to fetch content frame.")

        content_soup = BeautifulSoup(content_response.text, 'html.parser')
        if mode == 'default':
            return content_soup.get_text(separator='\n\n', strip=True)
        else:
            return self._parse_all_assertions(content_soup)

    def query_sentence(self, sentence, mt_monad='CurrentWorldDataCollectorMt-NonHomocentric',
                       mt_time_dimension_specified='t', mt_time_interval='Now',
                       mt_time_parameter='Null-TimeParameter', non_exp_sentence='', entry_MAX_NUMBER='',
                       radio_MAX_NUMBER='1', radio_MAX_TIME='0', entry_MAX_TIME='30', entry_MAX_STEP='',
                       radio_MAX_STEP='1', radio_INFERENCE_MODE='1', radio_MAX_TRANSFORMATION_DEPTH='0',
                       entry_MAX_TRANSFORMATION_DEPTH='1', radio_NEW_TERMS_ALLOWED='0', entry_MAX_PROOF_DEPTH='',
                       radio_MAX_PROOF_DEPTH='1', radio_ALLOW_HL_PREDICATE_TRANSFORMATION='0',
                       radio_ALLOW_UNBOUND_PREDICATE_TRANSFORMATION='0',
                       radio_ALLOW_EVALUATABLE_PREDICATE_TRANSFORMATION='1', radio_TRANSFORMATION_ALLOWED='0',
                       radio_REMOVAL_BACKTRACKING_PRODUCTIVITY_LIMIT='0',
                       entry_REMOVAL_BACKTRACKING_PRODUCTIVITY_LIMIT='0', radio_PRODUCTIVITY_LIMIT='0',
                       entry_PRODUCTIVITY_LIMIT='200000.0', radio_MAX_PROBLEM_COUNT='0',
                       entry_MAX_PROBLEM_COUNT='100000', radio_TRANSITIVE_CLOSURE_MODE='0',
                       radio_ADD_RESTRICTION_LAYER_OF_INDIRECTION='1', radio_MIN_RULE_UTILITY='0',
                       entry_MIN_RULE_UTILITY='-100', entry_PROBABLY_APPROXIMATELY_DONE='100.0',
                       radio_PROBABLY_APPROXIMATELY_DONE='1', radio_FORWARD_MAX_TIME='0',
                       entry_FORWARD_MAX_TIME='0', radio_BLOCK='0', radio_CACHE_INFERENCE_RESULTS='0',
                       radio_ANSWER_LANGUAGE='0', radio_CONTINUABLE='1', radio_METRICS='0',
                       entry_METRICS='', radio_ALLOW_INDETERMINATE_RESULTS='0',
                       radio_ALLOW_ABNORMALITY_CHECKING='1', radio_RESULT_UNIQUENESS='0',
                       radio_DISJUNCTION_FREE_EL_VARS_POLICY='1', entry_ALLOWED_MODULES='',
                       radio_ALLOWED_MODULES='1', radio_NEGATION_BY_FAILURE='0',
                       radio_COMPLETENESS_MINIMIZATION_ALLOWED='1', radio_DIRECTION='0',
                       radio_EQUALITY_REASONING_METHOD='0', radio_EQUALITY_REASONING_DOMAIN='0',
                       radio_INTERMEDIATE_STEP_VALIDATION_LEVEL='3', radio_EVALUATE_SUBL_ALLOWED='1',
                       radio_REWRITE_ALLOWED='0', radio_ABDUCTION_ALLOWED='0', radio_COMPUTE_ANSWER_JUSTIFICATIONS='1'):

        query_url = self.base_url + "cg?cb-query"
        uniquifier = self._get_uniquifier_code(query_url)
        post_data = {
            'cb-handle-query': '',
            'new': 'Start Inference',
            'mt-monad': mt_monad,
            'mt-time-dimension-specified': mt_time_dimension_specified,
            'mt-time-interval': mt_time_interval,
            'mt-time-parameter': mt_time_parameter,
            'sentence': sentence,
            'non_exp_sentence': non_exp_sentence,
            'entry-MAX-NUMBER': entry_MAX_NUMBER,
            'radio-MAX-NUMBER_' + uniquifier: radio_MAX_NUMBER,
            'radio-MAX-TIME_' + uniquifier: radio_MAX_TIME,
            'entry-MAX-TIME': entry_MAX_TIME,
            'entry-MAX-STEP': entry_MAX_STEP,
            'radio-MAX-STEP_' + uniquifier: radio_MAX_STEP,
            'radio-INFERENCE-MODE_' + uniquifier: radio_INFERENCE_MODE,
            'radio-MAX-TRANSFORMATION-DEPTH_' + uniquifier: radio_MAX_TRANSFORMATION_DEPTH,
            'entry-MAX-TRANSFORMATION-DEPTH': entry_MAX_TRANSFORMATION_DEPTH,
            'radio-NEW-TERMS-ALLOWED?_' + uniquifier: radio_NEW_TERMS_ALLOWED,
            'entry-MAX-PROOF-DEPTH': entry_MAX_PROOF_DEPTH,
            'radio-MAX-PROOF-DEPTH_' + uniquifier: radio_MAX_PROOF_DEPTH,
            'radio-ALLOW-HL-PREDICATE-TRANSFORMATION?_' + uniquifier: radio_ALLOW_HL_PREDICATE_TRANSFORMATION,
            'radio-ALLOW-UNBOUND-PREDICATE-TRANSFORMATION?_' + uniquifier: radio_ALLOW_UNBOUND_PREDICATE_TRANSFORMATION,
            'radio-ALLOW-EVALUATABLE-PREDICATE-TRANSFORMATION?_' + uniquifier: radio_ALLOW_EVALUATABLE_PREDICATE_TRANSFORMATION,
            'radio-TRANSFORMATION-ALLOWED?_' + uniquifier: radio_TRANSFORMATION_ALLOWED,
            'radio-REMOVAL-BACKTRACKING-PRODUCTIVITY-LIMIT_' + uniquifier: radio_REMOVAL_BACKTRACKING_PRODUCTIVITY_LIMIT,
            'entry-REMOVAL-BACKTRACKING-PRODUCTIVITY-LIMIT': entry_REMOVAL_BACKTRACKING_PRODUCTIVITY_LIMIT,
            'radio-PRODUCTIVITY-LIMIT_' + uniquifier: radio_PRODUCTIVITY_LIMIT,
            'entry-PRODUCTIVITY-LIMIT': entry_PRODUCTIVITY_LIMIT,
            'radio-MAX-PROBLEM-COUNT_' + uniquifier: radio_MAX_PROBLEM_COUNT,
            'entry-MAX-PROBLEM-COUNT': entry_MAX_PROBLEM_COUNT,
            'radio-TRANSITIVE-CLOSURE-MODE_' + uniquifier: radio_TRANSITIVE_CLOSURE_MODE,
            'radio-ADD-RESTRICTION-LAYER-OF-INDIRECTION?_' + uniquifier: radio_ADD_RESTRICTION_LAYER_OF_INDIRECTION,
            'radio-MIN-RULE-UTILITY_' + uniquifier: radio_MIN_RULE_UTILITY,
            'entry-MIN-RULE-UTILITY': entry_MIN_RULE_UTILITY,
            'entry-PROBABLY-APPROXIMATELY-DONE': entry_PROBABLY_APPROXIMATELY_DONE,
            'radio-PROBABLY-APPROXIMATELY-DONE_' + uniquifier: radio_PROBABLY_APPROXIMATELY_DONE,
            'radio-FORWARD-MAX-TIME_' + uniquifier: radio_FORWARD_MAX_TIME,
            'entry-FORWARD-MAX-TIME': entry_FORWARD_MAX_TIME,
            'radio-BLOCK?_' + uniquifier: radio_BLOCK,
            'radio-CACHE-INFERENCE-RESULTS?_' + uniquifier: radio_CACHE_INFERENCE_RESULTS,
            'radio-ANSWER-LANGUAGE_' + uniquifier: radio_ANSWER_LANGUAGE,
            'radio-CONTINUABLE?_' + uniquifier: radio_CONTINUABLE,
            'radio-METRICS_' + uniquifier: radio_METRICS,
            'entry-METRICS': entry_METRICS,
            'radio-ALLOW-INDETERMINATE-RESULTS?_' + uniquifier: radio_ALLOW_INDETERMINATE_RESULTS,
            'radio-ALLOW-ABNORMALITY-CHECKING?_' + uniquifier: radio_ALLOW_ABNORMALITY_CHECKING,
            'radio-RESULT-UNIQUENESS_' + uniquifier: radio_RESULT_UNIQUENESS,
            'radio-DISJUNCTION-FREE-EL-VARS-POLICY_' + uniquifier: radio_DISJUNCTION_FREE_EL_VARS_POLICY,
            'entry-ALLOWED-MODULES': entry_ALLOWED_MODULES,
            'radio-ALLOWED-MODULES_' + uniquifier: radio_ALLOWED_MODULES,
            'radio-NEGATION-BY-FAILURE?_' + uniquifier: radio_NEGATION_BY_FAILURE,
            'radio-COMPLETENESS-MINIMIZATION-ALLOWED?_' + uniquifier: radio_COMPLETENESS_MINIMIZATION_ALLOWED,
            'radio-DIRECTION_' + uniquifier: radio_DIRECTION,
            'radio-EQUALITY-REASONING-METHOD_' + uniquifier: radio_EQUALITY_REASONING_METHOD,
            'radio-EQUALITY-REASONING-DOMAIN_' + uniquifier: radio_EQUALITY_REASONING_DOMAIN,
            'radio-INTERMEDIATE-STEP-VALIDATION-LEVEL_' + uniquifier: radio_INTERMEDIATE_STEP_VALIDATION_LEVEL,
            'radio-EVALUATE-SUBL-ALLOWED?_' + uniquifier: radio_EVALUATE_SUBL_ALLOWED,
            'radio-REWRITE-ALLOWED?_' + uniquifier: radio_REWRITE_ALLOWED,
            'radio-ABDUCTION-ALLOWED?_' + uniquifier: radio_ABDUCTION_ALLOWED,
            'radio-COMPUTE-ANSWER-JUSTIFICATIONS?_' + uniquifier: radio_COMPUTE_ANSWER_JUSTIFICATIONS,
            'uniquifier-code': uniquifier
        }
        query_response = self.session.post(self.base_url + "cg", data=post_data)
        if query_response.status_code != 200:
            raise ValueError("Failed to start query.")

        soup = BeautifulSoup(query_response.text, 'html.parser')
        focal_problem_store = soup.find('input', {'name': 'focal-problem-store'})['value']
        focal_inference = soup.find('input', {'name': 'focal-inference'})['value']

        progress_url = self.base_url + f"cg?cb-inference-progress-page&{focal_problem_store}&{focal_inference}"
        progress_response = self.session.get(progress_url)
        if progress_response.status_code != 200:
            raise ValueError("Failed to fetch progress page.")

        progress_soup = BeautifulSoup(progress_response.text, 'html.parser')

        answers = []
        answers_div = progress_soup.find('div', id='inference-answers')
        if answers_div:
            rows = answers_div.find_all('tr')[1:]  # Skip header
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    explain_link = cells[0].find('a').text.strip() if cells[0].find('a') else ''
                    binding = cells[1].get_text(strip=True)
                    answers.append({'explain': explain_link, 'binding': binding})

        return {
            'status': 'success',
            'query_response_text': query_response.text,
            'progress_response_text': progress_response.text,
            'answers': answers
        }

    @staticmethod
    def _parse_all_assertions(soup) -> str:
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

    def alpha_paging(self) -> List:
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

    def _fetch_alpha_index(self, start=None) -> Tuple[List, str]:
        if start is None:
            url = self.base_url + "cg?cb-alpha-top"
        else:
            url = self.base_url + "cg?cb-alpha-pagedn|" + urllib.parse.quote(start)

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

