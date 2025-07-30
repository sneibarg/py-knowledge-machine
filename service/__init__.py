import requests
from requests import Session

cb_handle_assert = {
    'cb-handle-assert': 'T',
    'assert': 'Assert Sentence',
    'assertion-queue': ':local',
    'strength': ':default',
    'mt-monad': 'BaseKB',
    'mt-time-dimension-specified': 'na',
    'mt-time-interval': 'Always-TimeInterval',
    'mt-time-parameter': "Null-TimeParameter",
    'sentence': None,
    'uniquifier-code': None
}

cb_handle_create = {
    'cb-handle-create': 'T',
    'new-name': None,
    'uniquifier-code': None
}

cb_handle_query = {
    'cb-handle-query': '',
    'new': 'Start Inference',
    'mt-monad': 'CurrentWorldDataCollectorMt-NonHomocentric',
    'mt-time-dimension-specified': 't',
    'mt-time-interval': 'Now',
    'mt-time-parameter': 'Null-TimeParameter',
    'sentence': '',
    'non_exp_sentence': '',
    'entry-MAX-NUMBER': '',
    'radio-MAX-NUMBER': '1',
    'radio-MAX-TIME': '0',
    'entry-MAX-TIME': '30',
    'entry-MAX-STEP': '',
    'radio-MAX-STEP': '1',
    'radio-INFERENCE-MODE': '1',
    'radio-MAX-TRANSFORMATION-DEPTH': '0',
    'entry-MAX-TRANSFORMATION-DEPTH': '1',
    'radio-NEW-TERMS-ALLOWED?': '0',
    'entry-MAX-PROOF-DEPTH': '',
    'radio-MAX-PROOF-DEPTH': '1',
    'radio-ALLOW-HL-PREDICATE-TRANSFORMATION?': '0',
    'radio-ALLOW-UNBOUND-PREDICATE-TRANSFORMATION?': '0',
    'radio-ALLOW-EVALUATABLE-PREDICATE-TRANSFORMATION?': '1',
    'radio-TRANSFORMATION-ALLOWED?': '0',
    'radio-REMOVAL-BACKTRACKING-PRODUCTIVITY-LIMIT': '0',
    'entry-REMOVAL-BACKTRACKING-PRODUCTIVITY-LIMIT': '0',
    'radio-PRODUCTIVITY-LIMIT': '0',
    'entry-PRODUCTIVITY-LIMIT': '200000.0',
    'radio-MAX-PROBLEM-COUNT': '0',
    'entry-MAX-PROBLEM-COUNT': '100000',
    'radio-TRANSITIVE-CLOSURE-MODE': '0',
    'radio-ADD-RESTRICTION-LAYER-OF-INDIRECTION?': '1',
    'radio-MIN-RULE-UTILITY': '0',
    'entry-MIN-RULE-UTILITY': '-100',
    'entry-PROBABLY-APPROXIMATELY-DONE': '100.0',
    'radio-PROBABLY-APPROXIMATELY-DONE': '1',
    'radio-FORWARD-MAX-TIME': '0',
    'entry-FORWARD-MAX-TIME': '0',
    'radio-BLOCK?': '0',
    'radio-CACHE-INFERENCE-RESULTS?': '0',
    'radio-ANSWER-LANGUAGE': '0',
    'radio-CONTINUABLE?': '1',
    'radio-METRICS': '0',
    'entry-METRICS': '',
    'radio-ALLOW-INDETERMINATE-RESULTS?': '0',
    'radio-ALLOW-ABNORMALITY-CHECKING?': '1',
    'radio-RESULT-UNIQUENESS': '0',
    'radio-DISJUNCTION-FREE-EL-VARS-POLICY': '1',
    'entry-ALLOWED-MODULES': '',
    'radio-ALLOWED-MODULES': '1',
    'radio-NEGATION-BY-FAILURE?': '0',
    'radio-COMPLETENESS-MINIMIZATION-ALLOWED?': '1',
    'radio-DIRECTION': '0',
    'radio-EQUALITY-REASONING-METHOD': '0',
    'radio-EQUALITY-REASONING-DOMAIN': '0',
    'radio-INTERMEDIATE-STEP-VALIDATION-LEVEL': '3',
    'radio-EVALUATE-SUBL-ALLOWED?': '1',
    'radio-REWRITE-ALLOWED?': '0',
    'radio-ABDUCTION-ALLOWED?': '0',
    'radio-COMPUTE-ANSWER-JUSTIFICATIONS?': '1',
    'uniquifier-code': ''
}

cb_continue_query = cb_handle_query.copy()
cb_continue_query['continue'] = 'Continue the Focal Inference'


def get_session(max_retries=0) -> Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=max_retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def update_cyc_payload(payload, sentence=None, mt_monad=None, uniquifier=None, **kwargs):
    """
    Update the payload dictionary with query-specific values.

    Args:
        payload (dict): The base post_data dictionary to update.
        sentence (str): The CycL query sentence.
        mt_monad (str, optional): The microtheory to use. Defaults to post_data's mt-monad.
        uniquifier (str, optional): The uniquifier code. Defaults to empty string.
        **kwargs: Additional parameters to override in post_data.

    Returns:
        dict: Updated post_data dictionary.
    """
    updated_payload = payload.copy()
    updated_payload['sentence'] = sentence
    if mt_monad:
        updated_payload['mt-monad'] = mt_monad
    if uniquifier:
        updated_payload['uniquifier-code'] = uniquifier
        radio_keys = [key for key in updated_payload if key.startswith('radio-') and not key.endswith(uniquifier)]
        for key in radio_keys:
            new_key = f"{key}_{uniquifier}"
            updated_payload[new_key] = updated_payload[key]
            del updated_payload[key]
    for key, value in kwargs.items():
        updated_payload[key] = value
    return updated_payload
