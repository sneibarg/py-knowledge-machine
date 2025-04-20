import logging
import os
from functools import partial
from multiprocessing import Pool, Manager, current_process
from km_syntax import KMSyntaxGenerator
from rest_client import send_to_km
from utils import extract_subject

KM_URL = "http://km-server-url"  # Replace with actual KM server URL
FAIL_MODE = "some_mode"  # Replace with actual fail_mode


def extract_assertions_from_log(log_file):
    """
    Extract KM assertions from a log file.
    Returns a list of assertion strings.
    """
    assertions = []
    try:
        with open(log_file, 'r') as f:
            for line in f:
                if "Generated: " in line and " | Result:" in line:
                    start = line.index("Generated: ") + len("Generated: ")
                    end = line.index(" | Result:")
                    assertion = line[start:end].strip()
                    assertions.append(assertion)
    except FileNotFoundError:
        print(f"Warning: Log file {log_file} not found.")
    return assertions


def has_assertion_for_subject(subject_list, subject):
    """
    Check if there is a subject in all_subjects that matches the given subject.
    """
    return subject in subject_list


def init_worker(assertion_list):
    """
    Initialize the KMSyntaxGenerator and logger in each worker process.
    """
    global km_generator, worker_logger, global_assertions
    km_generator = KMSyntaxGenerator()
    global_assertions = assertion_list

    process_name = current_process().name
    log_file = f"logs/{process_name}.log"
    worker_logger = logging.getLogger(process_name)
    worker_logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    worker_logger.addHandler(handler)


def send_assertion(assertion, successfully_sent):
    """
    Send an assertion to the KM server and update shared memory if successful.
    Returns True if sent successfully, False otherwise.
    """
    worker_logger.info(f"Sending assertion: {assertion[:100]}...")

    try:
        response = None
        # response = send_to_km(assertion, dry_run=True)
        # if not "Error" in response:
        if response is None:
            subject = extract_subject(assertion)
            if subject:
                successfully_sent[subject] = assertion
                worker_logger.info(f"Successfully sent assertion for subject '{subject}'.")
            else:
                worker_logger.warning("No subject extracted from assertion.")
            return True
        else:
            worker_logger.warning(f"Failed to send assertion: Status {response.status_code}")
            return False
    except Exception as e:
        worker_logger.error(f"Error sending assertion: {str(e)}")
        return False


def process_assertion(index, assertion, successfully_sent, subject_list):
    """
    Process a single assertion, sending its dependencies first if not in shared memory.
    Returns (index, result_dict) for ordered output.
    """
    worker_logger.info(f"Processing assertion {index}: {assertion[:100]}...")
    try:
        prerequisites = km_generator.get_prerequisite_assertions(assertion)
        worker_logger.info(f"Found {len(prerequisites)} prerequisites.")

        for prereq in prerequisites:
            prereq_subject = extract_subject(prereq)
            if not prereq_subject:
                worker_logger.warning(f"Could not extract subject from prerequisite: {prereq}")
                continue
            if prereq_subject in successfully_sent:
                worker_logger.info(f"Prerequisite '{prereq_subject}' already sent.")
                continue
            if prereq_subject not in subject_list:
                worker_logger.warning(f"Prerequisite subject '{prereq_subject}' not found in assertions.")
                continue

            prereq_assertion = next((a for a in global_assertions if extract_subject(a) == prereq_subject), None)
            if not prereq_assertion:
                worker_logger.error(f"No assertion found for prerequisite subject '{prereq_subject}'.")
                continue

            prereq_prerequisites = km_generator.get_prerequisite_assertions(prereq_assertion)
            for sub_prereq in prereq_prerequisites:
                sub_prereq_subject = extract_subject(sub_prereq)
                if sub_prereq_subject and sub_prereq_subject not in successfully_sent and sub_prereq_subject in subject_list:
                    sub_prereq_assertion = next(
                        (a for a in global_assertions if extract_subject(a) == sub_prereq_subject), None)
                    if sub_prereq_assertion:
                        worker_logger.info(f"Sending sub-prerequisite '{sub_prereq_subject}'.")
                        if send_assertion(sub_prereq_assertion, successfully_sent):
                            worker_logger.info(f"Sub-prerequisite '{sub_prereq_subject}' sent successfully.")
                        else:
                            worker_logger.error(f"Failed to send sub-prerequisite '{sub_prereq_subject}'.")

            worker_logger.info(f"Sending prerequisite '{prereq_subject}'.")
            if send_assertion(prereq_assertion, successfully_sent):
                worker_logger.info(f"Prerequisite '{prereq_subject}' sent successfully.")
            else:
                worker_logger.error(f"Failed to send prerequisite '{prereq_subject}'.")

        if send_assertion(assertion, successfully_sent):
            return index, {"assertion": assertion[:100] + "...", "status": "success"}
        else:
            return index, {"assertion": assertion[:100] + "...", "status": "failed"}
    except Exception as e:
        worker_logger.error(f"Error processing assertion {index}: {str(e)}")
        return index, {"assertion": assertion[:100] + "...", "status": "error", "error": str(e)}


def get_all_assertions(logs):
    """
    Collect all assertions and their subjects from log files.
    """
    assertion_list = []
    subject_list = set()
    for log_file in logs:
        log = os.path.join("logs", log_file)
        print(f"Loading assertions for {log}")
        assertions = extract_assertions_from_log(log)
        assertion_list.extend(assertions)
        for assertion in assertions:
            subject = extract_subject(assertion)
            if subject:
                subject_list.add(subject)
    if not assertion_list:
        print("No assertions found in log files. Process aborted.")
        return None, None
    return assertion_list, subject_list


def send_assertions_with_dependencies(assertion_list, num_cpus):
    """
    Send assertions to KM server, with each process ensuring dependencies are sent first.
    """
    if not assertion_list:
        print("No assertions to send.")
        return
    manager = Manager()
    successfully_sent = manager.dict()
    subject_list = set(extract_subject(a) for a in assertion_list if extract_subject(a))
    if not os.path.exists("logs"):
        os.makedirs("logs")

    with Pool(processes=num_cpus, initializer=init_worker,
              initargs=assertion_list) as pool:
        results = pool.starmap(
            partial(process_assertion, successfully_sent=successfully_sent, all_subjects=subject_list),
            [(i, assertion) for i, assertion in enumerate(assertion_list, 1)]
        )

    results.sort(key=lambda x: x[0])
    successes = 0
    for index, result in results:
        print(f"Assertion {index}: {result['assertion']}")
        if result["status"] == "success":
            print("  Successfully sent.")
            successes += 1
        elif result["status"] == "failed":
            print("  Failed to send.")
        else:
            print(f"  Error: {result['error']}")

    print(f"\nSending completed. Successfully sent {successes} out of {len(assertion_list)} assertions.")
    with open("successfully_sent_assertions.txt", "w") as f:
        for assertion in successfully_sent.values():
            f.write(assertion + "\n")
    return successfully_sent


if __name__ == "__main__":
    num_processes = int(os.cpu_count() / 2)
    log_files = [
        "property_batch_0_20250411_225935.log",
        "class_batch_0_20250411_225935.log",
        "individual_batch_0_20250411_225935.log"
    ]
    all_assertions, all_subjects = get_all_assertions(log_files)
    if all_assertions:
        send_assertions_with_dependencies(all_assertions, num_processes)
