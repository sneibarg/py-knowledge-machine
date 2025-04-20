import os
import logging
import requests
from functools import partial
from multiprocessing import Pool, Manager, current_process
from km_syntax import KMSyntaxGenerator
from ontology_loader import load_ontology
from utils import extract_labels_and_ids, extract_subject

# Constants for KM server
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


def has_assertion_for_subject(all_subjects, subject):
    """
    Check if there is a subject in all_subjects that matches the given subject.
    """
    return subject in all_subjects


def init_worker(opencyc, opencyc_map, log_files):
    """
    Initialize the KMSyntaxGenerator and logger in each worker process.
    """
    global km_generator, worker_logger, global_assertions
    km_generator = KMSyntaxGenerator(opencyc, opencyc_map)

    # Load assertions from log files within the worker
    global_assertions = []
    for log_file in log_files:
        log_path = os.path.join("logs", log_file)
        global_assertions.extend(extract_assertions_from_log(log_path))

    # Set up logger with process name
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
    payload = {"expr": assertion, "fail_mode": FAIL_MODE}
    headers = {"Content-Type": "application/json"}
    try:
        #response = requests.post(KM_URL, json=payload, headers=headers)
        #if response.status_code == 200:
        response = None
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


def process_assertion(index, assertion, successfully_sent, all_subjects):
    """
    Process a single assertion, sending its dependencies first if not in shared memory.
    Returns (index, result_dict) for ordered output.
    """
    worker_logger.info(f"Processing assertion {index}: {assertion}...")
    try:
        # Get prerequisites
        prerequisites = km_generator.get_prerequisite_assertions(assertion)
        worker_logger.info(f"Found {len(prerequisites)} prerequisites.")

        # Ensure all prerequisites are sent
        for prereq in prerequisites:
            prereq_subject = extract_subject(prereq)
            if not prereq_subject:
                worker_logger.warning(f"Could not extract subject from prerequisite: {prereq}")
                continue
            if prereq_subject in successfully_sent:
                worker_logger.info(f"Prerequisite '{prereq_subject}' already sent.")
                continue
            if prereq_subject not in all_subjects:
                worker_logger.warning(f"Prerequisite subject '{prereq_subject}' not found in assertions.")
                continue

            # Find the assertion for this prerequisite subject
            prereq_assertion = next((a for a in global_assertions if extract_subject(a) == prereq_subject), None)
            if not prereq_assertion:
                worker_logger.error(f"No assertion found for prerequisite subject '{prereq_subject}'.")
                continue

            # Recursively ensure prerequisite's dependencies are sent
            prereq_prerequisites = km_generator.get_prerequisite_assertions(prereq_assertion)
            for sub_prereq in prereq_prerequisites:
                sub_prereq_subject = extract_subject(sub_prereq)
                if sub_prereq_subject and sub_prereq_subject not in successfully_sent and sub_prereq_subject in all_subjects:
                    sub_prereq_assertion = next(
                        (a for a in global_assertions if extract_subject(a) == sub_prereq_subject), None)
                    if sub_prereq_assertion:
                        worker_logger.info(f"Sending sub-prerequisite '{sub_prereq_subject}'.")
                        if send_assertion(sub_prereq_assertion, successfully_sent):
                            worker_logger.info(f"Sub-prerequisite '{sub_prereq_subject}' sent successfully.")
                        else:
                            worker_logger.error(f"Failed to send sub-prerequisite '{sub_prereq_subject}'.")

            # Send the prerequisite
            worker_logger.info(f"Sending prerequisite '{prereq_subject}'.")
            if send_assertion(prereq_assertion, successfully_sent):
                worker_logger.info(f"Prerequisite '{prereq_subject}' sent successfully.")
            else:
                worker_logger.error(f"Failed to send prerequisite '{prereq_subject}'.")

        # Send the main assertion
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
    all_assertions = []
    all_subjects = set()
    for log_file in logs:
        log = os.path.join("logs", log_file)
        print(f"Loading assertions for {log}")
        assertions = extract_assertions_from_log(log)
        all_assertions.extend(assertions)
        for assertion in assertions:
            subject = extract_subject(assertion)
            if subject:
                all_subjects.add(subject)
    if not all_assertions:
        print("No assertions found in log files. Process aborted.")
        return None, None
    return all_assertions, all_subjects


def test_prerequisite_assertions(data, opencyc_graph, ontology_map, num_cpus):
    """
    Test the get_prerequisite_assertions function using assertions from log files,
    distributing the workload across specified CPUs.
    """
    assertions, subjects = data
    if assertions is None:
        return
    manager = Manager()
    log_dict = manager.dict()
    pool = Pool(processes=num_cpus, initializer=init_worker, initargs=(opencyc_graph, ontology_map, log_files))
    test_func = partial(test_single_assertion, (subjects, log_dict))
    results = pool.starmap(test_func, [(i, assertion) for i, assertion in enumerate(assertions, 1)])
    pool.close()
    pool.join()
    results.sort(key=lambda x: x[0])
    total_missing = 0
    for index, result in results:
        for key in sorted(log_dict.keys()):
            if key.startswith(f"Testing {index}") or key.startswith(f"Prereqs {index}") or \
                    key.startswith(f"Warning {index}") or key.startswith(f"Check {index}") or \
                    key.startswith(f"Summary {index}") or key.startswith(f"Error {index}"):
                print(log_dict[key])
        if "error" in result:
            total_missing += 1
        elif not result["all_prereqs_found"]:
            total_missing += len(result["missing_prereqs"])
    print(f"\nTest completed. Total assertions with missing prerequisites: {total_missing}")


def send_assertions_with_dependencies(all_assertions, opencyc_graph, ontology_map, num_cpus):
    """
    Send assertions to KM server, with each process ensuring dependencies are sent first.
    """
    if not all_assertions:
        print("No assertions to send.")
        return
    manager = Manager()
    successfully_sent = manager.dict()  # Shared memory: {subject: assertion}
    all_subjects = set(extract_subject(a) for a in all_assertions if extract_subject(a))

    # Create logs directory if it doesn't exist
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Process assertions in parallel
    with Pool(processes=num_cpus, initializer=init_worker, initargs=(opencyc_graph, ontology_map, log_files)) as pool:
        results = pool.starmap(
            partial(process_assertion, successfully_sent=successfully_sent, all_subjects=all_subjects),
            [(i, assertion) for i, assertion in enumerate(all_assertions, 1)]
        )

    # Process results
    results.sort(key=lambda x: x[0])  # Sort for ordered output
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

    print(f"\nSending completed. Successfully sent {successes} out of {len(all_assertions)} assertions.")
    # Save successfully sent assertions to file
    with open("successfully_sent_assertions.txt", "w") as f:
        for assertion in successfully_sent.values():
            f.write(assertion + "\n")
    return successfully_sent


def test_single_assertion(args, index, assertion):
    """
    Test a single assertion and return the result.
    """
    all_subjects, log_dict = args
    try:
        worker_logger.info(f"Testing assertion {index}: {assertion[:100]}...")
        prerequisites = km_generator.get_prerequisite_assertions(assertion)
        log_dict[f"Prereqs {index}"] = f"  Found {len(prerequisites)} prerequisites."
        all_prereqs_found = True
        missing_prereqs = []
        for prereq in prerequisites:
            subject = extract_subject(prereq)
            if subject is None:
                log_dict[f"Warning {index}"] = f"  Warning: Could not extract subject from prerequisite: {prereq}"
                all_prereqs_found = False
                continue
            if has_assertion_for_subject(all_subjects, subject):
                log_dict[
                    f"Check {index} {subject}"] = f"  ✓ Prerequisite subject '{subject}' has a corresponding assertion."
            else:
                log_dict[f"Check {index} {subject}"] = f"  ✗ Missing assertion for prerequisite subject '{subject}'."
                all_prereqs_found = False
                missing_prereqs.append(subject)
        status = "All prerequisites verified successfully." if all_prereqs_found else "Some prerequisites could not be verified."
        log_dict[f"Summary {index}"] = f"  {status}"
        return index, {
            "assertion": assertion[:100] + "...",
            "prerequisites_found": len(prerequisites),
            "all_prereqs_found": all_prereqs_found,
            "missing_prereqs": missing_prereqs
        }
    except Exception as e:
        log_dict[f"Error {index}"] = f"  Error processing assertion {index}: {str(e)}"
        return index, {
            "assertion": assertion[:100] + "...",
            "error": str(e)
        }


if __name__ == "__main__":
    num_processes = int(os.cpu_count() - 2)
    log_files = [
        "property_batch_0_20250411_225935.log",
        "class_batch_0_20250411_225935.log",
        "individual_batch_0_20250411_225935.log"
    ]
    graph = load_ontology()
    object_map = extract_labels_and_ids(graph)
    all_assertions, all_subjects = get_all_assertions(log_files)
    if all_assertions:
        # test_prerequisite_assertions((all_assertions, all_subjects), graph, object_map, num_processes)
        send_assertions_with_dependencies(all_assertions, graph, object_map, num_processes)