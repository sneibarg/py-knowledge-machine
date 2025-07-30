import os
import re
import time
import rdflib
from collections import defaultdict
from functools import partial
from rdflib import Literal, URIRef, RDFS, RDF, OWL
from service.KMSyntaxService import STANDARD_PREDICATES


class OWLGraphProcessor:
    def __init__(self, parent_logger, pool, ontology_service, args):
        self.logger = parent_logger.getChild('OWL-Graph-Processor')
        self.pool = pool
        self.ontology_service = ontology_service
        self.args = args
        self.graph = self.load_ontology()
        self.successfully_sent = []

    def run(self, assertion_list) -> int:
        remaining_assertions = set(assertion_list)
        failed_assertions = {}
        progress_made = True

        if self.pool is None:
            self.logger.info("Attempted to invoke run() without a ForkJoinPool.")
            return 0

        while remaining_assertions and progress_made:
            new_remaining = set()
            progress_made = False
            process_func = partial(self.ontology_service.preprocess, dry_run=self.args.dry_run)
            results = self.pool.map(process_func, remaining_assertions)
            for assertion, success in zip(remaining_assertions, results):
                if not success:
                    new_remaining.add(assertion)
                    if assertion not in failed_assertions:
                        failed_assertions[assertion] = "unknown_failure"
                elif assertion in self.successfully_sent:
                    progress_made = True
            remaining_assertions = new_remaining

        if remaining_assertions:
            self.logger.info(f"Unprocessed assertions: {len(remaining_assertions)}")
            self.logger.info(f"Failure reasons: {dict(failed_assertions)}")

        return len(self.successfully_sent)

    def extract_labels_and_ids(self):
        child_logger = self.logger.getChild('LabelsExtractor')
        result = {}
        for subject in self.graph.subjects():
            label = next((str(obj) for obj in self.graph.objects(subject, rdflib.RDFS.label) if isinstance(obj, rdflib.Literal)), None)
            external_id = next((str(obj) for obj in self.graph.objects(subject, rdflib.OWL.sameAs) if isinstance(obj, rdflib.URIRef)), None)
            if label or external_id:
                result[subject] = {'label': label, 'external_id': external_id}
        child_logger.info(f"Extracted labels/IDs for {len(result)} resources.")
        return result

    def get_classes_via_sparql(self):
        query = """
        SELECT ?s WHERE {
            ?s rdf:type owl:Class .
        }
        """
        try:
            return [row.s for row in self.graph.query(query)]
        except Exception as e:
            self.logger.error("SPARQL query for classes failed: %s", str(e))
            raise

    def get_properties_via_sparql(self):
        query = """
        SELECT ?s WHERE {
            ?s rdf:type owl:ObjectProperty .
        }
        """
        try:
            return [row.s for row in self.graph.query(query)]
        except Exception as e:
            self.logger.error("SPARQL query for properties failed: %s", str(e))
            raise

    def get_individuals_via_sparql(self):
        query = """
        SELECT ?ind ?class WHERE {
            ?ind rdf:type ?class .
            ?class rdf:type owl:Class .
            FILTER (?class != owl:Class)
        }
        """
        try:
            return [(row.ind, row['class']) for row in self.graph.query(query)]
        except Exception as e:
            self.logger.error("SPARQL query for individuals failed: %s", str(e))
            raise

    def load_ontology(self):
        start_time = time.time()
        onto_logger = self.logger.getChild('OntologyLoader')

        if self.ontology_service.preprocessed_file is not None and not os.path.exists(self.ontology_service.preprocessed_file):
            onto_logger.info("Preprocessed OWL file not found. Triggering preprocessing.")
            try:
                self.ontology_service.preprocess(self.ontology_service.file)
            except Exception as e:
                raise RuntimeError(f"Preprocessing failed: {e}") from e

        try:
            import oxrdflib
            with open(self.ontology_service.preprocessed_file, 'r', encoding='utf-8') as f:
                g = rdflib.Graph(store=oxrdflib.OxigraphStore())
                g.parse(f, format="xml")
            onto_logger.info(f"Ontology loaded successfully with {len(g)} triples in {int(time.time() - start_time)} seconds.")
            return g
        except ImportError as ie:
            onto_logger.warning("Oxrdflib not installed: %s. Falling back to default rdflib store.", str(ie))
            g = rdflib.Graph()
        except Exception as e:
            onto_logger.warning("Failed to initialize Oxrdflib: %s. Falling back to default rdflib store.", str(e))
            g = rdflib.Graph()
        return g

    def print_classes(self, object_map):
        for subject in self.graph.subjects(RDF.type, OWL.Class):
            print(f"\nClass URI: {subject}")
            for predicate, obj in self.graph.predicate_objects(subject):
                pred_name = str(predicate).split('#')[-1] if '#' in str(predicate) else str(predicate).split('/')[-1]
                if pred_name in STANDARD_PREDICATES:
                    pred_name = STANDARD_PREDICATES[pred_name]
                if obj in object_map:
                    obj = object_map[obj]['label']
                print(f"  {pred_name}: {obj}")

    def print_properties(self, object_map):
        for subject in self.graph.subjects(RDF.type, OWL.ObjectProperty):
            print(f"\nProperty URI: {subject}")
            for predicate, obj in self.graph.predicate_objects(subject):
                pred_name = str(predicate).split('#')[-1] if '#' in str(predicate) else str(predicate).split('/')[-1]
                if pred_name in STANDARD_PREDICATES:
                    pred_name = STANDARD_PREDICATES[pred_name]
                if obj in object_map:
                    obj = object_map[obj]['label']
                print(f"  {pred_name}: {obj}")

    def print_records(self):
        records = defaultdict(list)
        for s, p, o in self.graph:
            records[s].append((p, o))

        for subject, props in records.items():
            subj_label = self.find_rdfs_label(subject) or self.pretty(subject)
            print(f"Subject: {subj_label}")
            for p, o in props:
                pred_label = self.pretty_predicate(p)
                obj_label = self.pretty(o)
                print(f"  {pred_label}: {obj_label}")
            print()

    def pretty(self, node):
        if isinstance(node, Literal):
            return str(node)
        elif isinstance(node, URIRef):
            label = self.find_rdfs_label(node)
            if label:
                return label
            m = re.search(r"(Mx[0-9A-Za-z\-]+)$", str(node))
            if m:
                concept_id = m.group(1)
                for base in BASES:
                    uri = URIRef(base + concept_id)
                    label = self.find_rdfs_label(uri)
                    if label:
                        return label
            return self.short_name(node)
        elif self.ontology_service.custom_matching_function(node):
            uri = self.get_full_uri(node)
            label = self.find_rdfs_label(uri)
            if label:
                return label
            return node
        else:
            return str(node)

    def pretty_predicate(self, node):
        return self.find_predicate_label(node)

    def find_predicate_label(self, node):
        if isinstance(node, str) and self.ontology_service.custom_matching_function(node):
            node = self.get_full_uri(node)
        for label in self.graph.objects(node, annotation_label):
            if not hasattr(label, 'language') or label.language is None or label.language == 'en':
                return str(label)
        for label in self.graph.objects(node, RDFS.label):
            if not hasattr(label, 'language') or label.language is None or label.language == 'en':
                return str(label)
        if isinstance(node, URIRef):
            return node.split('#')[-1] if '#' in node else node.split('/')[-1]
        return str(node)

    def get_full_uri(self, val):
        for base in BASES:
            uri = URIRef(base + val)
            if (uri, None, None) in self.graph or (None, None, uri) in self.graph:
                return uri
        return URIRef(BASES[0] + val)

    def find_rdfs_label(self, node):
        for label in self.graph.objects(node, RDFS.label):
            if not hasattr(label, 'language') or label.language is None or label.language == 'en':
                return str(label)
        return None

    @staticmethod
    def set_bases(bases):
        global BASES
        BASES = bases

    @staticmethod
    def set_assertions(assertion_list):
        global assertions
        assertions = assertion_list

    @staticmethod
    def set_annotation_label(ANNOT_LABEL):
        global annotation_label
        annotation_label = ANNOT_LABEL

    @staticmethod
    def short_name(node):
        if isinstance(node, URIRef):
            return node.split('#')[-1] if '#' in node else node.split('/')[-1]
        return str(node)

