# def rdf_to_krl_name(uri):
#     return uri.split('/')[-1].split('#')[-1].replace(':', '_')
import rdflib
from rdflib import OWL, RDFS


def rdf_to_krl_name(uri):
    """Convert an RDF URI to a KM-compatible name (fallback)."""
    return str(uri).split('/')[-1]


def extract_labels_and_ids(graph):
    """
    Extract labels and external IDs for each resource in the RDF graph.

    Args:
        graph (rdflib.Graph): The RDF graph to process.

    Returns:
        dict: A dictionary where keys are resources (URIs or blank nodes) and values
              are dictionaries containing 'label' and 'external_id'.
    """
    result = {}
    for subject in graph.subjects():
        label = None
        external_id = None

        for obj in graph.objects(subject, RDFS.label):
            if isinstance(obj, rdflib.Literal):
                label = str(obj)
                break

        for obj in graph.objects(subject, OWL.sameAs):
            if isinstance(obj, rdflib.URIRef):
                external_id = str(obj)
                break

        if label or external_id:
            result[subject] = {
                'label': label,
                'external_id': external_id
            }

    return result
