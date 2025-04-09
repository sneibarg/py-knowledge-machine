# def rdf_to_krl_name(uri):
#     return uri.split('/')[-1].split('#')[-1].replace(':', '_')

def rdf_to_krl_name(uri):
    """Convert an RDF URI to a KM-compatible name (fallback)."""
    return str(uri).split('/')[-1]