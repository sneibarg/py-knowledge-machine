def rdf_to_krl_name(uri):
    return uri.split('/')[-1].split('#')[-1].replace(':', '_')