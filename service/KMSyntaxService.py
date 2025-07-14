import sys
from typing import List

import rdflib
import json
import re

from service.KnowledgeMachineService import rdf_to_krl_name, STANDARD_PREDICATES, BUILT_IN_FRAMES
from service.OpenCycService import cyc_annot_label, TYPE_PREDICATES


class KMSyntaxGenerator:
    def __init__(self, graph, object_map, parent_logger):
        self.graph = graph
        self.object_map = object_map
        self.logger = parent_logger.getChild('KMSyntaxGenerator')
        self.resource_names = self.build_resource_names()
        self.predicate_names = self.build_predicate_names()
        self.logger.info("Initialized with %d resources.", len(self.resource_names))

    def build_resource_names(self) -> dict:
        names = {}
        self.logger.info("Building resource names...")
        for s in self.graph.subjects():
            if s in self.object_map and 'label' in self.object_map[s]:
                names[s] = self.object_map[s]['label']
            else:
                labels = [str(o) for o in self.graph.objects(s, cyc_annot_label) if isinstance(o, rdflib.Literal)]
                if labels:
                    names[s] = next((label for label in labels if label[0].isupper()), labels[0])
                else:
                    names[s] = rdf_to_krl_name(s)
        self.logger.info("Completed building %d resource names.", len(names))
        return names

    def build_predicate_names(self) -> dict:
        names = STANDARD_PREDICATES.copy()
        used_names = set(names.values())
        self.logger.info("Building predicate names...")
        for pred in self.graph.predicates():
            if pred in TYPE_PREDICATES:
                names[pred] = "instance-of"
            elif pred not in names:
                if pred in self.object_map and 'label' in self.object_map[pred]:
                    base_name = self.object_map[pred]['label']
                else:
                    base_name = rdf_to_krl_name(pred)
                name = base_name
                i = 1
                while name in used_names:
                    name = f"{base_name}_{i}"
                    i += 1
                names[pred] = name
                used_names.add(name)
        self.logger.info("Completed building %d predicate names.", len(names))
        return names

    def get_resource_name(self, resource) -> str:
        return self.resource_names.get(resource, rdf_to_krl_name(resource))

    def get_slot_name(self, predicate) -> str:
        return self.predicate_names.get(predicate, rdf_to_krl_name(predicate))

    def translate_assertion(self, assertion) -> str:
        expr = None
        if assertion[0] == "class":
            expr = self.class_to_km(assertion[1])
        elif assertion[0] == "property":
            expr = self.property_to_km(assertion[1])
        elif assertion[0] == "individual":
            expr = self.individual_to_km(assertion[1][0])
        return expr

    def individual_to_km(self, ind_uri) -> str:
        ind_name = self.get_resource_name(ind_uri)
        slots = {}
        self.logger.debug("Converting individual %s to KM syntax...", ind_name)
        for prop, obj in self.graph.predicate_objects(ind_uri):
            prop_name = self.get_slot_name(prop)
            value = self.get_resource_name(obj) if isinstance(obj, rdflib.URIRef) else json.dumps(str(obj))
            slots.setdefault(prop_name, []).append(value)
        expr = f"({ind_name} has"
        for slot, values in slots.items():
            unique_values = list(dict.fromkeys(values))
            expr += f" ({slot} ({' '.join(unique_values)}))"
        expr += ")"
        self.logger.debug("Generated KM for individual: %s...", expr)
        return expr

    def class_to_km(self, class_uri) -> str:
        frame_name = self.get_resource_name(class_uri)
        print(f"KM class given frame name {frame_name} for {class_uri}")
        slots = {}
        self.logger.debug("Converting class %s to KM syntax...", frame_name)
        for pred, obj in self.graph.predicate_objects(class_uri):
            slot_name = self.get_slot_name(pred)
            value = self.get_resource_name(obj) if isinstance(obj, rdflib.URIRef) else json.dumps(str(obj))
            slots.setdefault(slot_name, []).append(value)
        expr = f"({frame_name} has"
        for slot, values in slots.items():
            for value in values:
                if value == "owl#Class":
                    continue
                if slot in STANDARD_PREDICATES:
                    slot = STANDARD_PREDICATES[slot]
                # print(f"SLOT={slot}; VALUE={value}")
                expr += f" ({slot} ({' '.join(values)}))"
        expr += ")"
        print(f"Generated KM for class: {expr}")
        self.logger.debug("Generated KM for class: %s...", expr)
        return expr

    def property_to_km(self, prop_uri) -> str:
        prop_name = self.get_resource_name(prop_uri)
        self.logger.debug("Converting property %s to KM syntax...", prop_name)
        labels = [json.dumps(str(label)) for label in self.graph.objects(prop_uri, rdflib.RDFS.label)]
        domains = [self.get_resource_name(d) for d in self.graph.objects(prop_uri, rdflib.RDFS.domain)]
        ranges = [self.get_resource_name(r) for r in self.graph.objects(prop_uri, rdflib.RDFS.range)]
        superslots = [self.get_resource_name(sp) for sp in self.graph.objects(prop_uri, rdflib.RDFS.subPropertyOf)]
        inverses = [self.get_resource_name(i) for i in self.graph.objects(prop_uri, rdflib.OWL.inverseOf)]
        expr = f"({prop_name} has (instance-of (Slot))"
        if labels:
            expr += f" (label ({' '.join(labels)}))"
        if domains:
            expr += f" (domain ({' '.join(domains)}))"
        if ranges:
            expr += f" (range ({' '.join(ranges)}))"
        if superslots:
            expr += f" (superslots ({' '.join(superslots)}))"
        if inverses:
            expr += f" (inverse ({' '.join(inverses)}))"
        expr += ")"
        self.logger.debug("Generated KM for property: %s...", expr)
        return expr

    def get_referenced_assertions(self, assertion) -> List:
        self.logger.info(f"Getting reference assertions for {assertion}")
        clean_assertion = re.sub(r'"[^"]*"', '', assertion)
        self.logger.debug("Cleaned assertion: %s...", clean_assertion[:100])
        symbols = re.findall(r'[-\w]+', clean_assertion)
        referenced_frames = set(sym for sym in symbols if sym not in BUILT_IN_FRAMES)
        name_to_uri = {name: u for u, name in self.resource_names.items()}
        referenced_uris = [name_to_uri[frame_name] for frame_name in referenced_frames if frame_name in name_to_uri]

        ref_assertions = []
        for ref_uri in referenced_uris:
            ref_type = self.get_uri_type(ref_uri)
            if ref_type == "class":
                ref_assertions.append(("class", ref_uri))
            elif ref_type == "property":
                ref_assertions.append(("property", ref_uri))
            elif ref_type == "individual":
                classes = [o for s, o in self.graph.subject_objects(rdflib.RDF.type)
                           if s == ref_uri and (o, rdflib.RDF.type, rdflib.OWL.Class) in self.graph]
                for class_uri in classes:
                    ref_assertions.append(("individual", (ref_uri, class_uri)))
        self.logger.debug("Found %d referenced assertions.", len(ref_assertions))
        return ref_assertions

    def get_uri_type(self, uri) -> str:
        if (uri, rdflib.RDF.type, rdflib.OWL.Class) in self.graph:
            return "class"
        elif (uri, rdflib.RDF.type, rdflib.OWL.ObjectProperty) in self.graph:
            return "property"
        else:
            types = list(self.graph.objects(uri, rdflib.RDF.type))
            if types and any((t, rdflib.RDF.type, rdflib.OWL.Class) in self.graph for t in types):
                return "individual"
            return None
