import rdflib
from utils import rdf_to_krl_name
import json

cyc_annot_label = rdflib.URIRef("http://sw.cyc.com/CycAnnotations_v1#label")
TYPE_PREDICATES = [
    rdflib.RDF.type,
    rdflib.URIRef("http://sw.opencyc.org/2008/06/10/concept/Mx4rBVVEokNxEdaAAACgydogAg")
]
STANDARD_PREDICATES = {
    rdflib.RDF.type: "instance-of",
    rdflib.RDFS.subClassOf: "superclasses",
    rdflib.RDFS.label: "label",
    rdflib.OWL.sameAs: "same_as",
}
BUILT_IN_FRAMES = {
    "instance-of", "superclasses", "label", "Slot", "Class", "Thing", "has",
    "with", "a", "in", "where", "then", "else", "if", "forall", "oneof", "a-prototype"
}


class KMSyntaxGenerator:
    def __init__(self, graph, object_map):
        self.graph = graph
        self.object_map = object_map
        self.resource_names = self.build_resource_names()
        self.predicate_names = self.build_predicate_names()

    def build_resource_names(self):
        names = {}
        for s in self.graph.subjects():
            if s in self.object_map and 'label' in self.object_map[s]:
                names[s] = self.object_map[s]['label']
            else:
                labels = [str(o) for o in self.graph.objects(s, cyc_annot_label) if isinstance(o, rdflib.Literal)]
                if labels:
                    names[s] = next((l for l in labels if l[0].isupper()), labels[0])
                else:
                    names[s] = rdf_to_krl_name(s)
        return names

    def build_predicate_names(self):
        names = STANDARD_PREDICATES.copy()
        used_names = set(names.values())
        for pred in self.graph.predicates():
            if pred in TYPE_PREDICATES:  # Ensure type predicates map to "instance-of"
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
        return names

    def get_resource_name(self, resource):
        return self.resource_names.get(resource, rdf_to_krl_name(resource))

    def get_slot_name(self, predicate):
        return self.predicate_names.get(predicate, rdf_to_krl_name(predicate))

    def individual_to_km(self, ind_uri):
        ind_name = self.get_resource_name(ind_uri)
        slots = {}
        for prop, obj in self.graph.predicate_objects(ind_uri):
            prop_name = self.get_slot_name(prop)
            if isinstance(obj, rdflib.URIRef):
                value = self.get_resource_name(obj)
            else:
                value = json.dumps(str(obj))
            slots.setdefault(prop_name, []).append(value)

        expr = f"({ind_name} has"
        for slot, values in slots.items():
            unique_values = list(dict.fromkeys(values))  # Remove duplicates
            expr += f" ({slot} ({' '.join(unique_values)}))"
        expr += ")"
        return expr

    def get_prerequisite_assertions(self, assertion):
        """Return a list of prerequisite assertions for the given KM assertion using pattern matching."""
        clean_assertion = re.sub(r'"[^"]*"', '', assertion)
        symbols = re.findall(r'[-\w]+', clean_assertion)
        subject = symbols[0] if symbols else None
        referenced_frames = set(
            sym for sym in symbols
            if sym != subject and sym not in BUILT_IN_FRAMES
        )
        name_to_uri = {name: uri for uri, name in self.resource_names.items()}
        prerequisites = []
        for frame_name in referenced_frames:
            if frame_name in name_to_uri:
                uri = name_to_uri[frame_name]
                if (uri, rdflib.RDF.type, rdflib.OWL.Class) in self.graph:
                    prerequisites.append(self.class_to_km(uri))
                elif (uri, rdflib.RDF.type, rdflib.OWL.ObjectProperty) in self.graph:
                    prerequisites.append(self.property_to_km(uri))
                else:
                    prerequisites.append(self.individual_to_km(uri))
        return prerequisites

    def class_to_km(self, class_uri):
        """Convert an OWL class to KM syntax."""
        frame_name = self.get_resource_name(class_uri)
        slots = {}
        for pred, obj in self.graph.predicate_objects(class_uri):
            slot_name = self.get_slot_name(pred)
            if isinstance(obj, rdflib.URIRef):
                value = self.get_resource_name(obj)
            else:
                value = json.dumps(str(obj))
            slots.setdefault(slot_name, []).append(value)
        expr = f"({frame_name} has"
        for slot, values in slots.items():
            expr += f" ({slot} ({' '.join(values)}))"
        expr += ")"
        return expr

    def property_to_km(self, prop_uri):
        """Generate a KM frame for an OWL ObjectProperty."""
        prop_name = self.get_resource_name(prop_uri)
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
        return expr

    @staticmethod
    def aggregate_to_km(element_type, number_of_elements=None):
        """Generate an Aggregate frame (Section 29.6)."""
        expr = "(a Aggregate with"
        expr += f" (element-type ({element_type}))"
        if number_of_elements:
            expr += f" (number-of-elements ({number_of_elements}))"
        expr += ")"
        return expr

    @staticmethod
    def quoted_expression(expr):
        """Generate a quoted expression (Section 29.6)."""
        return f"'{expr}"

    @staticmethod
    def forall_expression(var, collection, body, where=None):
        """Generate a forall expression (Section 29)."""
        expr = f"(forall {var} in {collection}"
        if where:
            expr += f" where {where}"
        expr += f" {body})"
        return expr

    @staticmethod
    def _join_expressions(expressions):
        """Join a list of expressions into a space-separated string."""
        return ' '.join(str(expr) for expr in expressions)

    def arithmetic_expression(self, operator, *operands):
        """Generate an arithmetic expression, e.g., (+ 1 2)."""
        return f"({operator} {self._join_expressions(operands)})"

    def logical_expression(self, operator, *operands):
        """Generate a logical expression, e.g., (and A B)."""
        return f"({operator} {self._join_expressions(operands)})"

    @staticmethod
    def unification_expression(type_, expr1, expr2):
        """Generate a unification expression based on type (set, eager, bag)."""
        if type_ == "set":
            return f"(({expr1}) && ({expr2}))"
        elif type_ == "eager":
            return f"({expr1} &! {expr2})"
        elif type_ == "bag":
            return f"(({expr1}) || ({expr2}))"
        else:
            raise ValueError(f"Unknown unification type: {type_}")

    @staticmethod
    def if_expression(condition, then_expr, else_expr=None):
        """Generate an if-then-else expression."""
        expr = f"(if {condition} then {then_expr}"
        if else_expr:
            expr += f" else {else_expr}"
        expr += ")"
        return expr

    @staticmethod
    def user_defined_infix(operator, left, right):
        """Generate a user-defined infix operator expression."""
        return f"({left} {operator} {right})"

    def oneof_expression(self, *options):
        """Generate a oneof expression."""
        return f"(oneof {self._join_expressions(options)})"

    def prototype_to_km(self, class_name, slots=None):
        """Generate a prototype expression."""
        expr = f"(a-prototype {class_name}"
        if slots:
            expr += f" with {self._join_expressions(slots)}"
        expr += ")"
        return expr

    def aggregation_function(self, func_name, *args):
        """Generate a user-defined aggregation function expression."""
        return f"({func_name} {self._join_expressions(args)})"

    def get_prerequisite_assertions(self, assertion):
        """Return a list of prerequisite assertions for the given KM assertion using pattern matching.

        Args:
            assertion (str): A KM code string, e.g., '(fido has (instance-of (Dog)) (color ("brown")))'.

        Returns:
            list[str]: A list of KM code strings representing the prerequisite assertions.
        """
        import re

        clean_assertion = re.sub(r'"[^"]*"', '', assertion)
        symbols = re.findall(r'[-\w]+', clean_assertion)
        subject = symbols[0] if symbols else None
        referenced_frames = set(
            sym for sym in symbols
            if sym != subject and sym not in BUILT_IN_FRAMES
        )

        name_to_uri = {name: uri for uri, name in self.resource_names.items()}
        prerequisites = []
        for frame_name in referenced_frames:
            if frame_name in name_to_uri:
                uri = name_to_uri[frame_name]

                if (uri, rdflib.RDF.type, rdflib.OWL.Class) in self.graph:
                    prerequisites.append(self.class_to_km(uri))
                elif (uri, rdflib.RDF.type, rdflib.OWL.ObjectProperty) in self.graph:
                    prerequisites.append(self.property_to_km(uri))
                else:
                    classes = list(self.graph.objects(uri, rdflib.RDF.type))
                    if classes:
                        class_uri = classes[0]
                        prerequisites.append(self.individual_to_km(class_uri))

        return prerequisites
