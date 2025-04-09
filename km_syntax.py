import rdflib
from utils import rdf_to_krl_name
import json

cyc_annot_label = rdflib.URIRef("http://some.namespace/cycAnnot:label")
STANDARD_PREDICATES = {
    rdflib.RDF.type: "instance-of",
    rdflib.RDFS.subClassOf: "superclasses",
    rdflib.RDFS.label: "label",
}


class KMSyntaxGenerator:
    def __init__(self, graph):
        self.graph = graph
        self.resource_names = self.build_resource_names()
        self.predicate_names = self.build_predicate_names()

    def build_resource_names(self):
        """Map resource URIs to preferred names."""
        names = {}
        for s in self.graph.subjects():
            labels = [str(o) for o in self.graph.objects(s, cyc_annot_label) if isinstance(o, rdflib.Literal)]
            if not labels:
                labels = [str(o) for o in self.graph.objects(s, rdflib.RDFS.label) if isinstance(o, rdflib.Literal)]
            if labels:
                preferred = next((l for l in labels if l[0].isupper()), labels[0])
                names[s] = preferred
            else:
                names[s] = rdf_to_krl_name(s)
        return names

    def build_predicate_names(self):
        """Map predicate URIs to slot names."""
        names = STANDARD_PREDICATES.copy()
        for pred in self.graph.predicates():
            if pred not in names:
                labels = [str(o) for o in self.graph.objects(pred, rdflib.RDFS.label) if isinstance(o, rdflib.Literal)]
                names[pred] = labels[0] if labels else rdf_to_krl_name(pred)
        return names

    def get_resource_name(self, resource):
        """Get the preferred name for a resource."""
        return self.resource_names.get(resource, rdf_to_krl_name(resource))

    def get_slot_name(self, predicate):
        """Get the slot name for a predicate."""
        return self.predicate_names.get(predicate, rdf_to_krl_name(predicate))

    def class_to_km(self, class_uri):
        """Convert an OWL class to KM syntax."""
        frame_name = self.get_resource_name(class_uri)
        rdf_id = str(class_uri)
        slots = {}

        for pred, obj in self.graph.predicate_objects(class_uri):
            slot_name = self.get_slot_name(pred)
            if isinstance(obj, rdflib.URIRef):
                value = self.get_resource_name(obj)
            else:
                value = json.dumps(str(obj))
            slots.setdefault(slot_name, []).append(value)

        expr = f"({frame_name} has (rdfId (\"{rdf_id}\"))"
        for slot, values in slots.items():
            expr += f" ({slot} ({' '.join(values)}))"
        expr += ")"
        return expr

    def individual_to_km(self, ind_uri, class_uri):
        """Generate KM frame for an individual (instance has ...)."""
        ind_name = rdf_to_krl_name(ind_uri)
        class_name = rdf_to_krl_name(class_uri)
        slots = []
        for prop, obj in self.graph.predicate_objects(ind_uri):
            prop_name = rdf_to_krl_name(prop)
            if isinstance(obj, rdflib.URIRef):
                obj_name = rdf_to_krl_name(obj)
                slots.append(f"({prop_name} ({obj_name}))")
            else:
                slots.append(f"({prop_name} ({json.dumps(str(obj))}))")

        expr = f"({ind_name} has (instance-of ({class_name}))"
        if slots:
            expr += f" {' '.join(slots)}"
        expr += ")"
        return expr

    def property_to_km(self, prop_uri):
        """Generate a KM frame for an OWL ObjectProperty as a Slot instance."""
        # Convert the property URI to a KM-compatible name
        prop_name = rdf_to_krl_name(prop_uri)

        # Extract standard OWL attributes
        labels = [str(label) for label in self.graph.objects(prop_uri, rdflib.RDFS.label)]
        comments = [str(comment) for comment in self.graph.objects(prop_uri, rdflib.RDFS.comment)]
        domains = [rdf_to_krl_name(d) for d in self.graph.objects(prop_uri, rdflib.RDFS.domain)]
        ranges = [rdf_to_krl_name(r) for r in self.graph.objects(prop_uri, rdflib.RDFS.range)]
        superslots = [rdf_to_krl_name(sp) for sp in self.graph.objects(prop_uri, rdflib.RDFS.subPropertyOf)]
        inverses = [rdf_to_krl_name(i) for i in self.graph.objects(prop_uri, rdflib.OWL.inverseOf)]

        # Check if the property is functional (at most one value per instance)
        is_functional = (prop_uri, rdflib.RDF.type, rdflib.OWL.FunctionalProperty) in self.graph
        cardinality = "1-to-1" if is_functional else None

        # Capture additional types beyond ObjectProperty and FunctionalProperty
        additional_types = [rdf_to_krl_name(t) for t in self.graph.objects(prop_uri, rdflib.RDF.type)
                            if t not in [rdflib.OWL.ObjectProperty, rdflib.OWL.FunctionalProperty]]

        # Handle custom annotations (non-standard predicates)
        custom_annotations = {}
        for pred, obj in self.graph.predicate_objects(prop_uri):
            if pred not in [rdflib.RDFS.label, rdflib.RDFS.comment, rdflib.RDFS.domain, rdflib.RDFS.range,
                            rdflib.RDFS.subPropertyOf, rdflib.OWL.inverseOf, rdflib.RDF.type, rdflib.OWL.sameAs]:
                pred_name = rdf_to_krl_name(pred)
                if isinstance(obj, rdflib.Literal):
                    custom_annotations.setdefault(pred_name, []).append(str(obj))
                else:
                    custom_annotations.setdefault(pred_name, []).append(rdf_to_krl_name(obj))

        # Capture equivalent properties (owl:sameAs)
        same_as = [rdf_to_krl_name(sa) for sa in self.graph.objects(prop_uri, rdflib.OWL.sameAs)]

        # Build the KM expression
        expr = f"({prop_name} has (instance-of (Slot))"
        if labels:
            expr += f" (label ({' '.join([json.dumps(label) for label in labels])}))"
        if comments:
            expr += f" (comment ({' '.join([json.dumps(comment) for comment in comments])}))"
        if domains:
            expr += f" (domain ({' '.join(domains)}))"
        if ranges:
            expr += f" (range ({' '.join(ranges)}))"
        if superslots:
            expr += f" (superslots ({' '.join(superslots)}))"
        if inverses:
            expr += f" (inverse ({' '.join(inverses)}))"
        if cardinality:
            expr += f" (cardinality ({cardinality}))"
        if additional_types:
            expr += f" (additional_types ({' '.join(additional_types)}))"
        for annot, values in custom_annotations.items():
            expr += f" ({annot} ({' '.join([json.dumps(val) if isinstance(val, str) else val for val in values])}))"
        if same_as:
            expr += f" (same_as ({' '.join(same_as)}))"
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

    @staticmethod
    def user_defined_infix(operator, left, right):
        """Generate a user-defined infix operator expression."""
        return f"({left} {operator} {right})"

    def aggregation_function(self, func_name, *args):
        """Generate a user-defined aggregation function expression."""
        return f"({func_name} {self._join_expressions(args)})"
