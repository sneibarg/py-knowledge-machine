import rdflib
from utils import rdf_to_krl_name
import json


class KMSyntaxGenerator:
    def __init__(self, graph):
        """Initialize the generator with an RDF graph."""
        self.graph = graph

    # Helper Methods
    def _format_slot(self, slot_name, value):
        """Format a slot-value pair."""
        return f"({slot_name} {value})"

    def _join_expressions(self, expressions, separator=" "):
        """Join multiple expressions with a separator."""
        return separator.join(str(expr) for expr in expressions)

    # RDF to KM Methods
    def class_to_km(self, class_uri):
        class_name = rdf_to_krl_name(class_uri)
        superclasses = [rdf_to_krl_name(sup) for sup in self.graph.objects(class_uri, rdflib.RDFS.subClassOf)
                        if sup != rdflib.OWL.Thing]
        labels = [str(label) for label in self.graph.objects(class_uri, rdflib.RDFS.label)]
        comments = [str(comment) for comment in self.graph.objects(class_uri, rdflib.RDFS.comment)]
        custom_comment_pred = rdflib.URIRef("http://some.namespace/Mx4rwLSVCpwpEbGdrcN5Y29ycA")
        custom_comments = [str(cc) for cc in self.graph.objects(class_uri, custom_comment_pred)]
        cyc_annot_pred = rdflib.URIRef("http://some.namespace/cycAnnot:label")
        cyc_annot_labels = [str(label) for label in self.graph.objects(class_uri, cyc_annot_pred)]
        same_as = [rdf_to_krl_name(sa) for sa in self.graph.objects(class_uri, rdflib.OWL.sameAs)]
        additional_types = [rdf_to_krl_name(t) for t in self.graph.objects(class_uri, rdflib.RDF.type)
                            if t not in [rdflib.OWL.Class, rdflib.RDFS.Class]]

        slots = []
        for prop in self.graph.subjects(rdflib.RDFS.domain, class_uri):
            prop_name = rdf_to_krl_name(prop)
            ranges = [rdf_to_krl_name(r) for r in self.graph.objects(prop, rdflib.RDFS.range)]
            range_str = ranges[0] if ranges else "Thing"
            slots.append(f"({prop_name} ((must-be-a {range_str})))")

        expr = f"(every {class_name} has"
        if superclasses:
            expr += f" (superclasses ({' '.join(superclasses)}))"
        if labels:
            expr += f" (label ({' '.join([json.dumps(label) for label in labels])}))"
        if comments:
            expr += f" (comment ({' '.join([json.dumps(comment) for comment in comments])}))"
        if custom_comments:
            expr += f" (additional_comments ({' '.join([json.dumps(cc) for cc in custom_comments])}))"
        if cyc_annot_labels:
            expr += f" (cyc_annot_label ({' '.join([json.dumps(label) for label in cyc_annot_labels])}))"
        if same_as:
            expr += f" (same_as ({' '.join(same_as)}))"
        if additional_types:
            expr += f" (additional_types ({' '.join(additional_types)}))"
        if slots:
            expr += f" {' '.join(slots)}"
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

    # General KM Syntax Methods
    def aggregate_to_km(self, element_type, number_of_elements=None):
        """Generate an Aggregate frame (Section 29.6)."""
        expr = "(a Aggregate with"
        expr += f" (element-type ({element_type}))"
        if number_of_elements:
            expr += f" (number-of-elements ({number_of_elements}))"
        expr += ")"
        return expr

    def quoted_expression(self, expr):
        """Generate a quoted expression (Section 29.6)."""
        return f"'{expr}"

    def forall_expression(self, var, collection, body, where=None):
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

    def unification_expression(self, type_, expr1, expr2):
        """Generate a unification expression based on type (set, eager, bag)."""
        if type_ == "set":
            return f"(({expr1}) && ({expr2}))"
        elif type_ == "eager":
            return f"({expr1} &! {expr2})"
        elif type_ == "bag":
            return f"(({expr1}) || ({expr2}))"
        else:
            raise ValueError(f"Unknown unification type: {type_}")

    def if_expression(self, condition, then_expr, else_expr=None):
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

    def user_defined_infix(self, operator, left, right):
        """Generate a user-defined infix operator expression."""
        return f"({left} {operator} {right})"

    def aggregation_function(self, func_name, *args):
        """Generate a user-defined aggregation function expression."""
        return f"({func_name} {self._join_expressions(args)})"

    def rdf_to_krl_name(uri):
        # Simplified: Replace with actual URI-to-KM-name conversion logic
        return uri.split('/')[-1].replace(':', '_')

