# Assuming a km module or package exists to organize the code
# Import necessary utilities (assumed to be defined elsewhere in the KM system)
from re import search

from km_utils import (
    am_in_prototype_mode, single_valued_slotp, combine_values_by_appending_slotp,
    unify_in_prototypes, protoinstancep, dereference, own_rule_sets,
    find_constraints_in_exprs, am_in_local_situation, fluentp,
    inherited_rule_sets, reify_existentials_in_rule_sets, append_lists,
    inherit_with_overrides_slotp, immediate_subslots, km_int, vals_to_val,
    val_sets_to_expr, enforce_set_constraints, put_vals, get_vals,
    record_explanation_for, make_comment, km_trace, tracep, traceunifyp,
    traceconstraintsp, prev_situation, curr_situation, projectable,
    km_slotvals_via_projection, filter_using_constraints, dont_cache_values_slotp,
    remove_duplicates, remove_subsumers, remove_subsumees, note_done, un_done,
    check_slot, target_situation, bind_self, simple_inherit_with_overrides_slotp,
    satisfies_constraints, recursive_ruleset, lazy_unify, inertial_fluentp,
    reify_existentials_in_rule_set, reify_existentials_in_expr
)
from setuptools.namespaces import flatten

# Global variables (assumed to be defined elsewhere, e.g., in a header module)
use_inheritance_flag = True
use_prototypes_flag = True
are_some_prototypes = False
slots_not_to_clone_for = []
use_no_inheritance_flag = False
record_explanations = False
max_padding_instances = 0
project_cached_values_only = False
global_situation = '*global-situation*'


# Control use of inheritance and prototypes
def use_inheritance():
    """Check if inheritance should be used based on global flag and mode."""
    return use_inheritance_flag and not am_in_prototype_mode()


def use_prototypes():
    """Check if prototypes should be used based on global flag and mode."""
    return use_prototypes_flag and not am_in_prototype_mode()


# Main function to retrieve slot values from the knowledge base
def km_slotvals_from_kb(instance0, slot, fail_mode=None):
    """
    Retrieve slot values for a given instance and slot from the knowledge base.

    Args:
        instance0: The instance to query.
        slot: The slot to retrieve values for.
        fail_mode: Optional parameter (currently ignored).

    Returns:
        List of slot values.
    """
    n = 0  # For tracing purposes

    # Preliminaries
    single_valuedp = single_valued_slotp(slot)
    multivaluedp = not single_valuedp
    combine_values_by_appendingp = combine_values_by_appending_slotp(slot)

    # 0 1/2. Merge in relevant prototypes
    if (are_some_prototypes and
            slot not in slots_not_to_clone_for and
            use_prototypes() and
            not protoinstancep(instance0)):
        unify_in_prototypes(instance0, slot)

    # 0 3/4. Collect all the rule data needed
    instance = dereference(instance0)
    if protoinstancep(instance) and not am_in_prototype_mode():
        raise ValueError(
            f"Attempt to query a protoinstance {instance} when not in prototype mode! Doing (the {slot} of {instance})")

    target = ['the', slot, 'of', instance]
    own_rule_sets_list = own_rule_sets(instance, slot, retain_commentsp=True)
    own_constraints = [constraint for expr in own_rule_sets_list for constraint in find_constraints_in_exprs(expr)]

    if use_inheritance():
        if (not own_rule_sets_list and
                am_in_local_situation() and
                not fluentp(slot)):
            global_inherited_rule_sets = inherited_rule_sets(instance, slot, retain_commentsp=True)
            local_inherited_rule_sets = inherited_rule_sets(instance, slot, retain_commentsp=True,
                                                            climb_situation_hierarchyp=False)
            inherited_rule_sets_x = local_inherited_rule_sets + reify_existentials_in_rule_sets(
                global_inherited_rule_sets)
        else:
            inherited_rule_sets_x = inherited_rule_sets(instance, slot, retain_commentsp=True)
    else:
        inherited_rule_sets_x = []

    if combine_values_by_appendingp:
        xx = append_lists(inherited_rule_sets_x)
        inherited_rule_sets = [xx] if xx else []
    else:
        inherited_rule_sets = inherited_rule_sets_x

    if use_inheritance() and not inherit_with_overrides_slotp(slot):
        inherited_rule_sets_all = inherited_rule_sets
    else:
        inherited_rule_sets_all = inherited_rule_sets(instance, slot, retain_commentsp=True,
                                                      ignore_inherit_with_overrides_restriction=True)

    inherited_constraints = [constraint for expr in inherited_rule_sets_all for constraint in
                             find_constraints_in_exprs(expr)]
    constraints = inherited_constraints + own_constraints
    no_inheritancep = use_no_inheritance_flag and any(constraint == ['no-inheritance'] for constraint in constraints)

    # 1. Projection
    try_projectionp = (am_in_local_situation() and
                       projectable(slot, instance) and
                       prev_situation(curr_situation(), instance))
    if try_projectionp:
        if tracep():
            n += 1
            km_trace('comment', f"({n}) Look in previous situation")
        projected_vals0 = km_slotvals_via_projection(instance, slot)
    else:
        projected_vals0 = None

    if constraints and projected_vals0:
        if tracep() and not traceunifyp():
            # Suppress tracing temporarily (simulating Lisp's suspend-trace)
            with tracing_disabled():
                projected_vals = filter_using_constraints(projected_vals0, constraints, slot)
        else:
            km_trace('comment', f"({n}b) Test projected values {projected_vals0} against constraints {constraints}")
            projected_vals = filter_using_constraints(projected_vals0, constraints, slot)
    else:
        projected_vals = projected_vals0

    if tracep() and try_projectionp and projected_vals0 != projected_vals:
        km_trace('comment',
                 f"    Discarding projected values {set_difference(projected_vals0, projected_vals)} (conflicts with constraints {constraints})")

    if projected_vals and multivaluedp:
        prev_sit = prev_situation(curr_situation(), instance)
        for val in projected_vals:
            record_explanation_for(target, val, ['projected-from', prev_sit])
        make_comment(f"Projected (the {slot} of {instance}) = {projected_vals} from {prev_sit} to {curr_situation()}")

    # 2. Subslots
    subslots = immediate_subslots(slot)
    if subslots:
        if no_inheritancep:
            km_trace('comment', "(Ignore subslots, as there is a `(no-inheritance)' constraint on this slot)")
        else:
            if tracep():
                n += 1
                km_trace('comment', f"({n}) Look in subslot(s)")
            subslot_vals = km_int(vals_to_val([
                ['the', subslot, 'of', instance0, ['comm', '*SUBSLOT-COMMENT-TAG*', 'Self', subslot]]
                for subslot in subslots
            ]), target=target)
    else:
        subslot_vals = None

    # 3. Supersituations (disabled in original code)
    supersituation_vals = None

    # 4. Local values
    if own_rule_sets_list:
        if tracep():
            n += 1
            km_trace('comment',
                     f"({n}) Local value(s): {val_sets_to_expr(own_rule_sets_list, single_valuedp=single_valuedp)}")
        if (len(own_rule_sets_list) == 1 and
                len(own_rule_sets_list[0]) == 1 and
                isinstance(own_rule_sets_list[0][0], (str, int)) and
                own_rule_sets_list[0][0] != ':incomplete' and
                dereference(own_rule_sets_list[0][0]) == own_rule_sets_list[0][0]):
            local_vals = own_rule_sets_list[0]
        else:
            local_vals = km_int(
                val_sets_to_expr(own_rule_sets_list, combine_values_by_appendingp=combine_values_by_appendingp,
                                 single_valuedp=single_valuedp), target=target)
    else:
        local_vals = None

    local_situation = target_situation(curr_situation(), instance, slot)
    local_constraints = find_constraints_in_exprs(
        bind_self(get_vals(instance, slot, situation=local_situation), instance))

    # Intermediate combine and save of vals (but not rules)
    n_first_source = 2 if try_projectionp and single_valuedp else 1
    n_sources = n  # Simplified from original Lisp
    val_sets = remove_duplicates([val for val in [
        projected_vals if multivaluedp else None,
        subslot_vals,
        supersituation_vals,
        local_vals
    ] if val is not None], key=str)

    if not val_sets:
        vals = None
    else:
        singletonp_constraints = [c for c in constraints if
                                  isinstance(c, list) and c[0] in ['at-most', 'exactly'] and c[1] == 1]
        if len(val_sets) == 1:
            if not dont_cache_values_slotp(slot):
                vals0 = enforce_set_constraints([v for v in val_sets[0] if v != ':incomplete'], singletonp_constraints,
                                                target=target)
                put_vals(instance, slot, vals0)
                vals = vals0
            else:
                vals = val_sets[0]
        else:
            if n_first_source != n_sources:
                km_trace('comment', f"({n_first_source}-{n_sources}) CombineX {n_first_source}-{n_sources} together")
            vals0 = enforce_set_constraints(
                km_int(val_sets_to_expr(val_sets, combine_values_by_appendingp=combine_values_by_appendingp,
                                        single_valuedp=single_valuedp), target=target),
                singletonp_constraints, target=target
            )
            if not dont_cache_values_slotp(slot):
                put_vals(instance, slot, vals0)
            vals = vals0

    # Fold in rules
    if are_some_defaults:
        inherited_rule_sets00 = [
            evaluate_and_filter_defaults(expr_set, constraints, vals, slot, single_valuedp=single_valuedp)
            for expr_set in inherited_rule_sets
        ]
        default_own_rules = [find_exprs(own_rules, expr_type='default', plurality='plural') for own_rules in
                             own_rule_sets_list if own_rules]
        inherited_rule_sets00 = default_own_rules + inherited_rule_sets00
    else:
        inherited_rule_sets00 = inherited_rule_sets

    if not use_inheritance():
        km_trace('comment', "(No inherited rules (Inheritance is turned off))")
        all_vals00 = vals
    elif inherited_rule_sets00:
        if no_inheritancep:
            km_trace('comment', "(Ignore inherited rules, as there is a `(no-inheritance)' constraint on this slot)")
            all_vals00 = vals
        elif vals and simple_inherit_with_overrides_slotp(slot):
            km_trace('comment',
                     "(Ignore rules, as there are local values and the slot is a simple-inherit-with-overrides slot)")
            all_vals00 = vals
        else:
            if tracep():
                n += 1
                if inherit_with_overrides_slotp(slot):
                    km_trace('comment',
                             f"({n}) Lowest rules, from inheritance with over-rides: {val_sets_to_expr(inherited_rule_sets00, single_valuedp=single_valuedp)}")
                else:
                    km_trace('comment',
                             f"({n}) From inheritance: {val_sets_to_expr(inherited_rule_sets00, single_valuedp=single_valuedp)}")
            if vals:
                km_trace('comment', f"({n_first_source}-{n}) CombineY {n_first_source}-{n} together")
            if vals and inherit_with_overrides_slotp(slot):
                if single_valuedp:
                    loc_vals = km_int(vals_to_ & _expr(vals), target=target)
                    km_trace('comment', "See if inherited info is consistent with local vals...")
                    if km_int([loc_vals, '&?', val_sets_to_expr(inherited_rule_sets00, single_valuedp=True)]):
                        km_trace('comment', "...yes! Inherited info is consistent with local vals. Unifying it in...")
                        all_vals00 = km_int(
                            [loc_vals, '&', val_sets_to_expr(inherited_rule_sets00, single_valuedp=True)],
                            target=target)
                    else:
                        km_trace('comment',
                                 "...no, inherited info isn't consistent with local info, so dropping inherited info.")
                        all_vals00 = loc_vals
                else:
                    km_trace('comment', "See if inherited info is consistent with local vals...")
                    loc_vals = km_int(val_sets_to_expr([vals]), target=target)
                    locgen_vals = km_int(val_sets_to_expr([loc_vals] + inherited_rule_sets00), target=target)
                    if satisfies_constraints(locgen_vals, constraints, slot):
                        km_trace('comment', "...yes! Inherited info is consistent with local vals. Unifying it in...")
                        all_vals00 = locgen_vals
                    else:
                        km_trace('comment',
                                 "...no, inherited info isn't consistent with local info, so dropping inherited info.")
                        all_vals00 = loc_vals
            else:
                all_vals00 = km_int(val_sets_to_expr([vals] + inherited_rule_sets00, single_valuedp=single_valuedp),
                                    target=target)
    else:
        all_vals00 = vals

    # If rules are recursive, reiterate once more
    if all_vals00 and inherited_rule_sets00 and use_inheritance() and not no_inheritancep and not dont_cache_values_slotp(
            slot):
        recursive_rulesets = [ruleset for ruleset in inherited_rule_sets00 if
                              recursive_ruleset(instance, slot, ruleset)]
        if recursive_rulesets:
            km_trace('comment',
                     f"Recursive ruleset(s) {recursive_rulesets} encountered\n...retrying them now some other values have been computed!")
            put_vals(instance, slot, all_vals00)
            all_vals0 = km_int(val_sets_to_expr([all_vals00] + inherited_rule_sets00, single_valuedp=single_valuedp),
                               target=target)
        else:
            all_vals0 = all_vals00
    else:
        all_vals0 = all_vals00

    # Conditional projection of single-valued slot's value
    if multivaluedp:
        all_vals1 = all_vals0
    else:
        projected_val = maybe_project_value(projected_vals, all_vals0, slot, instance, n_sources)
        if projected_val:
            record_explanation_for(target, projected_val,
                                   ['projected-from', prev_situation(curr_situation(), instance)])
            all_vals1 = [projected_val]
        else:
            all_vals1 = all_vals0

    # Enforce constraints
    if constraints and (all_vals1 or max_padding_instances > 0):
        if tracep() and not traceconstraintsp():
            with tracing_disabled():
                all_vals2 = enforce_constraints(all_vals1, constraints, target=target)
        else:
            km_trace('comment', f"({n}b) Test values against constraints {constraints}")
            all_vals2 = enforce_constraints(all_vals1, constraints, target=target)
    else:
        all_vals2 = all_vals1

    if remove_subsumers_slotp(slot):
        all_vals = remove_subsumers(all_vals2)
    elif remove_subsumees_slotp(slot):
        all_vals = remove_subsumees(all_vals2)
    else:
        all_vals = all_vals2

    if local_constraints:
        if single_valuedp:
            all_vals_and_constraints = val_to_vals(vals_to_ & _expr(all_vals + local_constraints))
        else:
            all_vals_and_constraints = all_vals + local_constraints
    else:
        all_vals_and_constraints = all_vals

    if not dont_cache_values_slotp(slot):
        put_vals(instance, slot, all_vals_and_constraints)
        if record_explanations:
            for local_constraint in local_constraints:
                val = desource_decomment(local_constraint)
                if val != local_constraint:
                    record_explanation_for(target, val, local_constraint)

    check_slot(instance, slot, all_vals)

    target_sit = target_situation(curr_situation(), instance, slot, all_vals)
    if target_sit != global_situation and all_vals_and_constraints != get_vals(instance, slot, situation=target_sit):
        un_done(instance, slot=slot, situation=curr_situation())

    if not dont_cache_values_slotp(slot):
        note_done(instance, slot)

    return all_vals


# Temporal projection code
def km_slotvals_via_projection(instance, slot):
    """Retrieve slot values via projection from the previous situation."""
    prev_sit = prev_situation(curr_situation(),
                              instance) if not project_cached_values_only else prev_situation_with_vals(
        curr_situation(), instance, slot)
    if prev_sit:
        return km_int(['in-situation', prev_sit, ['the', slot, 'of', instance]])
    elif tracep():
        km_trace('comment', f"    (Can't compute what {curr_situation()}'s previous situation is)")
    return None


def maybe_project_value(projected_values, local_values, slot, instance, n_sources):
    """Project a single-valued slot's value if it unifies with local values."""
    if not projected_values:
        return None
    if projected_values == local_values:
        return projected_values[0]

    prev_sit = prev_situation(curr_situation(), instance)
    projected_value = projected_values[0]
    local_value = local_values[0] if local_values else None

    if len(projected_values) >= 2:
        print(
            f"ERROR! Projected multiple values {projected_values} for the single-valued slot '{slot}' on instance {instance}!")
        print(f"ERROR! Discarding all but the first value ({projected_value})...")
    if local_values and len(local_values) >= 2:
        print(
            f"ERROR! Found multiple values {local_values} for the single-valued slot '{slot}' on instance {instance}!")
        print(f"ERROR! Discarding all but the first value ({local_value})...")

    if not local_value:
        km_trace('comment',
                 f"(1-{n_sources}) Projecting (the {slot} of {instance}) = ({projected_value}) from {prev_sit}")
        make_comment(
            f"Projected (the {slot} of {instance}) = ({projected_value}) from {prev_sit} to {curr_situation()}")
        return projected_value
    else:
        unified = lazy_unify(projected_value, local_value)
        if unified:
            km_trace('comment',
                     f"(1-{n_sources}) Projecting and unifying (the {slot} of {instance}) = ({projected_value}) from {prev_sit}")
            make_comment(
                f"Projected (the {slot} of {instance}) = ({projected_value}) from {prev_sit} to {curr_situation()}")
            return unified
        else:
            km_trace('comment',
                     f"(1-{n_sources}) Discarding projected value (the {slot} of {instance}) = ({projected_value}) (conflicts with new value ({local_value}))")
            return None


def projectable(slot, instance):
    """Check if a slot is projectable."""
    return inertial_fluentp(slot)


# Helper functions (simplified versions, assuming full implementations elsewhere)
def reify_existentials_in_rule_sets(rule_sets):
    """Reify existentials in a list of rule sets."""
    return [reify_existentials_in_rule_set(rs) for rs in rule_sets]


def recursive_ruleset(instance, slot, ruleset):
    """Check if a ruleset is recursive (simplified implementation)."""
    return search(['the', slot, 'of', instance], flatten(ruleset))


# Utility to disable tracing temporarily (simulating Lisp behavior)
class tracing_disabled:
    def __enter__(self):
        global trace_enabled
        self.old_trace = trace_enabled
        trace_enabled = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        global trace_enabled
        trace_enabled = self.old_trace


# Placeholder for functions not provided in the Lisp code
def set_difference(list1, list2):
    """Return elements in list1 not in list2."""
    return [x for x in list1 if x not in list2]


def desource_decomment(expr):
    """Remove source and comment info from an expression (placeholder)."""
    return expr  # Simplified; actual implementation depends on KM system


trace_enabled = True  # Global flag to simulate tracep behavior