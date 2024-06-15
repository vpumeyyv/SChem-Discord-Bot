#!/usr/bin/env python
# -*- coding: utf-8 -*-

"Contains functions for parsing tournament metric equations."

import ast
import inspect
import math
import operator as op

from schem.waldo import InstructionType
from schem.components import Reactor, Recycler

# Operators allowed in puzzle metric strings
METRIC_OPS = {ast.Pow: op.pow, ast.USub: op.neg, ast.Mult: op.mul, ast.Div: op.truediv, ast.Add: op.add, ast.Sub: op.sub,
              # Built-in functions must be wrapped since otherwise they don't provide arg-count inspection info
              'log': lambda x: math.log(x, 10), 'max': lambda *x: max(*x), 'min': lambda *x: min(*x),
              'floor': lambda x: math.floor(x), 'ceil': lambda x: math.ceil(x)}
# Metric vars that require a custom runtime handler to collect.
# We use a dict instead of a set because we want this to have a fixed order when displayed in tables.
RUNTIME_METRIC_VARS = {'arrow_hits': None, 'rotate_hits': None, 'sync_hits': None,
                       'bond_plus_hits': None, 'bond_minus_hits': None, 'fuse_hits': None, 'split_hits': None, 'swap_hits': None,
                       'piped_molecules': None}
# Functions for calculating values in a metric equation, given a Solution object
METRIC_VAR_TO_FN = {'cycles': lambda soln: soln.expected_score.cycles,
                    'reactors': lambda soln: soln.expected_score.reactors,
                    'symbols': lambda soln: soln.expected_score.symbols,
                    'waldos': lambda soln: waldos(soln),
                    'waldopath': lambda soln: waldopath(soln),
                    'bonders': lambda soln: used_bonders(soln),
                    # Instruction counts
                    'arrows': lambda soln: num_arrows(soln),
                    # Note the var captures in these dynamically-generated lambdas, to avoid evil lambda scope bugs.
                    **{i.name.lower() + ('s' if not i.name.endswith('S') else 'es'):  # 'bond_pluses'
                       lambda soln, instr=i: num_instrs_of_type(soln, instr)  # Default lambda arg to avoid scope issues
                       for i in InstructionType
                       # Exclude the weird ones and inputs/outputs since they should be reserved for molecules
                       if i not in (InstructionType.START, InstructionType.INPUT, InstructionType.OUTPUT,
                                    InstructionType.PAUSE)},
                    # _instrs suffix for these to avoid sounding like 'number of input molecules'
                    'input_instrs': lambda soln: num_instrs_of_type(soln, InstructionType.INPUT),
                    'output_instrs': lambda soln: num_instrs_of_type(soln, InstructionType.OUTPUT),
                    # Puzzle requirements continue to get weirder with every year...
                    'alpha_input_instrs': lambda soln: num_instrs_of_type(soln, InstructionType.INPUT, target_idx=0),
                    'beta_input_instrs': lambda soln: num_instrs_of_type(soln, InstructionType.INPUT, target_idx=1),
                    'psi_output_instrs': lambda soln: num_instrs_of_type(soln, InstructionType.OUTPUT, target_idx=0),
                    'omega_output_instrs': lambda soln: num_instrs_of_type(soln, InstructionType.OUTPUT, target_idx=1),
                    'bonds': lambda soln: num_instrs_of_type(soln, InstructionType.BOND_PLUS)
                                          + num_instrs_of_type(soln, InstructionType.BOND_MINUS),
                    'pipe_segments': lambda soln: pipe_segments(soln),
                    'recycler_pipes': lambda soln: recycler_pipes(soln),
                    'max_symbols': lambda soln: max_symbols(soln),
                    'max_waldo_symbols': lambda soln: max_waldo_symbols(soln),
                    'symbol_footprint': lambda soln: symbol_footprint(soln),
                    'max_symbol_footprint': lambda soln: max_symbol_footprint(soln),
                    # Stupid hack for manual host scoring
                    'name': lambda soln: name_is_score(soln),
                    # Runtime metrics (requires passing cycle_handler_collect_instr_hit_counts to run())
                    **{k: lambda soln, m=k: soln.custom_data[m] for k in RUNTIME_METRIC_VARS}}
                    # TODO: 'outputs': completed_outputs
                    #       requires modifications to tournament validator to accept solutions without an expected
                    #       score if the metric contains 'outputs', and to eval the metric even if the solution crashes

METAMETRIC_VARS = {'your_metric', 'best_metric', 'your_rank_idx', 'num_solvers'}


def ast_vars(node):
    """Return a set of all variables in the given AST."""
    if isinstance(node, ast.Name):
        return {node.id}
    elif isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return set()
    elif isinstance(node, ast.BinOp):
        return ast_vars(node.left) | ast_vars(node.right)
    elif isinstance(node, ast.UnaryOp):
        return ast_vars(node.operand)
    elif isinstance(node, ast.Call):
        return set().union(*(ast_vars(arg) for arg in node.args))
    else:
        raise TypeError(f"ast_vars: Arg {node} is not an AST type.")


def ast_operators(node):
    """Return a set of all operators and calls in the given AST, or return an error if any are invalid."""
    if isinstance(node, (ast.Name, ast.Constant)):
        return set()
    elif isinstance(node, ast.BinOp):
        return {type(node.op)} | ast_operators(node.left) | ast_operators(node.right)
    elif isinstance(node, ast.UnaryOp):
        return {type(node.op)} | ast_operators(node.operand)
    elif isinstance(node, ast.Call):
        if node.func.id not in METRIC_OPS:
            raise ValueError(f"Unknown fn `{node.func.id}` in metric equation.")

        # Make sure the number of args matches the fn signature
        fn_argspec = inspect.getfullargspec(METRIC_OPS[node.func.id])
        if (not node.args or
                (fn_argspec.varargs is None and fn_argspec.varkw is None
                 and len(node.args) != len(fn_argspec.args))):
            raise ValueError(f"Unexpected number of args to {node.func.id}")

        return {node.func.id}.union(*(ast_operators(arg) for arg in node.args))
    else:
        raise TypeError(f"ast_operators: Arg {node} is not an AST type.")


def validate_metric(metric_str):
    """Raise an error if the given metric string is unparsable."""
    # Handle vars/fns case-insensitively and allow specifying powers as either ^ or **
    metric_str = metric_str.lower().replace('^', '**')

    # Parse the string as AST
    try:
        metric_ast = ast.parse(metric_str, mode='eval').body
    except SyntaxError as e:
        raise ValueError(f"In metric: {e}") from e  # Raise a more descriptive error

    for oper in ast_operators(metric_ast):
        if oper not in METRIC_OPS:
            raise ValueError(f"Unknown operator `{oper}` in metric equation.")

    for v in ast_vars(metric_ast):
        if v not in METRIC_VAR_TO_FN:
            raise ValueError(f"Unknown var `{v}` in metric equation. Allowed vars: {METRIC_VAR_TO_FN.keys()}")


def validate_metametric(metametric_str):
    """Raise an error if the given metametric string is unparsable."""
    # Handle vars/fns case-insensitively and allow specifying powers as either ^ or **
    metametric_str = metametric_str.lower().replace('^', '**')

    # Parse the string as AST
    try:
        metametric_ast = ast.parse(metametric_str, mode='eval').body
    except SyntaxError as e:
        raise ValueError(f"In metametric: {e}")  # Raise a more descriptive error

    for x in ast_operators(metametric_ast):
        if x not in METRIC_OPS:
            raise ValueError(f"Unknown operator `{x}` in metric equation.")

    for v in ast_vars(metametric_ast):
        if v not in METAMETRIC_VARS:
            raise ValueError(f"Unknown var `{v}` in metric equation.")


def eval_ast(node, vars_dict):
    """Helper for evaluating a puzzle metric (safer than built-in eval)"""
    if isinstance(node, ast.Name):
        if node.id not in vars_dict:
            raise Exception(f"Unknown metric var `{node.id}`")
        return vars_dict[node.id]
    elif isinstance(node, ast.Constant):
        return node.value
    elif isinstance(node, ast.BinOp):
        return METRIC_OPS[type(node.op)](eval_ast(node.left, vars_dict), eval_ast(node.right, vars_dict))
    elif isinstance(node, ast.UnaryOp):
        return METRIC_OPS[type(node.op)](eval_ast(node.operand, vars_dict))
    elif isinstance(node, ast.Call):
        return METRIC_OPS[node.func.id](*(eval_ast(arg, vars_dict) for arg in node.args))
    else:
        raise TypeError(f"eval_ast: Arg {node} is not an AST type.")


def get_metric_and_terms(soln, metric_str):
    """Score the (assumed to be already-validated) given solution using the given metric expression. Return the score
    along with a dict of the value for each term in the metric.
    Respects python's usual order of operations (i.e. BEDMAS).
    Valid ops: +, -, *, /, ** or ^
    Valid terms: any real number, or any of:
        cycles, reactors, symbols: Per usual.
        waldos: Number of non-empty waldos in the solution.
        waldopath: Number of reactor cells crossed by a waldopath
    """
    # Handle vars/fns case-insensitively and allow specifying powers as either ^ or **
    metric_str = metric_str.lower().replace('^', '**')

    # Parse the metric into an AST
    ast_tree = ast.parse(metric_str, mode='eval').body

    # Calculate all variables the metric needs. Sorted in same order as they appear in METRIC_VAR_TO_FN
    # (this is the order they'll appear as column in results announcements)
    used_vars = ast_vars(ast_tree)
    vars_dict = {var: fn(soln) for var, fn in METRIC_VAR_TO_FN.items() if var in used_vars}

    return eval_ast(ast_tree, vars_dict), vars_dict


def eval_metric(soln, metric_str):
    """Score the (assumed to be already-validated) given solution using the given metric expression. Return the score.
    Respects python's usual order of operations (i.e. BEDMAS).
    Valid ops: +, -, *, /, ** or ^
    Valid terms: any real number, or any of:
        cycles, reactors, symbols: Per usual.
        waldos: Number of non-empty waldos in the solution.
        waldopath: Number of reactor cells crossed by a waldopath
        <instr_name>s: E.g. `arrows`, `syncs`, `bond_pluses`. Number of the given instruction.
        <instr_name>_hits: E.g. `swap_hits`. Number of times the solution hits this instruction while run.
                           Note that the *_hits metrics can only be measured at runtime, so their use has performance
                           implications (e.g. loop fast-forwarding will be disabled).
                           input_hits and output_hits are currently unavailable.
    """
    return get_metric_and_terms(soln, metric_str)[0]


def eval_metametric(metametric_str, metametric_vars):
    """Given a metametric equation and a dict of metric/placement vars for a submission, return its metametric score.
    Respects python's usual order of operations (i.e. BEDMAS).
    Valid ops: +, -, *, /, ** or ^
    Required terms in metametric_vars: your_metric, best_metric, your_rank_idx (0-indexed), num_solvers
    """
    # Handle vars/fns case-insensitively and allow specifying powers as either ^ or **
    metametric_str = metametric_str.lower().replace('^', '**')

    ast_tree = ast.parse(metametric_str, mode='eval').body

    return eval_ast(ast_tree, metametric_vars)


def get_metametric_term_values(metametric_str, metametric_vars):
    """Given a metametric equation and a dict of metric/rank vars for a submission, separate and evaluate its
    relative metric and rank bonus terms, stripped of any constant factors. Return their evaluated values or None
    if the metametric does not include that term.
    E.g. `(4 * rel_metric + rank) / 5` => rel_metric, rank
    """
    # Handle vars/fns case-insensitively and allow specifying powers as either ^ or **
    metametric_str = metametric_str.lower().replace('^', '**')

    ast_tree = ast.parse(metametric_str, mode='eval').body
    rel_metric_ast = ast_tree if 'your_metric' in metametric_str else None
    rank_ast = ast_tree if 'your_rank_idx' in metametric_str else None

    while rel_metric_ast is rank_ast is not None:
        # Strip all constant terms until we've separated metric from placement
        non_const_terms = [node for node in ast.iter_child_nodes(rel_metric_ast) if not isinstance(node, ast.Constant)
                           and node is not ast_tree.op]  # iter_child_node includes its operator for some reason...
        assert non_const_terms, f"Internal error while separating terms of {metametric_str}: no variable terms"

        if len(non_const_terms) == 1:
            rel_metric_ast = rank_ast = non_const_terms[0]
            continue

        for node in non_const_terms:
            term_str = ast.unparse(node)
            rel_metric_ast = node if 'your_metric' in term_str else rel_metric_ast
            rank_ast = node if 'your_rank_idx' in term_str else rank_ast

    # Once we've separated rel_metric and rank_bonus (or found one to be missing), do a final strip of any constant
    # factors on each (but leave other ops alone so e.g. (1 - rel_rank) or max(0, 10 - rel_rank) go untouched)
    # TODO: Refactor to merge these
    while isinstance(rel_metric_ast, ast.BinOp) and isinstance(rel_metric_ast.op, (ast.Mult, ast.Div)):
        if isinstance(rel_metric_ast.right, ast.Constant):
            rel_metric_ast = rel_metric_ast.left
        elif isinstance(rel_metric_ast.left, ast.Constant):
            rel_metric_ast = rel_metric_ast.right
        else:
            break

    while isinstance(rank_ast, ast.BinOp) and isinstance(rank_ast.op, (ast.Mult, ast.Div)):
        if isinstance(rank_ast.right, ast.Constant):
            rank_ast = rank_ast.left
        elif isinstance(rank_ast.left, ast.Constant):
            rank_ast = rank_ast.right
        else:
            break

    rel_metric = eval_ast(rel_metric_ast, metametric_vars) if rel_metric_ast is not None else None
    rank_bonus = eval_ast(rank_ast, metametric_vars) if rank_ast is not None else None

    return rel_metric, rank_bonus


def has_runtime_metrics(metric: str):
    return not ast_vars(ast.parse(metric, mode='eval').body).isdisjoint(RUNTIME_METRIC_VARS.keys())


def waldos(soln):
    """Return the number of waldos used by the solution (i.e. that have any non-Start instruction)."""
    return sum(1 for reactor in soln.reactors for waldo in reactor.waldos if waldo)


def pipe_segments(soln):
    """Return the total length of pipes used by the solution."""
    return sum(len(pipe) for component in soln.components for pipe in component.out_pipes)


def waldopath(soln):
    """Return the total number of reactor cells that are covered by the path of either waldo. Note that this may
    include cells the waldo never actually enters due to how SC draws paths.
    Also includes the (at least one) cell occupied by an unused waldo, unlike some older tournament definitions.
    """
    def is_valid_posn(posn):
        return 0 <= posn.col < Reactor.NUM_COLS and 0 <= posn.row < Reactor.NUM_ROWS

    total_waldopath = 0
    branching_instr_types = {InstructionType.SENSE, InstructionType.FLIP_FLOP}
    for reactor in soln.reactors:
        covered_posns = set()
        for waldo in reactor.waldos:
            # Note that this hasn't accounted for any arrow on the start posn yet
            start_posn, start_dirn = next((posn, cmd.direction) for posn, cmd in waldo.commands.items()
                                          if cmd.type == InstructionType.START)
            visited_posn_dirns = set()  # posn + direction tuples to catch when we're looping
            unexplored_branches_stack = [(start_posn, start_dirn)]
            while unexplored_branches_stack:
                cur_posn, cur_dirn = unexplored_branches_stack.pop()

                # Arrows update the direction of the current branch but don't create a new one
                if cur_posn in waldo.arrows:
                    cur_dirn = waldo.arrows[cur_posn]

                # Check the current position/direction against the visit map. We do this after evaluating the arrow to
                # reduce excess visits (since the original direction of a waldo never matters to its future path if an
                # arrow is present, unlike with branching commands)
                posn_dirn = (cur_posn, cur_dirn)
                if posn_dirn in visited_posn_dirns:
                    # We've already explored this cell in the current direction and must have already added any branches
                    # starting from this cell, so end this branch
                    continue

                visited_posn_dirns.add(posn_dirn)

                # Add any new branch
                cmd = waldo.commands[cur_posn] if cur_posn in waldo.commands else None
                if cmd is not None and cmd.type in branching_instr_types:
                    next_branch_posn = cur_posn + cmd.direction
                    if is_valid_posn(next_branch_posn):
                        unexplored_branches_stack.append((next_branch_posn, cmd.direction))

                # Put the current branch back on top of the stack
                next_posn = cur_posn + cur_dirn
                if is_valid_posn(next_posn):
                    unexplored_branches_stack.append((next_posn, cur_dirn))

            # Once we've explored all branches, add this waldo's visited posns to the reactor's covered posns
            covered_posns |= set(posn for posn, _ in visited_posn_dirns)

        # Add this reactor's number of covered posns to the total waldopath
        total_waldopath += len(covered_posns)

    return total_waldopath


def used_bonders(soln):
    """Return the number of bonders in the solution which have been placed adjacent to another (compatible) bonder."""
    num_used_bonders = 0
    for reactor in soln.reactors:
        # TODO: These weren't really meant to be user-exposed, relying on them is a bit sus
        cur_used_bonders = set(p1 for p1, _, _, in reactor.bond_plus_pairs)
        cur_used_bonders |= set(p2 for _, p2, _, in reactor.bond_plus_pairs)
        cur_used_bonders |= set(p1 for p1, _, _, in reactor.bond_minus_pairs)
        cur_used_bonders |= set(p2 for _, p2, _, in reactor.bond_minus_pairs)
        num_used_bonders += len(cur_used_bonders)

    return num_used_bonders


def num_arrows(soln):
    """Return the number of arrows in the solution."""
    return sum(len(waldo.arrows) for reactor in soln.reactors for waldo in reactor.waldos)


def num_instrs_of_type(soln, instr_type, target_idx=None):
    """Return the number of non-arrow instructions of the given type (and optionally target_idx) in the solution."""
    return sum(1
               for reactor in soln.reactors
               for waldo in reactor.waldos
               for cmd in waldo.commands.values()
               if cmd.type == instr_type
               and (target_idx is None or cmd.target_idx == target_idx))


def completed_outputs(soln):
    """Given a Solution object that has run to completion or error, return the number of completed output molecules."""
    return sum(output.current_count for output in soln.outputs)


def recycler_pipes(soln):
    """Return the number of pipes in the solution that attach to a recycler."""
    return sum(1
               for component in soln.components
               if isinstance(component, Recycler)
               for pipe in component.in_pipes
               if pipe is not None)


def max_symbols(soln):
    """Return the maximum number of symbols in any reactor."""
    max_symbols = 0
    for reactor in soln.reactors:
        max_symbols = max(max_symbols, sum(len(waldo) for waldo in reactor.waldos))

    return max_symbols


def max_waldo_symbols(soln):
    """Return the maximum number of symbols in any waldo."""
    max_symbols = 0
    for reactor in soln.reactors:
        for waldo in reactor.waldos:
            max_symbols = max(max_symbols, len(waldo))

    return max_symbols


def symbol_footprint(soln):
    """Return the number of reactor tiles that have a symbol on them. Like waldopath but ignoring empty paths."""
    total = 0
    for reactor in soln.reactors:
        symbol_posns = set()
        for waldo in reactor.waldos:
            symbol_posns.update(waldo.arrows)
            symbol_posns.update(waldo.commands)
        total += len(symbol_posns)

    return total


def max_symbol_footprint(soln):
    """Return the maximum symbol_footprint of any single reactor."""
    max_footprint = 0
    for reactor in soln.reactors:
        symbol_posns = set()
        for waldo in reactor.waldos:
            symbol_posns.update(waldo.arrows)
            symbol_posns.update(waldo.commands)
        max_footprint = max(max_footprint, len(symbol_posns))

    return max_footprint


def name_is_score(soln):
    try:
        return float(soln.name.split()[-1])  # Dodge any [author] prefix we may have added
    except ValueError:
        return 999_999_999


def cycle_handler_runtime_metrics(solution):
    """A custom handler we can pass to schem to collect the stats from RUNTIME_METRIC_VARS (which require measurement
    during solution runtime). Pass to schem.Solution.run's cycle_handler; runs once per cycle.
    """
    if solution.cycle == 1:
        solution.custom_data = {v: 0 for v in RUNTIME_METRIC_VARS}

    for r, reactor in enumerate(solution.reactors):
        red_cmd = None  # Helper to check for input stalls
        for waldo in reactor.waldos:
            if waldo.position in waldo.arrows:
                arrow = waldo.arrows[waldo.position]
                solution.custom_data['arrow_hits'] += 1

            if waldo.position in waldo.commands:
                cmd = waldo.commands[waldo.position]
                if waldo.idx == 0:
                    red_cmd = cmd

                if cmd.type == InstructionType.BOND_PLUS:
                    solution.custom_data['bond_plus_hits'] += 1
                elif cmd.type == InstructionType.BOND_MINUS:
                    solution.custom_data['bond_minus_hits'] += 1
                elif cmd.type == InstructionType.FUSE:
                    solution.custom_data['fuse_hits'] += 1
                elif cmd.type == InstructionType.SPLIT:
                    solution.custom_data['split_hits'] += 1
                elif cmd.type == InstructionType.SWAP:
                    solution.custom_data['swap_hits'] += 1
                elif cmd.type == InstructionType.ROTATE and not waldo.is_rotating:  # Avoid double-counting
                    solution.custom_data['rotate_hits'] += 1
                elif cmd.type == InstructionType.SYNC:
                    # We'll only count the number of times the waldos actually both synchronized, not the number of
                    # cycles either waldo was waiting for the other.
                    if waldo.idx == 0:  # Avoid double-count.
                        other_waldo = reactor.waldos[1]
                        if (other_waldo.position in other_waldo.commands
                            and other_waldo.commands[other_waldo.position] == InstructionType.SYNC):
                            solution.custom_data['sync_hits'] += 1

    # Piped molecules
    if solution.cycle == 1:
        solution.custom_data['piped_molecules'] = 0
        solution.custom_data['_empty_pipes'] = set()

        # The below code will miss any molecules piped on the last cycle. This would be fine if consistent, but
        # multiple outputs on the last cycle could save 1 piped_molecule over other solutions.
        # To avoid this gaming, we'll simply disallow 1-long pipes going to outputs.
        # Note that this also makes the metric nonsense in research levels, which it is.
        for output in solution.outputs:
            if len(output.in_pipe) == 1:
                raise ValueError("Due to technical limitations, 1-long pipes to outputs are disallowed in piped_molecules metric puzzles.")

    for reactor in solution.reactors:
        for pipe in reactor.out_pipes:
            if ((pipe._add_cycles and pipe._add_cycles[0] == solution.cycle - 1)
                # If the pipe was empty last we checked but it just outputted, it's
                # a 1-long pipe and we missed its instant input-ouput cycle.
                or (pipe in solution.custom_data['_empty_pipes']
                    and pipe._last_pop_cycle == solution.cycle - 1)):
                solution.custom_data['piped_molecules'] += 1

            # To detect 1-long pipe same-cycle in-outs without double-counting,
            # track whether the pipe was empty last cycle
            if len(pipe) == 1:
                if len(pipe._molecules) == 0:
                    solution.custom_data['_empty_pipes'].add(pipe)
                else:
                    solution.custom_data['_empty_pipes'].discard(pipe)
