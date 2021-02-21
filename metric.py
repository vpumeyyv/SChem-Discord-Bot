#!/usr/bin/env python
# -*- coding: utf-8 -*-

"Contains functions for parsing tournament metric equations."

import ast
import operator as op

from schem.waldo import InstructionType

# Operators allowed in puzzle metric strings
METRIC_OPS = {ast.Add: op.add, ast.Sub: op.sub, ast.Mult: op.mul, ast.Div: op.truediv, ast.Pow: op.pow, ast.USub: op.neg}
# Functions for calculating values in a metric equation, given a Solution object
METRIC_VAR_TO_FN = {'cycles': lambda soln: soln.expected_score.cycles,
                    'reactors': lambda soln: soln.expected_score.reactors,
                    'symbols': lambda soln: soln.expected_score.symbols,
                    'waldopath': lambda soln: waldopath(soln),
                    'waldos': lambda soln: waldos(soln),
                    'bonders': lambda soln: used_bonders(soln)}
                    # TODO: 'outputs': lambda soln: completed_outputs(soln)
                    #       requires modifications to tournament validator to accept solutions without an expected
                    #       score if the metric contains 'outputs', and to eval the metric even if the solution crashes

def waldos(soln):
    """Return the number of waldos used by the solution (i.e. that have any non-Start instruction)."""
    return sum(1 for reactor in soln.reactors for waldo in reactor.waldos if waldo)

def waldopath(soln):
    """Return the total number of reactor cells that are covered by the path of either waldo. Note that this may
    include cells the waldo never actually enters due to how SC draws paths.
    Also includes the (at least one) cell occupied by an unused waldo, unlike some older tournament definitions.
    """
    def is_valid_posn(posn):
        return 0 <= posn.col < 10 and 0 <= posn.row < 8

    total_waldopath = 0
    branching_instr_types = set(InstructionType.SENSE, InstructionType.FLIP_FLOP)
    for reactor in solution.reactors:
        covered_posns = set()
        for waldo in reactor.waldos:
            # Note that this hasn't accounted for any arrow on the start posn yet
            start_posn, start_dirn = next((posn, cmd.direction) for posn, (_, cmd) in waldo.instr_map.items()
                                          if cmd.type == InstructionType.START)
            visited_posn_dirns = set()  # posn + direction tuples to catch when we're looping
            unexplored_branches_stack = [(start_posn, start_dirn)]
            while unexplored_branches:
                cur_posn, cur_dirn = unexplored_branches_stack.pop()

                # Check the current cell for an arrow and/or branching instruction
                arrow_dirn, cmd = waldo.instr_map[cur_posn]

                # Arrows update the direction of the current branch but don't create a new one
                if arrow_dirn is not None:
                    cur_dirn = arrow_dirn

                # Check the current position/direction against the visit map. We do this after evaluating the arrow to
                # reduce excess visits (since the original direction of a waldo never matters to its future path if an
                # arrow is present, unlike with branching commands)
                posn_dirn = (cur_posn, cur_dirn)
                if posn_dirn in visited_posn_dirns:
                    # We've already explored this cell in the current direction and must have already added any branches
                    # starting from this cell, so end this branch
                    continue
                else:
                    visited_posn_dirns.add(posn_dirn)

                # Add any new branch
                if cmd.type in branching_instr_types:
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

    return waldopath

def used_bonders(soln):
    """Return the number of bonders in the solution which have been placed adjacent to another (compatible) bonder."""
    num_used_bonders = 0
    for reactor in soln.reactors:
        # TODO: These weren't really meant to be user-exposed
        used_bonders = set(p1 for p1, _, _, in reactor.bonder_plus_pairs)
        used_bonders |= set(p2 for _, p2, _, in reactor.bonder_plus_pairs)
        used_bonders |= set(p1 for p1, _, _, in reactor.bonder_minus_pairs)
        used_bonders |= set(p2 for _, p2, _, in reactor.bonder_minus_pairs)
        num_used_bonders += len(used_bonders)

    return num_used_bonders

def completed_outputs(soln):
    """Given a Solution object that has run to completion or error, return the number of completed output molecules."""
    return sum(output.current_count for output in soln.outputs)

def metric_vars(metric_str):
    """Given a metric equation string, return a set of all variables (as strings) used in it."""
    return ast_vars(ast.parse(metric_str, mode='eval').body)

def ast_vars(node):
    """Helper to metric_vars. Return a set of all variables (as strings) in the given AST."""
    if isinstance(node, ast.Name):
        return set((node.id,))
    elif isinstance(node, ast.Num):
        return set()
    elif isinstance(node, ast.BinOp):
        return ast_vars(node.left) | ast_vars(node.right)
    elif isinstance(node, ast.UnaryOp):
        return ast_vars(node.operand)
    else:
        raise TypeError(node)

def validate_metric(metric_str):
    """Raise an error if the given metric string is unparsable."""
    for metric_var in metric_vars(metric_str):
        if metric_var not in METRIC_VAR_TO_FN:
            raise ValueError(f"Unknown var `{metric_var}` in metric equation.")

def eval_metric(soln, metric_str):
    """Score the (assumed to be already-validated) given solution using the given metric expression.
    Valid ops: +, -, *, /, ** or ^
    Valid terms: any real number, or any of:
        cycles, reactors, symbols: Per usual.
        waldos: Number of non-empty waldos in the solution.
        waldopath: Number of reactor cells crossed by a waldopath
    """
    # Parse the metric into an AST
    ast_tree = ast.parse(metric_str, mode='eval').body

    # Calculate all variables the metric needs
    vars_dict = {var: METRIC_VAR_TO_FN[var](soln) for var in ast_vars(ast_tree)}

    return eval_ast(ast_tree, vars_dict)

def eval_ast(node, vars_dict):
    """Helper for evaluating a puzzle metric (safer than built-in eval)"""
    if isinstance(node, ast.Name):
        if node.id not in vars_dict:
            raise Exception(f"Unknown metric var `{node.id}`")
        return vars_dict[node.id]
    elif isinstance(node, ast.Num):
        return node.n
    elif isinstance(node, ast.BinOp):
        return METRIC_OPS[type(node.op)](eval_ast(node.left, names), eval_ast(node.right, names))
    elif isinstance(node, ast.UnaryOp):
        return METRIC_OPS[type(node.op)](eval_ast(node.operand, names))
    else:
        raise TypeError(node)
