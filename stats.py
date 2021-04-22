#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

import plotly
import plotly.graph_objs as go
from schem import Score

from utils import format_date


def pareto_graph(out_file: Path, scoring_submit_history, fun_submit_history=None):
    """Create an html file containing all submitted solutions plotted as symbols vs. cycles.

    Include toggleable lines tracing the optimization path of each player's scoring submissions.
    """
    # Create a scatter plot of symbols vs. cycles for all submissions
    combined_submissions = []
    for submissions in scoring_submit_history.values():
        combined_submissions.extend(submissions)
    if fun_submit_history is not None:
        for submissions in fun_submit_history.values():
            combined_submissions.extend(submissions)

    scores = [Score.from_str(submission[1]) for submission in combined_submissions]

    node_trace = go.Scatter(mode='markers',
                            x=tuple(x.symbols for x in scores),
                            y=tuple(x.cycles for x in scores),
                            marker=dict(
                                color='black',
                                size=5,
                                line=dict(width=0.5)),  # The thickness of the node's border line
                            hoverinfo='text',
                            hoverlabel=dict(align='left'),
                            hovertext=tuple(f"{r[1]} ({round(r[2], 3)})"               # c-r-s (metric)
                                            f"<br>{r[3] if r[3] is not None else ''}"  # Solution name
                                            f"<br>{format_date(r[0])}"                 # Date
                                            for r in combined_submissions),
                            showlegend=False)

    # Overlay a line for each player showing the path their scoring submissions took
    # TODO: Pass in teams info too, and in the case of teams formed partway through the round, connect their last
    #       solo submissions up the first team submission
    author_traces = []
    for author, submissions in scoring_submit_history.items():
        scores = [Score.from_str(submission[1]) for submission in submissions]
        author_traces.append(
            go.Scatter(mode='lines',
                       name=author,
                       x=tuple(score.symbols for score in scores),
                       y=tuple(score.cycles for score in scores),
                       line=dict(width=0.5),
                       marker=dict(size=5,
                                   line=dict(width=0.5)),  # The thickness of the node's border line
                       hoverinfo='none'))

    # TODO: Add lines showing the pareto frontier globally and by reactor count

    # Prepare the figure layout
    fig_layout = go.Layout(
        title=dict(text="<b>Solution Space</b>",
                   font=dict(color='black', size=16),
                   x=0.5),
        legend=dict(itemdoubleclick=False),
        hovermode='closest',  # Show hover text of node closest to the mouse position
        dragmode='pan',  # Default mouse mode
        margin=dict(b=5, l=5, r=5, t=40, pad=0),
        annotations=[dict(
            text="Click on legend items to show/hide them.",
            showarrow=False,
            xref="paper", yref="paper",
            x=1.0, y=1.0)],
        plot_bgcolor='white',
        xaxis=dict(title='Symbols', linecolor='black', ticks='outside', tickcolor='black'),
        yaxis=dict(title='Cycles', linecolor='black', ticks='outside', tickcolor='black'))

    fig = go.Figure(data=[node_trace] + author_traces, layout=fig_layout)

    plotly.offline.plot(fig, filename=str(out_file), show_link=False, auto_open=False)


def metric_over_time(out_file: Path, scoring_submit_history, puzzle_start, puzzle_end):
    """Create a plot showing the metric scores of all submitters over time."""
    # Add a line trace for each player's metric over time
    author_traces = []
    for author, submissions in scoring_submit_history.items():
        author_traces.append(
            go.Scatter(mode='lines+markers',
                       name=author,
                       x=tuple(submission[0] for submission in submissions),  # Submit date
                       y=tuple(submission[2] for submission in submissions),  # metric
                       line=dict(width=0.5),
                       marker=dict(size=5,
                                   line=dict(width=0.5)),  # The thickness of the node's border line
                       hoverinfo='text',
                       hoverlabel=dict(align='left'),
                       hovertext=tuple(f"{r[1]} ({round(r[2], 3)})"                   # c-r-s (metric)
                                       + (f"<br>{r[3]}" if r[3] is not None else '')  # Soln name
                                       + f"<br>{format_date(r[0])}"                   # Date
                                       + (f"<br>{r[4]}" if r[4] is not None else '')  # Comment
                                       for r in submissions)))

    # TODO: Calculate and add a line for the global minimum metric over time

    # Prepare the figure layout
    fig_layout = go.Layout(
        title=dict(text="<b>Metric Progression</b>",
                   font=dict(color='black', size=16),
                   x=0.5),
        hovermode='closest',  # Show hover text of node closest to the mouse position
        dragmode='pan',  # Default mouse mode
        margin=dict(b=5, l=5, r=5, t=40, pad=0),
        annotations=[dict(
            text="Click on legend items to show/hide them.",
            showarrow=False,
            xref="paper", yref="paper",
            x=1.0, y=1.0)],
        plot_bgcolor='white',
        xaxis=dict(title="Time", linecolor='black', ticks='outside', tickcolor='black',
                   range=[puzzle_start, puzzle_end]),
        yaxis=dict(title="Metric", linecolor='black', ticks='outside', tickcolor='black'))

    fig = go.Figure(data=author_traces, layout=fig_layout)

    plotly.offline.plot(fig, filename=str(out_file), show_link=False, auto_open=False)
