import pickle
import sys

from dash import Dash, dash_table, dcc, callback, Output, Input, html, _dash_renderer
import pandas as pd
import plotly.express as px
import dash_mantine_components as dmc
sys.path.append('../')
from dask_md_objs import TaskHandler, Task, SchedulerEvent

_dash_renderer._set_react_version("18.2.0")

th: TaskHandler = None

with open("../../data/compressed_out.pickle", 'rb') as f:
    th = pickle.load(f)
names = th.return_names()
#df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gapminder_unfiltered.csv')

app = Dash()

app.layout = dmc.MantineProvider(
    html.Div(
        dmc.Container(
            [
            html.H1(children='DASK Metadata Collection Viewer', style={'textAlign':'center'}),
            dcc.Dropdown(names, names[0], id='dropdown-selection'),
            #dcc.Graph(id='graph-content'),
            html.Details([
                html.Summary("Label"),
                html.Div("contents", id="TaskDisplay")
            ])
            ]
        )
    )
)

@callback(
    Output('TaskDisplay','children'),
    Input('dropdown-selection', 'value')
)
def update_div(selected_task) :
    divs = []
    res: Task = th.get_task_by_name(selected_task)
    for ev in res.events :
        if isinstance(ev, SchedulerEvent) :
            divs.append(generate_div_from_sched_event(ev))
    
    return divs

def generate_div_from_sched_event(ev: SchedulerEvent) :
    div = html.Div([
        html.Div(html.B("Scheduler Event")),
        html.Div(f"Event time: {ev.t_event}")
    ])
    return div

#@callback(
#    Output('graph-content', 'figure'),
#    Input('dropdown-selection', 'value')
#)
#def update_graph(value):
#    dff = df[df.country==value]
#    return px.line(dff, x='year', y='pop')

if __name__ == '__main__':
    app.run(debug=True)