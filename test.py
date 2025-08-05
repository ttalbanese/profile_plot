from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Select, Slider, CustomJS, MultiChoice
from bokeh.plotting import figure
import pandas as pd
import os
from datetime import date
import duckdb

# Directory containing the parquet files
DATA_DIR = r"C:/Users/talbanese/OneDrive - Schnabel Engineering Inc/Documents/python_projects/time_series_plots/data/parquet"
CON = duckdb.connect(database=":memory:")

CON.execute(
    f"""CREATE TABLE wse AS
    SELECT * FROM '{DATA_DIR +"/*.parquet"}';"""
)

# List available parquet files
# available_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')]
dataset_names = [
    d[0] for d in CON.execute('SELECT DISTINCT "Plan Name"  FROM wse').fetchall()
]
dataset_times = [
    d[0]
    for d in CON.execute(
        """ 
            SELECT
                DISTINCT Timestep
                FROM (
                SELECT 
                    Timestep, 
                    list_contains(list_value(*COLUMNS(* EXCLUDE (Timestep))), 0) missing 
                FROM 
                    (PIVOT (SELECT Timestep, "Plan Name" FROM wse) ON "Plan Name") 
                WHERE NOT missing
            )
            """
    ).fetchall()
]
timesteps = pd.to_datetime(dataset_times, format="%d%b%Y %H:%M:%S").sort_values()

t = [t.strftime("%d%b%Y %H:%M:%S").upper() for t in timesteps]

# Initial dataset
initial_dataset = [dataset_names[0]]
initial_time = t[0]
df = CON.execute(
    'SELECT * FROM wse WHERE "Plan Name" IN $dataset_name AND Timestep = $dataset_time',
    {"dataset_name": initial_dataset, "dataset_time": initial_time},
).fetch_df()


# Create a ColumnDataSource
source = ColumnDataSource(data=df)

# Create the plot
plot = figure(
    title="Line Graph",
    x_axis_label="Milepost",
    y_axis_label="WSE",
    width=800,
    height=400,
)
plot.line("Milepost", "WSE", source=source)

# Create widgets
# dataset_select = Select(
#     title="Select Dataset", value=initial_dataset[0], options=dataset_names
# )

dataset_select = MultiChoice(value=initial_dataset, options = dataset_names)

timestep_slider = Slider(
    start=0, end=len(timesteps) - 1, step=1, value=0, title=f"Time: {t[0]}"
)
timestep_slider.js_on_change(
    "value",
    CustomJS(
        args=dict(sl=timestep_slider, t=t),
        code="""
    sl.title = "Time: " + t[this.value];
""",
    ),
)


# Update function
def update_data(attr, old, new):
    dataset = dataset_select.value
    timestep = t[int(timestep_slider.value)]
    df = CON.execute(
        'SELECT * FROM wse WHERE "Plan Name" IN $dataset_name AND Timestep = $dataset_time',
        {"dataset_name": dataset, "dataset_time": timestep},
    ).fetch_df()
    source.data = df


# Attach callbacks
dataset_select.on_change("value", update_data)
timestep_slider.on_change("value", update_data)

# Initial load
#update_data(None, None, None)

# Layout
layout = column(row(dataset_select, timestep_slider), plot)

# Add to document
curdoc().add_root(layout)
curdoc().title = "Dynamic Line Plot"
