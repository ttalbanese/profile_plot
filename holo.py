import pandas as pd
import duckdb
import holoviews as hv
from holoviews import opts
hv.extension('bokeh')

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
initial_dataset = dataset_names[0]
initial_time = t[0]
# df = CON.execute(
#     'SELECT * FROM wse WHERE "Plan Name" IN $dataset_name AND Timestep = $dataset_time',
#     {"dataset_name": initial_dataset, "dataset_time": initial_time},
# ).fetch_df()
df = CON.execute('SELECT * FROM wse WHERE Timestep = $ts AND "Plan Name" = $ids', {'ts': initial_time, 'ids': initial_dataset}).fetch_df()
ds = hv.Dataset(df, ['Timestep', 'Milepost', "Plan Name"], ['WSE'])

layout = ds.to(hv.Curve, 'Milepost', 'WSE')
layout.opts(
    opts.Curve(width=600, height=250))