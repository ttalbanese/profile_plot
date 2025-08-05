# %%
import duckdb
import holoviews as hv
import pandas as pd
import panel as pn
from holoviews import opts
from functools import partial

pn.extension()
hv.extension("bokeh")

# %% [markdown]
# ### File Locations

# %%
DATA_DIR = r"data/parquet"
BANKIES = r"data/embankment_z.csv"
BANKIE_POINTS = r"data/all_embankment_points.csv"
STORMS = r"data/storms.csv"

# %% [markdown]
# ### Constants

# %%
PROFILE_WIDTH = 900
PROFILE_HEIGHT = 600

# %% [markdown]
# ### Initialize Data

# %%
CON = duckdb.connect(database=":memory:")

CON.execute(
    f"""CREATE TABLE wse AS
    SELECT * FROM '{DATA_DIR +"/*.parquet"}';"""
)

CON.execute(
    """
ALTER TABLE wse
RENAME COLUMN "Plan Name" to plan_name
"""
)
CON.execute(
    f"""CREATE TABLE banks AS
    SELECT InterimNam, Side, M_Start, M_End, z FROM '{BANKIES}';"""
)

CON.execute(
    f"""CREATE TABLE bank_points AS
    SELECT InterimNam, Side, Milepost, z FROM '{BANKIE_POINTS}';"""
)
CON.execute(
    f"""CREATE TABLE storms AS
    SELECT * FROM '{STORMS}';"""
)
CON.execute(
    """
CREATE TABLE overtops AS (
SELECT *
FROM wse
CROSS JOIN banks
WHERE wse.Milepost BETWEEN banks.M_Start AND banks.M_End 
      AND wse.WSE > banks.z
)
            
"""
)

# List available parquet files
# available_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')]
dataset_names = [
    d[0]
    for d in CON.execute(
        "SELECT DISTINCT plan_name FROM wse ORDER BY plan_name"
    ).fetchall()
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
                    (PIVOT (SELECT Timestep, plan_name FROM wse) ON plan_name) 
                WHERE NOT missing
            )
            """
    ).fetchall()
]
timesteps = pd.to_datetime(dataset_times, format="%d%b%Y %H:%M:%S").sort_values()

t = [t.strftime("%d%b%Y %H:%M:%S").upper() for t in timesteps]

storm_names = [
    d[0]
    for d in CON.execute("SELECT DISTINCT Storm FROM storms ORDER BY Storm").fetchall()
]
# Initial dataset
initial_dataset = dataset_names[0]
initial_time = t[0]
max_t = t[-1]

# %% [markdown]
# ### Intialize Controls

# %%
PLAN_WIDGET = pn.widgets.MultiChoice(
    name="Plan Name", value=dataset_names[0:1], options=dataset_names, solid=False
)
TIMESTEP_WIDGET = pn.widgets.IntSlider(
    name="Timestep", value=0, start=0, end=len(t) - 1
)

# %% [markdown]
# ### Load Static Data

# %%
storm_data = CON.execute(
    """
        SELECT
        Timestep,
        Accumulation,
        Rate,
        Storm
        FROM storms
        WHERE Timestep IN $dataset_times
        ORDER BY Timestep
        """,
    dict(dataset_times=dataset_times),
).fetch_df()

fixed_data = []
for ix, storm in storm_data.groupby("Storm"):
    missing_timesteps = list(set(t) - set(storm["Timestep"].to_list()))
    if len(missing_timesteps) > 0:
        new_df = pd.DataFrame(
            data={
                "Timestep": missing_timesteps,
                "Rate": [0] * len(missing_timesteps),
                "Storm": [ix] * len(missing_timesteps),
                "Accumulation": [storm["Accumulation"].iloc[-1]]
                * len(missing_timesteps),
            }
        )
        storm = pd.concat([storm, new_df])
    fixed_data.append(storm)
storm_data = pd.concat(fixed_data)

embankment_z = CON.execute(
    """SELECT *
FROM bank_points ORDER BY Side, Milepost"""
).fetch_df()

# %% [markdown]
# ### Profile Plot

# %%
embankment_z_dots = (
    hv.Scatter(embankment_z, "milepost", ["z", "Side"])
    .groupby("Side")
    .overlay("Side")
    .redim(milepost="Milepost")
)

posxy = hv.streams.Tap(source=embankment_z_dots, x=250, y=450)


def load_profile(timestep, plan_name, tap_x=None, **kwargs):

    df = CON.execute(
        "SELECT * FROM wse WHERE Timestep = $t AND plan_name IN $p",
        {"t": t[timestep], "p": plan_name},
    ).fetch_df()  # .groupby("plan_name")

    wse = (
        hv.Curve(df, "Milepost", ["WSE", "plan_name"])
        .groupby("plan_name")
        .overlay("plan_name")
    )
    if tap_x is not None:
        wse = wse * hv.VLine(tap_x)
    return wse


def load_north_rects(timestep, plan_name, tap_x=None, **kwargs):
    north_rects = CON.execute(
        """SELECT 
            M_End,
            0,
            M_start,
            50,
            CASE 
            WHEN InterimNam IN 
                (SELECT InterimNam 
                FROM overtops WHERE plan_name IN $plan_name AND Timestep =$Timestep) 
            THEN 'red' ELSE 'green' 
            END AS color
            FROM banks
            WHERE Side = 'North'
            """,
        {"plan_name": plan_name, "timestep": t[timestep]},
    ).fetchall()

    north = (
        hv.Rectangles(north_rects, vdims="color")
        .relabel("North Embankments")
        .redim(x0="Milepost", y0="North")
    )
    if tap_x is not None:
        north = north * hv.VLine(tap_x)
    return north


def load_south_rects(timestep, plan_name, tap_x=None, **kwargs):
    south_rects = CON.execute(
        """SELECT 
            M_End,
            0,
            M_start,
            50,
            CASE 
            WHEN InterimNam IN 
                (SELECT InterimNam 
                FROM overtops WHERE plan_name IN $plan_name AND Timestep =$Timestep) 
            THEN 'red' ELSE 'green' 
            END AS color
            FROM banks
            WHERE Side = 'South'
            """,
        {"plan_name": plan_name, "timestep": t[timestep]},
    ).fetchall()

    south = (
        hv.Rectangles(south_rects, vdims="color")
        .relabel("South Embankments")
        .redim(x0="Milepost", y0="South")
    )
    if tap_x is not None:
        south = south * hv.VLine(tap_x)
    return south


streams = dict(
    timestep=TIMESTEP_WIDGET.param.value,
    plan_name=PLAN_WIDGET.param.value,
    tap_x=posxy.param.x,
)


profile_panel = (
    (
        embankment_z_dots * hv.DynamicMap(load_profile, streams=streams)
        + hv.DynamicMap(load_north_rects, streams=streams)
        + hv.DynamicMap(load_south_rects, streams=streams)
    )
    .relabel("17-Mile Pool Hydro System")
    .opts(
        opts.Scatter(
            legend_position="top_right",
            alpha=0.4,
            toolbar=None,
            responsive=True,
            min_width=PROFILE_WIDTH,
            min_height=PROFILE_HEIGHT,
        ),
        opts.Curve(
            invert_xaxis=True,
            toolbar=None,
        ),
        opts.Rectangles(
            responsive=True,
            min_width=PROFILE_WIDTH,
            max_height=100,
            min_height=100,
            invert_xaxis=True,
            color="color",
            xlabel="",
            ylabel="",
            yticks=0,
            xticks=0,
            show_legend=False,
            toolbar=None,
        ),
        opts.VLine(
            color="black",
            line_dash="dashed",
            line_alpha=0.5,
            toolbar=None,
        ),
    )
    .cols(1)
)
# definemp.update()

# %% [markdown]
# ### Milepost Plot

# %%
# CURVE showing WSE at location over Time


def milepost_profile(tap_x, timestep, plan_name):

    closest_milepost = CON.execute(
        """
    SELECT Milepost
    FROM wse
    ORDER BY ABS(Milepost - $x)
    LIMIT 1
""",
        dict(x=tap_x),
    ).fetchall()[0][0]

    # closest_milepost = 250.00
    wse_points = CON.execute(
        "SELECT Timestep, WSE, plan_name FROM wse WHERE Timestep = $t AND plan_name IN $p AND Milepost = $m",
        {"t": t[timestep], "p": plan_name, "m": closest_milepost},
    ).fetch_df()

    wse_curve = CON.execute(
        """SELECT 
        Timestep, 
        WSE, 
        plan_name 
        FROM wse 
        WHERE plan_name IN $p AND Milepost = $m AND Timestep <= $max_t ORDER BY Timestep""",
        {"p": plan_name, "m": closest_milepost, "max_t": max_t},
    ).fetch_df()
    scatter = (
        hv.Scatter(wse_points, "Timestep", ["WSE", "plan_name"])
        .groupby("plan_name")
        .overlay("plan_name")
    )
    curve = (
        hv.Curve(wse_curve, "Timestep", ["WSE", "plan_name"])
        .groupby("plan_name")
        .overlay("plan_name")
    )

    return (
        (curve * scatter)
        .redim(WSE="wse")
        .relabel(f"Milepost: {closest_milepost:.2f}\nTimestep: {t[timestep]}")
        .opts(
            opts.Scatter(
                marker="s",
                size=14,
                width=500,
                alpha=0.9,
                show_legend=False,
                # xticks=[(0, 'zero'), (50, 'fifty'), (100, 'one hundred')]
                # xrotation=45,
                xticks=0,
                xlabel="",
                toolbar=None,
            ),
            opts.Curve(
                show_legend=False,
                toolbar=None,
                # xrotation=45,
                # xticks=5,
            ),
        )
    )


detail = hv.DynamicMap(milepost_profile, streams=streams)

# hyeto_plot = hv.DynamicMap(hyetograph, streams=dict(storm_name=STORM_WIDGET.param.value))

# %% [markdown]
# ### Storm Plots


# %%
def add_dot(storm, timestep):

    filtered = storm_data.loc[
        (storm_data["Storm"] == storm) & (storm_data["Timestep"] == t[timestep]), :
    ]

    return hv.Scatter(filtered, "Timestep", "Accumulation").opts(
        marker="s", size=14, width=600, alpha=0.9, xticks=0
    )


hyeto_stream = dict(
    timestep=TIMESTEP_WIDGET.param.value,
)

hyeto_list = [
    (
        ix,
        (
            hv.Bars(storm, "Timestep", "Rate").opts(ylabel="Rate", cticks=0, xticks=0)
            * hv.Curve(storm, "Timestep", "Accumulation")
            * hv.DynamicMap(
                partial(add_dot, ix),
                streams=hyeto_stream,
            )
        )
        .redim(Rate=ix)
        .relabel("Simulated Storm(s)")
        .opts(
            multi_y=True,
            shared_axes=False,
            width=540,
            xticks=0,
            xlabel="",
            toolbar=None,
        ),
    )
    for ix, storm in storm_data.sort_values("Timestep").groupby("Storm")
]

hyeto_tabs = pn.Tabs(*hyeto_list, tabs_location="below")

# %% [markdown]
# ### App

# %%
template = pn.template.BootstrapTemplate(title="17-Mile Pool Hydrosystem")
template.sidebar.append(PLAN_WIDGET)
template.sidebar.append(TIMESTEP_WIDGET)

template.main.append(pn.Row(profile_panel, pn.Column(detail, hyeto_tabs)))
template.servable()


template
