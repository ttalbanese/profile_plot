importScripts("https://cdn.jsdelivr.net/pyodide/v0.27.5/full/pyodide.js");

function sendPatch(patch, buffers, msg_id) {
  self.postMessage({
    type: 'patch',
    patch: patch,
    buffers: buffers
  })
}

async function startApplication() {
  console.log("Loading pyodide!");
  self.postMessage({type: 'status', msg: 'Loading pyodide'})
  self.pyodide = await loadPyodide();
  self.pyodide.globals.set("sendPatch", sendPatch);
  console.log("Loaded!");
  await self.pyodide.loadPackage("micropip");
    let zipResponse = await fetch("data.zip");

      let zipBytes = await zipResponse.arrayBuffer();
      pyodide.FS.writeFile("/data.zip", new Uint8Array(zipBytes));

      await pyodide.runPythonAsync(`
    import zipfile
    import os

    with zipfile.ZipFile("/data.zip", "r") as zip_ref:
        zip_ref.extractall("data")

    print("Extracted files:", os.listdir("data"))
  `);
  const env_spec = ['https://cdn.holoviz.org/panel/wheels/bokeh-3.7.3-py3-none-any.whl', 'https://cdn.holoviz.org/panel/1.7.2/dist/wheels/panel-1.7.2-py3-none-any.whl', 'pyodide-http==0.2.1', 'duckdb', 'holoviews', 'pandas']
  for (const pkg of env_spec) {
    let pkg_name;
    if (pkg.endsWith('.whl')) {
      pkg_name = pkg.split('/').slice(-1)[0].split('-')[0]
    } else {
      pkg_name = pkg
    }
    self.postMessage({type: 'status', msg: `Installing ${pkg_name}`})
    try {
      await self.pyodide.runPythonAsync(`
        import micropip
        await micropip.install('${pkg}');
      `);
    } catch(e) {
      console.log(e)
      self.postMessage({
	type: 'status',
	msg: `Error while installing ${pkg_name}`
      });
    }
  }
  console.log("Packages loaded!");
  self.postMessage({type: 'status', msg: 'Executing code'})
  const code = `
  \nimport asyncio\n\nfrom panel.io.pyodide import init_doc, write_doc\n\ninit_doc()\n\n\nimport duckdb\nimport holoviews as hv\nimport pandas as pd\nimport panel as pn\nfrom holoviews import opts\nfrom functools import partial\n\npn.extension()\nhv.extension("bokeh")\n\n\n# ### File Locations\n\n\nDATA_DIR = r"data/parquet"\nBANKIES = r"data/embankment_z.csv"\n#BANKIE_POINTS = r"data/all_embankment_points.csv"\nBANKIE_POINTS = r"data/weir_profiles.csv"\nSTORMS = r"data/storms.csv"\n\n\n# ### Constants\n\n\nPROFILE_WIDTH = 900\nPROFILE_HEIGHT = 600\n\n\n# ### Initialize Data\n\n\nCON = duckdb.connect(database=":memory:")\n\nCON.execute(\n    f"""CREATE TABLE wse AS\n    SELECT \n    UPPER(strftime(Timestep, '%d%b%Y %H:%M:%S')) Timestep,\n    replace("Profile Name", 'Half of', '50% of') "Profile Name",\n         * \n         FROM '{DATA_DIR +"/*.parquet"}';"""\n)\n\nCON.execute(\n    """\nALTER TABLE wse\nRENAME COLUMN "Profile Name" to plan_name\n"""\n)\n# CON.execute(\n#     f"""CREATE TABLE banks AS\n#     SELECT InterimNam, Side, M_Start, M_End, z FROM '{BANKIES}';"""\n# )\n\nCON.execute(\n    f"""CREATE TABLE bank_points AS\n    SELECT InterimNam, Side, Milepost, z FROM '{BANKIE_POINTS}';"""\n)\n\nCON.execute(\n    """CREATE TABLE banks AS\n    SELECT \n    InterimNam, \n    ANY_VALUE(Side) Side, \n    MIN(milepost) M_Start, \n    MAX(milepost) M_End, \n    MIN(z) z\n    FROM bank_points\n    GROUP BY InterimNam;\n    """\n)\nCON.execute(\n    f"""CREATE TABLE storms AS\n    SELECT * FROM '{STORMS}';"""\n)\nCON.execute(\n    """\nCREATE TABLE overtops AS (\nSELECT *\nFROM wse\nCROSS JOIN banks\nWHERE wse.Milepost BETWEEN banks.M_Start AND banks.M_End \n      AND wse.WSE > banks.z\n)\n            \n"""\n)\n\n# List available parquet files\n# available_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')]\n\n\ndataset_names = [\n    #name_dict[d[0]]\n    d[0]\n    for d in CON.execute(\n        "SELECT DISTINCT plan_name FROM wse ORDER BY plan_name"\n    ).fetchall()\n]\n\ndataset_times = [\n    d[0]\n    for d in CON.execute(\n        """ \n            SELECT\n                DISTINCT Timestep\n                FROM (\n                SELECT \n                    Timestep, \n                    list_contains(list_value(*COLUMNS(* EXCLUDE (Timestep))), 0) missing \n                FROM \n                    (PIVOT (SELECT Timestep, plan_name FROM wse) ON plan_name) \n                WHERE NOT missing\n            )\n            """\n    ).fetchall()\n]\ntimesteps = pd.to_datetime(dataset_times, format="%d%b%Y %H:%M:%S").sort_values()\n#print(dataset_times)\nt = [t.strftime("%d%b%Y %H:%M:%S").upper() for t in timesteps]\n\nstorm_names = [\n    d[0]\n    for d in CON.execute("SELECT DISTINCT Storm FROM storms ORDER BY Storm").fetchall()\n]\n# Initial dataset\ninitial_dataset = dataset_names[0]\ninitial_time = t[0]\nmax_t = t[-1]\n\n\n# ### Intialize Controls\n\n\nPLAN_WIDGET = pn.widgets.MultiChoice(\n    name="Plan Name", value=dataset_names[0:1], options=dataset_names, solid=False\n)\n\n\n\n# TIMESTEP_WIDGET = pn.widgets.IntSlider(\n#     name="Timestep", value=0, start=0, end=len(t) - 1\n# )\nTIMESTEP_WIDGET = pn.widgets.DiscreteSlider(\n    name="Timestep", value=t[0], options=t, align='start', width=450\n)\n\n\n# ### Load Static Data\n\n\n\nstorm_data = CON.execute(\n    """\n        SELECT\n        Timestep,\n        Accumulation,\n        Rate,\n        Storm\n        FROM storms\n        WHERE Timestep IN $dataset_times\n        ORDER BY Timestep\n        """,\n    dict(dataset_times=dataset_times),\n).fetch_df()\n\nfixed_data = []\nfor ix, storm in storm_data.groupby("Storm"):\n    missing_timesteps = list(set(t) - set(storm["Timestep"].to_list()))\n    if len(missing_timesteps) > 0:\n        new_df = pd.DataFrame(\n            data={\n                "Timestep": missing_timesteps,\n                "Rate": [0] * len(missing_timesteps),\n                "Storm": [ix] * len(missing_timesteps),\n                "Accumulation": [storm["Accumulation"].iloc[-1]]\n                * len(missing_timesteps),\n            }\n        )\n        storm = pd.concat([storm, new_df])\n    fixed_data.append(storm)\nstorm_data = pd.concat(fixed_data)\n\nstorm_data["Storm"] = storm_data['Storm'].map({\n    '100': '100-Year',\n      '10_sqmi_0.5PMF': '10 Sq Mi Half PMF',\n       '10_sqmi_PMF': '10 Sq Mi PMF',\n       '150': '150% of 100-Year',\n       '50': '50-Year',\n       '500': '500-Year',\n       '50_sqmi_0.5PMF': '50 Sq Mi Half PMF',\n '50_sqmi_PMF':'50 Sq Mi PMF',\n})\n\nembankment_z = CON.execute(\n    """SELECT *\nFROM bank_points ORDER BY Side, Milepost"""\n).fetch_df()\n\n\n# ### Profile Plot\n\n\nembankment_z_dots = (\n    hv.Scatter(embankment_z, "milepost", ["z", "Side"])\n    .groupby("Side")\n    .overlay("Side")\n    .redim(milepost="Milepost")\n)\n\nposxy = hv.streams.Tap(source=embankment_z_dots, x=250, y=450)\n\n\ndef load_profile(timestep, plan_name, tap_x=None, **kwargs):\n\n    df = CON.execute(\n        "SELECT * FROM wse WHERE Timestep = $t AND plan_name IN $p",\n        {"t": timestep, "p": plan_name},\n    ).fetch_df()  # .groupby("plan_name")\n\n    wse = (\n        hv.Curve(df, "Milepost", ["WSE", "plan_name"])\n        .groupby("plan_name")\n        .overlay("plan_name")\n    )\n    if tap_x is not None:\n        wse = wse * hv.VLine(tap_x)\n    return wse\n\n\ndef load_north_rects(timestep, plan_name, tap_x=None, **kwargs):\n    north_rects = CON.execute(\n        """SELECT \n            M_End,\n            0,\n            M_start,\n            50,\n            CASE \n            WHEN InterimNam IN \n                (SELECT InterimNam \n                FROM overtops WHERE plan_name IN $plan_name AND Timestep =$Timestep) \n            THEN 'red' ELSE 'green' \n            END AS color\n            FROM banks\n            WHERE Side = 'North'\n            """,\n        {"plan_name": plan_name, "timestep": timestep},\n    ).fetchall()\n\n    north = (\n        hv.Rectangles(north_rects, vdims="color")\n        .relabel("North Embankments")\n        .redim(x0="Milepost", y0="North")\n    )\n    if tap_x is not None:\n        north = north * hv.VLine(tap_x)\n    return north\n\n\ndef load_south_rects(timestep, plan_name, tap_x=None, **kwargs):\n    south_rects = CON.execute(\n        """SELECT \n            M_End,\n            0,\n            M_start,\n            50,\n            CASE \n            WHEN InterimNam IN \n                (SELECT InterimNam \n                FROM overtops WHERE plan_name IN $plan_name AND Timestep =$Timestep) \n            THEN 'red' ELSE 'green' \n            END AS color\n            FROM banks\n            WHERE Side = 'South'\n            """,\n        {"plan_name": plan_name, "timestep": timestep},\n    ).fetchall()\n\n    south = (\n        hv.Rectangles(south_rects, vdims="color")\n        .relabel("South Embankments")\n        .redim(x0="Milepost", y0="South")\n    )\n    if tap_x is not None:\n        south = south * hv.VLine(tap_x)\n    return south\n\n\nstreams = dict(\n    timestep=TIMESTEP_WIDGET.param.value,\n    plan_name=PLAN_WIDGET.param.value,\n    tap_x=posxy.param.x,\n)\n\n\nprofile_panel = (\n    (\n        embankment_z_dots * hv.DynamicMap(load_profile, streams=streams)\n        + hv.DynamicMap(load_north_rects, streams=streams)\n        + hv.DynamicMap(load_south_rects, streams=streams)\n    )\n    .relabel("17-Mile Pool Hydro System")\n    .opts(\n        opts.Scatter(\n            legend_position="top_right",\n            alpha=0.4,\n            toolbar=None,\n            responsive=True,\n            min_width=PROFILE_WIDTH,\n            min_height=PROFILE_HEIGHT,\n        ),\n        opts.Curve(\n            invert_xaxis=True,\n            toolbar=None,\n        ),\n        opts.Rectangles(\n            responsive=True,\n            min_width=PROFILE_WIDTH,\n            max_height=100,\n            min_height=100,\n            invert_xaxis=True,\n            color="color",\n            xlabel="",\n            ylabel="",\n            yticks=0,\n            xticks=0,\n            show_legend=False,\n            toolbar=None,\n        ),\n        opts.VLine(\n            color="black",\n            line_dash="dashed",\n            line_alpha=0.5,\n            toolbar=None,\n        ),\n    )\n    .cols(1)\n)\n# definemp.update()\n\n\n# ### Milepost Plot\n\n\n# CURVE showing WSE at location over Time\n\n\ndef milepost_profile(tap_x, timestep, plan_name):\n\n    closest_milepost = CON.execute(\n        """\n    SELECT Milepost\n    FROM wse\n    ORDER BY ABS(Milepost - $x)\n    LIMIT 1\n""",\n        dict(x=tap_x),\n    ).fetchall()[0][0]\n\n    # closest_milepost = 250.00\n    wse_points = CON.execute(\n        "SELECT Timestep, WSE, plan_name FROM wse WHERE Timestep = $t AND plan_name IN $p AND Milepost = $m",\n        {"t": timestep, "p": plan_name, "m": closest_milepost},\n    ).fetch_df()\n\n    wse_curve = CON.execute(\n        """SELECT \n        Timestep, \n        WSE, \n        plan_name \n        FROM wse \n        WHERE plan_name IN $p AND Milepost = $m AND Timestep <= $max_t ORDER BY Timestep""",\n        {"p": plan_name, "m": closest_milepost, "max_t": max_t},\n    ).fetch_df()\n    scatter = (\n        hv.Scatter(wse_points, "Timestep", ["WSE", "plan_name"])\n        .groupby("plan_name")\n        .overlay("plan_name")\n    )\n    curve = (\n        hv.Curve(wse_curve, "Timestep", ["WSE", "plan_name"])\n        .groupby("plan_name")\n        .overlay("plan_name")\n    )\n\n    return (\n        (curve * scatter)\n        .redim(WSE="wse")\n        .relabel(f"Milepost: {closest_milepost:.2f}\\nTimestep: {timestep}")\n        .opts(\n            opts.Scatter(\n                marker="s",\n                size=14,\n                width=500,\n                alpha=0.9,\n                show_legend=False,\n                # xticks=[(0, 'zero'), (50, 'fifty'), (100, 'one hundred')]\n                # xrotation=45,\n                xticks=0,\n                xlabel="",\n                toolbar=None,\n            ),\n            opts.Curve(\n                show_legend=False,\n                toolbar=None,\n                # xrotation=45,\n                # xticks=5,\n            ),\n        )\n    )\n\n\ndetail = hv.DynamicMap(milepost_profile, streams=streams)\n\n# hyeto_plot = hv.DynamicMap(hyetograph, streams=dict(storm_name=STORM_WIDGET.param.value))\n\n\n# ### Storm Plots\n\n\n\ndef add_dot(storm, timestep):\n\n    filtered = storm_data.loc[\n        (storm_data["Storm"] == storm) & (storm_data["Timestep"] == timestep), :\n    ]\n\n    return hv.Scatter(filtered, "Timestep", "Accumulation").opts(\n        marker="s", size=14, width=600, alpha=0.9, xticks=0\n    )\n\n\nhyeto_stream = dict(\n    timestep=TIMESTEP_WIDGET.param.value,\n)\n\nhyeto_list = [\n    (\n        ix,\n        (\n            hv.Bars(storm, "Timestep", "Rate").opts(ylabel="Rate", cticks=0, xticks=0)\n            * hv.Curve(storm, "Timestep", "Accumulation")\n            * hv.DynamicMap(\n                partial(add_dot, ix),\n                streams=hyeto_stream,\n            )\n        )\n        .redim(Rate=ix)\n        .relabel("Simulated Storm(s)")\n        .opts(\n            multi_y=True,\n            shared_axes=False,\n            width=540,\n            xticks=0,\n            xlabel="",\n            toolbar=None,\n        ),\n    )\n    for ix, storm in storm_data.sort_values("Timestep").groupby("Storm")\n]\n\nhyeto_tabs = pn.Tabs(*hyeto_list, tabs_location="right", dynamic=True)\n\n\n\ntemplate = pn.template.BootstrapTemplate(title="17-Mile Pool Hydrosystem")\ntemplate.sidebar.append(PLAN_WIDGET)\n#template.sidebar.append(TIMESTEP_WIDGET)\n\ntemplate.main.append(pn.Row(profile_panel, pn.Column(detail, hyeto_tabs, pn.Row(pn.Spacer(width=40), TIMESTEP_WIDGET))))\ntemplate.servable()\n\n\ntemplate\n\n\nawait write_doc()
  `

  try {
    const [docs_json, render_items, root_ids] = await self.pyodide.runPythonAsync(code)
    self.postMessage({
      type: 'render',
      docs_json: docs_json,
      render_items: render_items,
      root_ids: root_ids
    })
  } catch(e) {
    const traceback = `${e}`
    const tblines = traceback.split('\n')
    self.postMessage({
      type: 'status',
      msg: tblines[tblines.length-2]
    });
    throw e
  }
}

self.onmessage = async (event) => {
  const msg = event.data
  if (msg.type === 'rendered') {
    self.pyodide.runPythonAsync(`
    from panel.io.state import state
    from panel.io.pyodide import _link_docs_worker

    _link_docs_worker(state.curdoc, sendPatch, setter='js')
    `)
  } else if (msg.type === 'patch') {
    self.pyodide.globals.set('patch', msg.patch)
    self.pyodide.runPythonAsync(`
    from panel.io.pyodide import _convert_json_patch
    state.curdoc.apply_json_patch(_convert_json_patch(patch), setter='js')
    `)
    self.postMessage({type: 'idle'})
  } else if (msg.type === 'location') {
    self.pyodide.globals.set('location', msg.location)
    self.pyodide.runPythonAsync(`
    import json
    from panel.io.state import state
    from panel.util import edit_readonly
    if state.location:
        loc_data = json.loads(location)
        with edit_readonly(state.location):
            state.location.param.update({
                k: v for k, v in loc_data.items() if k in state.location.param
            })
    `)
  }
}

startApplication()