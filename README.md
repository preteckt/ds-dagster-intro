# ds-dagster-intro

This repo contains a simple [Dagster](https://dagster.io/) pipeline and some representative telematics data. It's used during interviews for data science and data engineering.

Before the interview, please install the project, run the pipeline from the Dagster UI, run the tests, and look through the notebook.

## Project layout

```
data/
  example_telematics_data.csv   # 1 vehicle: vehicle speed (every 5 s) and engine speed (every 1 s)
notebooks/
  explore_telematics.ipynb      # data exploration with time series plots
src/ds_dagster_intro/
  definitions.py                # loads everything in defs/
  defs/
    processing.py               # plain pandas functions (interpolation, stats)
    assets.py                   # Dagster assets: preprocessed_telematics -> telematics_summary
    schedules.py                # job + daily schedule (with a cron syntax cheat sheet)
tests/
  test_assets.py                # unit tests (unittest)
```

## Getting started

### Installing dependencies

Create a virtual environment (Python 3.10–3.14):

| OS | Command |
| --- | --- |
| MacOS / Linux | ```python3 -m venv .venv``` |
| Windows | ```py -m venv .venv``` |

Then activate it:

| OS | Command |
| --- | --- |
| MacOS / Linux | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the project and its dev dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser.

- **Materialize the assets:** go to **Assets** and open the `telematics` group (or click **View lineage**) to see the DAG. Click **Materialize all** to run both assets.
- **Monitor runs:** open the **Runs** tab to see each run's status, step timings and logs.
- **Inspect results:** click an asset to see the metadata it produced, such as NaN counts, a data preview, the `describe()` table and the correlation matrix.


- **Schedules:** `daily_telematics_schedule` runs `telematics_job` every day at 06:00 UTC. It starts turned off. Enable it from the **Automation** tab. See [schedules.py](src/ds_dagster_intro/defs/schedules.py) for a cron syntax reference.

You can also check and run the pipeline from the command line:

```bash
dg list defs          # list assets, jobs and schedules
dg launch --assets '*'
```

### Running the tests
Use the test explorer in vs code, or run:

```bash
python -m unittest discover tests
```

### Notebooks

To view and interact with the notebooks, install the jupyter extension in vs code and open the notebook, or run:

```bash
jupyter lab notebooks/
```

## Learn more

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
