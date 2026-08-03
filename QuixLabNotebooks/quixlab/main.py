import quixlab as ql

canvas = ql.Canvas(title="My Notebook", lake_tree_open=['billing_events', 'billing_events/environment_id=testrigorg-ingestionpipelineforreal-6deb6d8f'], markups=[{'id': 'markup_d9c232a0', 'text': '**`credit_type_summary`** — Event count, total ms, and average ms per credit type.\n\n`quixlab-dataset` is the most expensive per-call (avg ~5,085ms) despite being the least frequent (616 events) — SQL query execution dominates cost. `quixlab-cell` fires most often (1,481 events) but is cheap per-call (avg ~1,033ms). `quixlab-notebook` sits in between (424 events, avg ~1,721ms).', 'x': 2221, 'y': 773, 'w': 280, 'h': 200, 'rendered': True, 'linkedTo': ''}, {'id': 'markup_37cc6123', 'text': '**`daily_cost_by_type`** — Daily compute time (ms), split by credit type.\n\nUsage is bursty, not steady: **2026-07-28** is the heaviest single day (~881k ms dataset + ~690k ms cell), and **2026-07-24** spikes due to unusually costly cell runs (754k ms across 242 events, vs. the ~1k ms/event average elsewhere). Several days (07-18, 07-19, 07-25, 07-26) show zero activity — likely inactive/weekend gaps.', 'x': 3483, 'y': 370, 'w': 280, 'h': 200, 'rendered': True, 'linkedTo': ''}, {'id': 'markup_3bf22722', 'text': '**`top_deployments_by_cost`** — Top 15 deployments ranked by total compute ms consumed.\n\nTwo deployments dominate: `1b357b9e-a0f0-4244-bc77-ec69fe7808c6` (576 events, 2.77M ms) and `18b45df4-6e0b-4777-9267-36126c7fd9b7` (174 events, 1.27M ms) together account for over half of all compute time in the period. Worth checking if these are legitimately heavy notebooks or candidates for optimization (query limits, caching, reduced run frequency).', 'x': 4083, 'y': 370, 'w': 280, 'h': 200, 'rendered': True, 'linkedTo': ''}])


@canvas.dataset(position=(365, 128), size=(712, 651), code_height=200)
def billing_events():
    return ql.sql("""SELECT *
    FROM billing_events
    WHERE environment_id = 'testrigorg-ingestionpipelineforreal-6deb6d8f'
    ORDER BY event_datetime
    """)


    # ql-ai-mode: agent


@canvas.datastore(position=(1486, -784), size=(560, 420), code_height=120, viz={'datastore': True, 'sourceNode': 'ai_1'})
def ai_1_store(ai_1):
    return ql.datastore("ai_1_store")


@canvas.cell(position=(1434, 685), size=(560, 420), code_height=200, viz={'type': 'table'})
def credit_type_summary(billing_events):
    summary = billing_events.groupby("credit_type").agg(
        events=("event_id", "count"),
        total_ms=("duration_ms", "sum"),
        avg_ms=("duration_ms", "mean"),
    ).reset_index()
    summary["avg_ms"] = summary["avg_ms"].round(1)
    return summary.sort_values("total_ms", ascending=False)


@canvas.cell(position=(1414, -41), size=(560, 420), code_height=200, viz={'type': 'bar', 'x': 'day', 'y': ['quixlab-dataset', 'quixlab-cell', 'quixlab-notebook']})
def daily_cost_by_type(billing_events):
    df = billing_events.copy()
    df["day"] = df["event_datetime"].str.slice(0, 10)
    pivot = df.pivot_table(index="day", columns="credit_type", values="duration_ms", aggfunc="sum", fill_value=0).reset_index()
    return pivot


@canvas.cell(position=(2634, 225), size=(560, 420), code_height=200, viz={'type': 'bar', 'x': 'deployment_id', 'y': 'total_ms'})
def top_deployments_by_cost(billing_events):
    df = billing_events.groupby("deployment_id", as_index=False)["duration_ms"].sum()
    df = df.rename(columns={"duration_ms": "total_ms"}).sort_values("total_ms", ascending=False).head(15)
    return df


@canvas.ai(position=(437, -707), size=(560, 420), code_height=200)
def ai_1(billing_events):
    """Plot credits burned per day per credit_type"""
    # ql-ai: generated from prompt 2fd5810498346b39
    import pandas as pd

    df = billing_events.copy()
    df["event_datetime"] = pd.to_datetime(df["event_datetime"])
    df["day"] = df["event_datetime"].dt.date.astype(str)

    daily = (
        df.groupby(["day", "credit_type"])["duration_ms"]
        .sum()
        .reset_index()
    )

    wide = daily.pivot_table(index="day", columns="credit_type", values="duration_ms", fill_value=0)
    wide = wide.sort_index().reset_index()

    ql.viz(wide, type="line", x="day", y=[c for c in wide.columns if c != "day"])


if __name__ == "__main__":
    canvas.serve()
