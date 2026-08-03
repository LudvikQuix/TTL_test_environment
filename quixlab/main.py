import quixlab as ql

canvas = ql.Canvas(title="My Notebook", lake_tree_open=['billing_events', 'billing_events/environment_id=testrigorg-ingestionpipelineforreal-6deb6d8f'])


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


if __name__ == "__main__":
    canvas.serve()
