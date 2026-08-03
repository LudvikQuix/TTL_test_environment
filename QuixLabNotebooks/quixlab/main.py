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


@canvas.ai(position=(1171, 695), size=(573, 470), code_height=200)
def ai_2(billing_events):
    """Aggregate different credit_types. Calculate sum, average, min and max. Calculate this for each day."""
    # ql-ai: generated from prompt 0dbedf70882143e8
    import pandas as pd

    df = billing_events.copy()
    df["event_datetime"] = pd.to_datetime(df["event_datetime"])
    df["day"] = df["event_datetime"].dt.date

    result = (
        df.groupby(["day", "credit_type"])["duration_ms"]
        .agg(sum="sum", average="mean", min="min", max="max")
        .reset_index()
        .sort_values(["day", "credit_type"])
        .reset_index(drop=True)
    )

    result


@canvas.datastore(position=(1751, 128), size=(560, 420), code_height=120, viz={'datastore': True, 'sourceNode': 'ai_1'})
def ai_1_store(ai_1):
    return ql.datastore("ai_1_store")


if __name__ == "__main__":
    canvas.serve()
