import quixlab as ql

canvas = ql.Canvas(title="My Notebook", lake_tree_open=['billing_events', 'billing_events/environment_id=testrigorg-ingestionpipelineforreal-6deb6d8f'])


@canvas.dataset(position=(365, 128), size=(712, 651), code_height=200)
def billing_events():
    return ql.sql("""SELECT *
    FROM billing_events
    WHERE environment_id = 'testrigorg-ingestionpipelineforreal-6deb6d8f'
    ORDER BY event_datetime
    """)


@canvas.ai(position=(1131, 128), size=(560, 420), code_height=200, viz={'aiMode': 'agent'})
def ai_1(billing_events):
    """Analyse this billing data please"""
    # ql-ai-mode: agent


@canvas.ai(position=(1171, 695), size=(573, 470), code_height=200)
def ai_2(billing_events):
    """Aggregate different credit_types. Calculate sum, average, min and max. Calculate this for each day."""
    # ql-ai: generated from prompt 0dbedf70882143e8
    df = billing_events.copy()

    # Parse event_datetime robustly (ISO8601 with timezone offset)
    df['event_datetime'] = pd.to_datetime(df['event_datetime'], utc=True, errors='coerce')
    df = df.dropna(subset=['event_datetime'])

    df['day'] = df['event_datetime'].dt.date

    agg = (
        df.groupby(['day', 'credit_type'])['duration_ms']
        .agg(sum='sum', average='mean', min='min', max='max')
        .reset_index()
        .sort_values(['day', 'credit_type'])
    )

    agg


if __name__ == "__main__":
    canvas.serve()
