import quixlab as ql

canvas = ql.Canvas(title="My Notebook", lake_tree_open=['billing_events', 'billing_events/environment_id=testrigorg-ingestionpipelineforreal-6deb6d8f'])


@canvas.dataset(position=(365, 128), size=(706, 471), code_height=200)
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


if __name__ == "__main__":
    canvas.serve()
