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


@canvas.datastore(position=(1303, 90), size=(560, 420), code_height=120, viz={'datastore': True, 'sourceNode': 'ai_1'})
def ai_1_store(ai_1):
    return ql.datastore("ai_1_store")


@canvas.cell(position=(2128, 231), size=(560, 420), code_height=200)
def cell_1(billing_events):
    return billing_events


@canvas.cell(position=(2811, 999), size=(560, 420), code_height=200, viz={'type': 'line', 'x': 'n', 'y': ['fibonacci']})
def fibonacci():
    n = 20
    fib = [0, 1]
    for i in range(2, n):
        fib.append(fib[-1] + fib[-2])
    pd.DataFrame({"n": range(n), "fibonacci": fib})


if __name__ == "__main__":
    canvas.serve()
