from datapackage_pipelines.wrapper import process

def process_row(row, *args):
    for k, v in row.items():
        if isinstance(v, str):
            row[k] = v.capitalize()
    return row

process(process_row=process_row)
