import os


def main(*args, **kwargs):
    original_directory = os.getcwd()
    try:
        base_path = os.path.join(os.path.dirname(__file__), '..')
        os.chdir(os.path.join(base_path, 'projects'))

        # We import after setting env vars because these are read on import.
        from datapackage_pipelines.cli import cli
        cli(*args, **kwargs)
    finally:
        os.chdir(original_directory)
