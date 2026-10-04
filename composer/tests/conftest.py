import pathlib
import sys

# Composer puts the dags/ folder on sys.path; mirror that for local tests.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
