import sys
from pathlib import Path

# Les modules applicatifs vivent dans apps/ (ils y sont exécutés tels quels dans l'image).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps"))
