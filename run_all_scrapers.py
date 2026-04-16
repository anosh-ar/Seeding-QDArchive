import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
SCRIPTS = (
    ROOT_DIR / "src" / "Scraper.py",
    ROOT_DIR / "src" / "IHSNScraper.py",
)


def run_script(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing script: {path}")
    print(f"\n==> Running {path.name}", flush=True)
    subprocess.run([sys.executable, str(path)], cwd=str(ROOT_DIR), check=True)


def main() -> None:
    for script in SCRIPTS:
        run_script(script)
    print("\nAll scrapers finished successfully.", flush=True)


if __name__ == "__main__":
    main()

