"""Build wheel/sdist, then install and test each in a fresh virtual environment."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def run(*args, cwd, env=None):
    subprocess.run([str(arg) for arg in args], cwd=cwd, env=env, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default=sys.executable, help="Python runtime to test")
    parser.add_argument("--constraints", type=Path, action="append", default=[])
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    constraints = []
    for path in args.constraints:
        constraints.extend(["--constraint", str(path.resolve())])

    with tempfile.TemporaryDirectory(prefix="ddp-smoke-") as directory:
        work = Path(directory)
        dist = work / "dist"
        # The default build makes a wheel FROM the sdist, checking source completeness.
        run(sys.executable, "-m", "build", "--outdir", dist, project, cwd=work)
        artifacts = sorted(dist.iterdir())
        if len(artifacts) != 2 or {path.suffix for path in artifacts} != {".whl", ".gz"}:
            raise RuntimeError("Expected exactly one wheel and one sdist")
        run(sys.executable, "-m", "twine", "check", "--strict", *artifacts, cwd=work)
        for index, artifact in enumerate(artifacts):
            runtime = work / f"runtime-{index}"
            run(args.python, "-m", "venv", runtime, cwd=work)
            python = runtime / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            run(python, "-m", "pip", "install", "--no-compile", *constraints, artifact, cwd=work)
            run(python, "-m", "pip", "check", cwd=work)
            # -I ignores PYTHONPATH and cwd; test_imports also verifies installed origin.
            env = {**os.environ, "DDP_INSTALL_SMOKE": "1"}
            run(python, "-I", "-m", "unittest", "discover", "-v", "-s",
                project / "tests", cwd=work, env=env)
            run(python, "-m", "pip", "freeze", cwd=work)
            print(f"PASS: clean install and contracts for {artifact.name}", flush=True)


if __name__ == "__main__":
    main()
