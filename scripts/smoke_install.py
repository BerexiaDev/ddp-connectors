"""Build and test wheel and sdist in fresh environments.

This script needs:
- build
- twine
"""

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile

GITHUB_REPO_URL = "https://github.com/BerexiaDev/{project}.git"


def run(*args, cwd):
    """Helper: run one terminal command.
    Args:
        *args: The command to run.
        cwd: The folder where the command runs.
    """
    # check=True = if the command fails, stop the script and show the error.
    subprocess.run([str(arg) for arg in args], cwd=cwd, check=True)


def main():
    # ---------- STEP 1: read the command-line options ----------
    parser = argparse.ArgumentParser(description=__doc__)

    # Which Python to test with.
    # Default: the same Python that runs this script.
    # Example: python scripts/smoke_install.py --python python3.11
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Runtime to test",
    )

    # These two options cannot be used together:
    #   1. use ddp-lib from a local folder
    #   2. use a published ddp-lib tag from GitHub
    dependency = parser.add_mutually_exclusive_group()

    # Option 1: use a local ddp-lib folder.
    # Example: python scripts/smoke_install.py --dependency ../ddp-lib
    dependency.add_argument(
        "--dependency",
        type=Path,
        help="Build a ddp-lib checkout "
             "(default: use sibling ../ddp-lib when present)",
    )

    # Option 2: build ddp-lib from a published GitHub tag.
    # Example: python scripts/smoke_install.py --published-dependency 0.2.0
    dependency.add_argument(
        "--published-dependency",
        metavar="TAG",
        help="Build ddp-lib from a release tag in BerexiaDev/ddp-lib on GitHub, "
             "ignoring the sibling checkout",
    )

    # Read what the user typed in the terminal.
    args = parser.parse_args()

    # ---------- STEP 2: find the project folders ----------

    # __file__ = ddp-connectors/scripts/smoke_install.py
    # parents[1] = ddp-connectors/   (the project root)
    project = Path(__file__).resolve().parents[1]

    # Look for ddp-lib in the folder next to ddp-connectors.
    sibling = project.parent / "ddp-lib"

    # If the user did not choose anything,
    # and ../ddp-lib exists, use it automatically.
    if (
        not args.dependency
        and not args.published_dependency
        and (sibling / "pyproject.toml").is_file()
    ):
        args.dependency = sibling

    # ---------- STEP 3: check the dependency source ----------
    if args.dependency:
        args.dependency = args.dependency.resolve()

        # A Python project must have a pyproject.toml file.
        if not (args.dependency / "pyproject.toml").is_file():
            parser.error(
                f"No pyproject.toml found in dependency checkout: "
                f"{args.dependency}"
            )

        print(
            f"Building local ddp-lib dependency: {args.dependency}",
            flush=True,
        )

    else:
        # A GitHub release must be selected explicitly when there is no checkout.
        if not args.published_dependency:
            parser.error(
                "No sibling ddp-lib checkout found. Use --dependency /path/to/ddp-lib "
                "or --published-dependency TAG to select a GitHub release."
            )
        print(
            f"Building ddp-lib from GitHub tag: {args.published_dependency}",
            flush=True,
        )

    # ---------- STEP 4: make a temporary work folder ----------
    # It is deleted automatically when the "with" block ends.
    with tempfile.TemporaryDirectory(prefix="ddp-smoke-") as directory:
        work = Path(directory)

        # Folder for the files we build (wheel and sdist).
        dist = work / "dist"

        # Folder for local dependency files (like the ddp-lib wheel).
        links = work / "dependencies"
        links.mkdir()

        # ---------- STEP 5: build the selected ddp-lib version ----------
        if args.dependency:
            run(
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--outdir",
                links,     # put the result in dependencies/
                args.dependency.resolve(),
                cwd=work,
            )
        else:
            # --branch works with a branch name OR a tag name
            checkout = work / "ddp-lib-src"
            run(
                "git", "clone", "--quiet", "--depth", "1",
                "--branch", args.published_dependency,
                GITHUB_REPO_URL.format(project="ddp-lib"),
                checkout,
                cwd=work,
            )
            run(
                sys.executable, "-m", "build", "--wheel",
                "--outdir", links,
                checkout,
                cwd=work,
            )

        dependency_wheels = list(links.glob("*.whl"))
        if len(dependency_wheels) != 1:
            raise RuntimeError("Expected exactly one ddp-lib wheel")

        # ---------- STEP 6: build ddp-connectors ----------
        # This makes two files:
        #   1. sdist (source package):  ddp_connectors-0.2.0.tar.gz
        #   2. wheel (ready to install): ddp_connectors-0.2.0-py3-none-any.whl
        run(
            sys.executable,
            "-m",
            "build",
            "--outdir",
            dist,
            project,
            cwd=work,
        )

        # List the files inside dist/.
        artifacts = sorted(dist.iterdir())

        # We expect exactly 2 files: one wheel and one sdist.
        # If not, something is wrong.
        if len(artifacts) != 2:
            raise RuntimeError(
                "Expected exactly one wheel and one sdist"
            )

        # ---------- STEP 7: check the package information ----------
        # twine checks that the metadata and README are correct.
        # It does NOT upload anything.
        # --strict = treat warnings as errors.
        run(
            sys.executable,
            "-m",
            "twine",
            "check",
            "--strict",
            *artifacts,
            cwd=work,
        )

        # ---------- STEP 8: test each file in a clean environment ----------
        # Loop 1 tests the first file, loop 2 tests the second file
        # (one is the wheel, the other is the sdist).
        for index, artifact in enumerate(artifacts):

            # Make a brand new virtual environment (a clean, empty Python).
            env = work / f"runtime-{index}"

            run(
                args.python,
                "-m",
                "venv",
                env,
                cwd=work,
            )

            # Find the Python program inside the new environment.
            # Windows: runtime-0/Scripts/python.exe
            # Linux/macOS: runtime-0/bin/python
            python = env / (
                "Scripts/python.exe"
                if os.name == "nt"   # "nt" means Windows
                else "bin/python"
            )

            # Install the package (wheel or sdist) in the clean environment.
            # Install the selected ddp-lib wheel explicitly, so pip cannot
            # substitute another version from a package index. Its version must
            # still satisfy the ddp-lib range declared by ddp-connectors.
            run(
                python,
                "-m",
                "pip",
                "install",
                artifact,
                *dependency_wheels,
                cwd=work,
            )

            # Check that all installed libraries work well together.
            # Good result: "No broken requirements found."
            run(
                python,
                "-m",
                "pip",
                "check",
                cwd=work,
            )

            # Run the tests inside the clean environment.
            # -I = isolated mode: ignore our own folders and settings,
            # so Python uses only the package installed in this clean venv.
            run(
                python,
                "-I",
                "-m",
                "unittest",
                "discover",
                "-v",                   # show detailed output
                "-s",
                project / "tests",      # folder where the tests are
                cwd=work,
            )

            # Show all installed libraries and their versions.
            run(
                python,
                "-m",
                "pip",
                "freeze",
                cwd=work,
            )

        # If we reach this line, both the wheel and the sdist passed.
        print(
            "Wheel and sdist clean-install checks passed",
            flush=True,
        )


if __name__ == "__main__":
    main()
