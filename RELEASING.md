# Release on GitHub

This example releases `ddp-connectors 0.2.0` using `ddp-lib 0.2.0`.
Publish the compatible ddp-lib version first. You need Git, GitHub CLI (`gh`),
and access to both repositories.

1. Update the version in `pyproject.toml` and `tests/test_imports.py`.
   Update `CHANGELOG.md`. Merge to `main` after the Python 3.10–3.12 checks pass.

2. From a clean checkout of that commit, test and build:

   ```sh
   python -m pip install build twine
   python scripts/smoke_install.py --published-dependency 0.2.0
   python -m build
   python -m twine check --strict dist/*
   ```

3. Make sure `dist/` contains only this release's files. The package version and
   Git tag must match. Publish to GitHub:

   ```sh
   git tag -a 0.2.0 -m "Release 0.2.0"
   git push origin 0.2.0
   gh release create 0.2.0 dist/* --repo BerexiaDev/ddp-connectors --verify-tag --title "0.2.0" --notes-file CHANGELOG.md
   ```

   Never change a published tag or replace its files with a different build.

4. Install both releases in a new virtual environment:

   ```sh
   python -m pip install \
     'ddp-lib @ git+https://github.com/BerexiaDev/ddp-lib.git@refs/tags/0.2.0' \
     'ddp-connectors @ git+https://github.com/BerexiaDev/ddp-connectors.git@refs/tags/0.2.0'
   python -m pip check
   ```

   Run the import tests outside the checkout. Then update Core to use these
   release URLs and check that it works with its databases.
