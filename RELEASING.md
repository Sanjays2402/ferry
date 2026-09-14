# Releasing Ferry

Releases are cut from tags. Pushing a tag like `v0.1.0` fires `.github/workflows/release.yml`, which builds the sdist + wheel, smoke-tests the installed wheel in a clean venv, and publishes a GitHub Release with the artifacts attached.

```bash
# 1. bump the version in pyproject.toml and add a CHANGELOG entry
# 2. commit, push to main
# 3. tag and push the tag
git tag v0.1.0
git push origin v0.1.0
```

## Publishing to PyPI (one-time setup)

PyPI publishing is intentionally not wired up yet — it needs a one-time trusted-publisher setup that only the maintainer can do:

1. Create an account at [pypi.org](https://pypi.org) (if needed).
2. Go to **Account settings → Publishing → Add a new pending publisher** with:
   - PyPI project name: `ferry`
   - Owner: `Sanjays2402`
   - Repository: `ferry`
   - Workflow name: `release.yml`
   - Environment name: `pypi`
3. In this repo: **Settings → Environments → New environment** named `pypi` (no reviewers needed, or add yourself).
4. Add the publish job to `.github/workflows/release.yml`:

```yaml
  publish-pypi:
    needs: release
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist
      - uses: pypa/gh-action-pypi-publish@release/v1
```

   (and add an `upload-artifact` step for `dist/*` to the `release` job so the artifacts carry over).

After that, every version tag publishes to PyPI automatically — no tokens to rotate, ever.
