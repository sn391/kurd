# Kurd v0.3.0 Release Checklist

## Metadata

- [ ] `Cargo.toml` version is `0.3.0`
- [ ] `pyproject.toml` version is `0.3.0`
- [ ] `README.md` reflects beta / production-hardening status
- [ ] Python requirement is `>=3.10`
- [ ] MIT license metadata is present
- [ ] Repository URLs are correct

## Build Validation

Run:

```powershell
cargo check
maturin develop --release
python -m pytest -q
python -m pytest tests/test_load.py -q -s
```

Expected:

- `cargo check` succeeds
- `maturin develop --release` succeeds
- `48 passed`
- `3 passed` in `tests/test_load.py`

## MCP Interoperability

Start the Kurd interop server:

```powershell
python .\interop_server.py
```

Run the official MCP Python client test:

```powershell
python .\interop_client.py
```

Expected:

```text
protocol: 2026-07-28
tools: ['add']
INTEROP PASS
```

## Package Build

```powershell
maturin build --release
```

Confirm a fresh wheel is created under:

```text
target\wheels\
```

## Git

```powershell
git status
git add .
git commit -m "release: v0.3.0"
git tag -a v0.3.0 -m "Kurd v0.3.0"
git push origin main
git push origin v0.3.0
```

## PyPI

The `v0.3.0` tag should trigger the existing GitHub Actions release workflow and publish through the configured PyPI Trusted Publisher.

Verify after publication:

```powershell
python -m pip install --upgrade kurd==0.3.0
python -c "import kurd; print(kurd)"
```

## Final Verification

- [ ] GitHub Actions CI green
- [ ] release workflow green
- [ ] PyPI shows `0.3.0`
- [ ] Windows wheel installs
- [ ] Linux wheel produced
- [ ] macOS wheel produced
- [ ] source distribution produced
- [ ] README renders correctly on GitHub
- [ ] changelog included in repository
- [ ] security policy included in repository
