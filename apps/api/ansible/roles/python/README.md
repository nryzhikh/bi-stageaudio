# python role

Installs a fully isolated CPython interpreter from the official Python
**embeddable zip distribution**, with pip bootstrapped via `get-pip.py`.
No Windows Installer, no registry entries, no Add/Remove Programs entry.
Uninstall = `rmdir /s /q {{ python_home }}`.

## Why embeddable instead of the regular installer?

- **True isolation.** The MSI-based installer writes to the registry and
  Programs list regardless of `TargetDir`. Embeddable does not.
- **Portable.** The whole interpreter is a folder you can move, zip, copy.
- **Offline-capable.** No MSI runtime dependencies, no reboot prompts.
- **No .NET requirement** (unlike some regular-installer edge paths).

## Trade-offs (being honest)

- `venv` is **not** available in embeddable. The interpreter itself *is* the
  isolated environment. Application dependencies install directly into
  `{{ python_home }}\Lib\site-packages\`.
- `pip` is not shipped — we bootstrap it via `get-pip.py` (vendored under
  `files/`).
- Must overwrite `python313._pth` to enable `import site` so
  `Lib\site-packages` is added to `sys.path` at startup (done by this role).
- No Tkinter, IDLE, or HTML docs. Irrelevant for a headless API server.

## Contents

```
files/
  python-3.13.13-embed-win32.zip   ← official python.org embeddable
  python313._pth                    ← site-enabled path config we deploy
  get-pip.py                        ← official pip bootstrap script
  CHECKSUMS.txt                     ← SHA-256 for integrity verification
```

## Verifying integrity

```bash
shasum -a 256 roles/python/files/*.{zip,py}
# Expected values are in files/CHECKSUMS.txt.
# Cross-reference:
#   - https://www.python.org/downloads/release/python-31313/
#   - https://pip.pypa.io/en/stable/installation/#get-pip-py
```

## Variables

| Variable | Default | Purpose |
|---|---|---|
| `python_version`  | `3.13.13` | Informational / version check. |
| `python_home`     | `C:\Python313` | Target directory on Windows. |
| `python_exe`      | `{{ python_home }}\python.exe` | Full path to interpreter. |
| `python_embed_zip`| `python-3.13.13-embed-win32.zip` | Filename in `files/`. |

Override `python_home` in `group_vars/windows.yml` to colocate Python with
your application for full isolation.

## Why the x86 (32-bit) build?

This project talks to NexusDB through its ODBC driver, which is 32-bit only.
Loading a 32-bit ODBC driver requires a 32-bit Python process. Do not switch
to `embed-amd64.zip` unless you also replace NexusDB's driver with a 64-bit
equivalent (which does not exist).
