# nssm role

Installs the [NSSM](https://nssm.cc/) service wrapper by deploying a
vendored binary from `files/` to the target Windows host. No network access
required at deploy time.

## Why vendored?

- `nssm.cc` is single-maintainer and periodically returns `530` (Cloudflare
  origin down). Vendoring decouples deploys from upstream availability.
- NSSM 2.24 is public domain and bitwise-stable since August 2014; there is
  no newer release worth fetching.
- A 350 KB binary under version control is the industry-standard trade-off
  for deployment determinism.

## Contents

```
files/
  nssm-2.24-win64.exe    ← deployed to {{ nssm_exe }} on the target
  CHECKSUMS.txt          ← SHA-256 for integrity verification
  README.txt             ← upstream docs (for reference)
  ChangeLog.txt          ← upstream changelog (for reference)
```

## Verifying integrity

Before trusting this binary, verify its SHA-256 against a second independent
source (e.g. Chocolatey's `nssm` package):

```bash
shasum -a 256 roles/nssm/files/nssm-2.24-win64.exe
# expected: f689ee9af94b00e9e3f0bb072b34caaf207f32dcb4f5782fc9ca351df9a06c97
```

## Variables

| Variable | Default | Purpose |
|---|---|---|
| `nssm_home` | `C:\Program Files\nssm` | Target directory on Windows. |
| `nssm_exe`  | `{{ nssm_home }}\nssm.exe` | Full path to deployed binary. |
| `nssm_local_binary` | `nssm-2.24-win64.exe` | Name of the file in `files/`. |

Override `nssm_home` in `group_vars/windows.yml` to colocate NSSM with your
application for isolated, uninstall-by-rmdir deployments.
