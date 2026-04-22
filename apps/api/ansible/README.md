# HireTrack Sync API — Ansible

Idempotent provisioning for the Windows host that runs the HireTrack ODBC Flask API.
Runs over SSH via Tailscale (no WinRM, no RDP).

## What gets installed

| Role            | What it does                                                                  |
|-----------------|-------------------------------------------------------------------------------|
| `chocolatey`    | Ensures Chocolatey is present and current                                     |
| `python32`      | Installs Python 3.13 (x86) into `C:\Python313-32` — required by NexusDB ODBC  |
| `nssm`          | Installs NSSM (for wrapping Python as a Windows service)                      |
| `nexusdb_odbc`  | Downloads + installs the NexusDB ODBC driver, writes the 32-bit DSN registry  |
| `ssh_firewall`  | Locks SSH (22) and the API (5003) to `100.64.0.0/10` (Tailscale CGNAT)        |
| `hiretrack_api` | Deploys `apps/api/app.py`, pip-installs deps, registers the NSSM service      |

All modules used (`win_chocolatey`, `win_package`, `win_regedit`, `win_nssm`,
`win_service`, `win_firewall_rule`) are idempotent — re-running `site.yml` on an
already-provisioned box produces `changed=0` until something actually drifts.

## Prerequisites on your Mac

```bash
brew install ansible
# Python interpreter Ansible will use locally:
pipx install ansible-core

cd apps/api/ansible
ansible-galaxy collection install -r requirements.yml
```

SSH into the box once to accept host keys (Tailscale up on both sides):

```bash
ssh Admin@100.100.139.110 whoami
```

## One-time secrets setup

```bash
cp inventory/production/group_vars/windows/vault.yml.example \
   inventory/production/group_vars/windows/vault.yml

# edit real values, then encrypt
ansible-vault encrypt inventory/production/group_vars/windows/vault.yml
```

You will need a NexusDB ODBC driver installer URL (S3 works) plus:
- `sha256` of the MSI
- `product_id` GUID (inside the MSI — `msiexec /a foo.msi TARGETDIR=...` or
  `Get-Package` after a manual test install)

## Run

Full bootstrap (fresh box):
```bash
ansible-playbook playbooks/site.yml --ask-vault-pass
```

Redeploy just the app code after editing `apps/api/app.py`:
```bash
ansible-playbook playbooks/deploy.yml --ask-vault-pass
```

Dry run to see what would change:
```bash
ansible-playbook playbooks/site.yml --ask-vault-pass --check --diff
```

Smoke test:
```bash
curl http://100.100.139.110:5003/health
```

## Known gaps / follow-ups

1. **Pinned dependencies.** `apps/api/requirements.txt` now pins exact versions
   but isn't hash-locked. For stricter reproducibility, run
   `uv pip compile --generate-hashes requirements.in -o requirements.txt`
   and commit the lock; the role installs it as-is.
2. **Rotate secrets.** The plaintext values currently in the repo's `.env`
   (AWS keys, Tailscale auth key, Windows password) should be rotated — Ansible
   Vault is the only place they should live going forward.
3. **DB-level read-only user.** The `/api/query` endpoint filters keywords,
   but the real guarantee should come from `nx_db_user` having only `SELECT`
   grants in NexusDB. Fix at the database, not in Python.
