# Linux VPS Ansible

This scaffolds two playbooks for the Linux production host:

- `bootstrap.yml`
  Installs base packages, Docker Engine, Docker Compose plugin, and prepares the
  filesystem layout under `/opt/hiretrack-sync`.

- `deploy.yml`
  Syncs the repo to the VPS, renders `deploy/.env`, installs the `systemd`
  service/timer units, starts the long-running Compose services, and enables the
  sync timers.

## Structure

```text
deploy/ansible/
  ansible.cfg
  requirements.yml
  bootstrap.yml
  deploy.yml
  inventory/production/
  roles/
  templates/
```

## Usage

```bash
cd deploy/ansible
ansible-galaxy collection install -r requirements.yml
ansible-playbook bootstrap.yml
ansible-playbook deploy.yml
```

Edit `inventory/production/group_vars/linux/main.yml` and create
`inventory/production/group_vars/linux/vault.yml` from the example before
running `deploy.yml`.

