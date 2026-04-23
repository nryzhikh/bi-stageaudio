# Linux VPS Ansible

This Ansible scaffold is now bootstrap-only for the Linux production host.

- `bootstrap.yml`
  Installs base packages, Docker Engine, Docker Compose plugin, and prepares the
  filesystem layout under `/opt/hiretrack-sync`.

## Structure

```text
deploy/ansible/
  ansible.cfg
  requirements.yml
  bootstrap.yml
  inventory/production/
  roles/
```

## Usage

```bash
cd deploy/ansible
ansible-galaxy collection install -r requirements.yml
ansible-playbook bootstrap.yml
```

Runtime deployment is now managed directly from `deploy/docker-compose.yml` and
`deploy/.env`, not from Ansible group vars or templates.
