# Linux VPS Ansible

This Ansible scaffold is now bootstrap-only for the Linux production host.

- `bootstrap.yml`
  Installs base packages, Docker Engine, Docker Compose plugin, Tailscale, and
  prepares the filesystem layout under `deploy_root` (default
  `/opt/bi-stageaudio`, configurable in
  `inventory/production/group_vars/linux/main.yml`).

The bootstrap creates `{{ deploy_root }}/deploy`, which must equal `DEPLOY_DIR`
in `deploy/.env.production`. Keep those two in sync.

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

The bootstrap playbook prompts for a one-time Tailscale auth key. Leave it
blank on later runs if the VPS is already authenticated.

Runtime deployment is now managed directly from `deploy/docker-compose.yml` and
`deploy/.env`, not from Ansible group vars or templates.
