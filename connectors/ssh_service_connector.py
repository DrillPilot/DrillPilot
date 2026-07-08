"""
SSH-based service connector.

Works the same whether the orchestrator itself runs on Linux or Windows --
paramiko is cross-platform. The one thing that had to change moving to the
Windows orchestrator: key_file path expansion. The old version hardcoded
a Linux-only substitution ("~" -> "/root"); os.path.expanduser() below
handles both a Windows user profile path and /home/<you> or /root on
Linux correctly, so config.yaml's key_file value can just use "~" on
either OS.
"""

import os
import paramiko
import yaml

CONFIG_PATH = "config.yaml"


def _load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def _update_roles(primary_name: str):
    cfg = _load_config()

    for db in cfg["databases"]:
        if db["name"] == primary_name:
            db["role"] = "primary"
        else:
            db["role"] = "secondary"

    _save_config(cfg)


def _find_target(name: str, cfg: dict) -> dict:
    for entry in cfg.get("app_servers", []) + cfg.get("databases", []):
        if entry["name"] == name:
            return entry
    raise ValueError(f"Target '{name}' not found in config.yaml")


def _connect(host: str, cfg: dict) -> paramiko.SSHClient:
    ssh_cfg = cfg["ssh"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    key_path = os.path.expanduser(ssh_cfg["key_file"])

    client.connect(
        hostname=host,
        username=ssh_cfg["user"],
        key_filename=key_path,
        timeout=10,
    )

    return client


def _run_mysql(client, sql: str):
    cmd = f'''mysql -uroot -e "{sql}"'''
    stdin, stdout, stderr = client.exec_command(cmd)

    rc = stdout.channel.recv_exit_status()
    out = stdout.read().decode()
    err = stderr.read().decode()

    if rc != 0:
        raise RuntimeError(err.strip())

    return out


def service_action(target_name: str, verb: str) -> str:
    cfg = _load_config()
    target = _find_target(target_name, cfg)
    client = _connect(target["ip"], cfg)

    try:

        #
        # WEB SERVER
        #
        if verb in ("start", "stop"):

            service = target["service"]
            cmd = f"sudo systemctl {verb} {service}"

            stdin, stdout, stderr = client.exec_command(cmd)

            rc = stdout.channel.recv_exit_status()
            err = stderr.read().decode().strip()

            if rc != 0:
                raise RuntimeError(err)

            return f"{verb} succeeded on {target_name}"

        #
        # MYSQL REPLICATION HEALTH
        #
        elif verb == "check_replication":

            out = _run_mysql(client, "SHOW REPLICA STATUS\\G")

            if (
                "Replica_IO_Running: Yes" in out
                and "Replica_SQL_Running: Yes" in out
                and "Seconds_Behind_Source: 0" in out
            ):
                return "Replication Healthy"

            raise RuntimeError("Replication not healthy")

        #
        # PROMOTE
        #
        elif verb == "promote":

            _run_mysql(
                client,
                """
STOP REPLICA;
SET GLOBAL super_read_only=OFF;
SET GLOBAL read_only=OFF;
""",
            )

            _update_roles(target_name)

            return f"{target_name} promoted"

        #
        # DEMOTE
        #
        elif verb == "demote":

            _run_mysql(
                client,
                """
SET GLOBAL read_only=ON;
SET GLOBAL super_read_only=ON;
""",
            )

            return f"{target_name} demoted"

        #
        # REVERSE REPLICATION
        #
        elif verb == "reverse_sync":

            peer = None

            for db in cfg["databases"]:
                if db["name"] != target_name:
                    peer = db

            sql = f"""
STOP REPLICA;
CHANGE REPLICATION SOURCE TO
SOURCE_HOST='{peer["ip"]}',
SOURCE_USER='repl',
SOURCE_PASSWORD='Repl@123',
SOURCE_AUTO_POSITION=1,
GET_SOURCE_PUBLIC_KEY=1;
START REPLICA;
"""

            _run_mysql(client, sql)

            return "Reverse replication configured"

        else:
            raise RuntimeError(f"Unknown action {verb}")

    finally:
        client.close()


def check_status(target_name: str):
    """Returns True if the service is active, False if not, raises if unreachable."""
    cfg = _load_config()
    target = _find_target(target_name, cfg)

    if target.get("type") == "mysql":
        return target.get("role")

    client = _connect(target["ip"], cfg)

    try:
        service = target["service"]
        stdin, stdout, stderr = client.exec_command(
            f"systemctl is-active {service}"
        )
        result = stdout.read().decode().strip()
        return result == "active"

    finally:
        client.close()