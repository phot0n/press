# ZFS Snapshot — Implementation Plan

ZFS-on-data-volumes snapshot/restore for providers without EBS-style disk snapshots
(Hetzner, etc.). Snapshots are `zfs send` streams uploaded to S3 by the agent.

**Decisions locked:** incremental + periodic full · both restore modes (new server +
in-place) · S3 transport via agent endpoint · pool name `frappe-zfs-pool` · raid-0
(striped, no redundancy) · data disk only (root disk untouched).

---

## 0. Concepts / invariants

- Pool: `frappe-zfs-pool`, striped over all data volumes (raid-0). `zpool add` to grow.
- Datasets:
  - Database Server: `frappe-zfs-pool/mariadb`, `mountpoint=/opt/volumes/mariadb`
  - App Server: `frappe-zfs-pool/benches`, `mountpoint=/opt/volumes/benches`
- Bind mounts unchanged from today (`/opt/volumes/mariadb/var/lib/mysql → /var/lib/mysql`,
  `/etc/mysql`; benches → `/home/frappe/benches`, `/var/lib/docker`).
- ZFS replaces the **ext4 + mkfs + UUID-in-fstab** layer only. Binds stay in fstab.
- `zfs send` is logical → restore is **topology-independent** (volume count/size/device
  names need not match; only constraint: target pool capacity ≥ used bytes).
- Incremental chain: each snapshot keeps `parent_snapshot`; the on-server base `@snap` is
  `zfs hold`-retained so the next `send -i` has a reference. Missing base ⇒ fall back to full.

---

## 1. `frappe/agent` repo (SEPARATE — spec to hand off)

Not in this tree. New flask routes mirroring the binlog-upload pattern. S3 creds arrive in
the `offsite` payload (`{bucket, auth:{ACCESS_KEY,SECRET_KEY,REGION}, path}`) exactly like
`/database/binlogs/upload`.

### `POST /server/zfs/snapshot`
```
body: { dataset, snapshot_name, base_snapshot|null, offsite }
steps:
  zfs snapshot <dataset>@<snapshot_name>
  if base_snapshot: STREAM = zfs send -i <dataset>@<base> <dataset>@<snapshot_name>
  else:             STREAM = zfs send         <dataset>@<snapshot_name>
  STREAM | zstd -T0 | <S3 multipart upload to s3://bucket/path/key>   # streaming multipart, size unknown upfront
  zfs hold press <dataset>@<snapshot_name>        # retain as next base
  zfs release press <dataset>@<old_base>          # free old base if rotating
returns: { s3_key, size_bytes, parent }
```

### `POST /server/zfs/restore`
```
body: { target_dataset, s3_keys:[full, inc1, inc2, ...], offsite }   # ordered chain
steps:
  for key in s3_keys (in order):
    aws s3 cp s3://bucket/path/key - | unzstd | zfs receive [-F] target_dataset
returns: { status }
```

### `POST /server/zfs/pool/destroy-dataset`  (in-place restore only)
```
body: { dataset }
steps: zfs destroy -r <dataset>     # after services stopped
```

> Pool *creation* is NOT an agent route — done by ansible at setup time (§3).

---

## 2. Config flags (press)

### `Cluster` — `cluster.json` + `cluster.py`
- Add Check `enable_zfs` (label "Enable ZFS for Data Volumes"). Master switch per region.

### `Server` — `server.json`, `Database Server` — `database_server.json`
- Add Check `enable_zfs`, default fetched from cluster but per-server overridable
  (no `fetch_from` — copy on insert so override sticks; set in `before_insert`/`autoname`
  from `frappe.db.get_value("Cluster", self.cluster, "enable_zfs")`).
- Validation in `validate()` (BaseServer, server.py): if `enable_zfs` and not
  `frappe.db.get_value("Virtual Machine", self.virtual_machine, "has_data_volume")`
  → `frappe.throw("ZFS requires a data volume")`.

Regenerate auto-typed blocks after JSON edits (the `# begin: auto-generated types`).

---

## 3. Pool setup at provisioning (press, ansible)

### New role `press/playbooks/roles/zfs_pool/tasks/main.yml`
```yaml
- name: Install zfsutils-linux        # apt, update_cache
- name: Discover data devices         # lsblk by volume_id, skip root (mirror server.py:1838 skip rule)
- name: Create zpool                   # zpool create -o autoexpand=on -f frappe-zfs-pool {{ data_devices }}  (creates: when pool absent)
- name: Create dataset                 # zfs create -o mountpoint={{ zfs_mountpoint }} frappe-zfs-pool/{{ zfs_dataset }}
- name: Set ownership of mountpoint
# bind mounts handled by existing mount tasks afterward
```
Idempotent: guard `zpool create` with `zpool list frappe-zfs-pool` check.

### `press/playbooks/server.yml` (+ `self_hosted.yml` if applicable)
- Include `zfs_pool` role before the existing mount/bind tasks, `when: enable_zfs | bool`.
- Keep existing ext4 path under `when: not enable_zfs`.

### `server.py` (BaseServer)
- `set_default_mount_points` (server.py:1846): when `enable_zfs`, emit **only the Bind
  mounts** (skip the `Volume`-type mount; ZFS auto-mounts the dataset). DB → mysql binds,
  App → benches/docker binds.
- `_setup_server` (server.py:3238) and `database_server.py` `_setup_server` (:821): add to
  `variables`: `enable_zfs`, `zfs_dataset` (`mariadb`/`benches`), `zfs_mountpoint`
  (`/opt/volumes/...`), and the data-device discovery inputs already available via
  `get_mount_variables`.
- `update_fstab_with_mounts` role: skip the `UUID=... ext4` line when `enable_zfs`
  (binds only). Gate with a var.

---

## 3a. Grow pool — add a volume (press, cloud API + ansible)

Two-step: attach blank volume via cloud-provider API, then `zpool add` via ansible.
Mirror the existing `AttachVolumeJob` (`press_job/jobs/attach_volume.py`) +
`VirtualMachine.attach_new_volume` (vm.py:2731, handles AWS/OCI/Hetzner).

### Mechanics
- `zpool add frappe-zfs-pool <newdev>`  — **not** `zpool attach` (attach = mirror).
  `add` appends a new top-level vdev → pool stripes new writes across it. Existing data
  stays put (no auto-rebalance; harmless).
- **One-way**: a top-level vdev can't be cleanly removed from a striped pool. Each added
  volume joins the SPOF set (still raid-0, no redundancy). Surface in UI confirm.
- Snapshot chain **unaffected** — `zfs send/recv` is logical; incrementals keep working
  across a grow. No new full forced.

### New Press Job `Add ZFS Volume`  `press_job/jobs/add_zfs_volume.py`
```python
@flow
def execute(self):
    self.attach_volume()        # cloud API: machine.attach_new_volume(size, iops, throughput)
    self.wait_for_volume()      # provider-specific available check (vm.wait_for_volume_to_be_available)
    self.sync_virtual_machine() # refresh machine.volumes so new device is known
    self.add_volume_to_pool()   # ansible: zpool add
```
- `add_volume_to_pool`: run new role `zfs_pool_extend` with the new volume_id.
- Register: fixture `press_job_type.json` (`Add ZFS Volume`, empty steps) +
  `press_job.py:77` `job_class_map`.

### New role `press/playbooks/roles/zfs_pool_extend/tasks/main.yml`
```yaml
- name: Resolve new device          # lsblk by volume_id -> /dev/...  (reuse server.py device-from-volume-id logic via var)
- name: Check device not already in pool   # zpool status frappe-zfs-pool | grep, fail-safe skip if present
- name: Add device to pool          # zpool add frappe-zfs-pool {{ new_device }}
- name: Verify                       # zpool list frappe-zfs-pool capacity grew
```
Idempotent: skip `zpool add` if device already a pool member.

### Trigger (press)
- `@dashboard_whitelist add_zfs_volume(size, iops=None, throughput=None)` on Server /
  Database Server → guards (`enable_zfs`, provider supports attach) → enqueue `Add ZFS
  Volume` press job. Pre-flight: provider in {AWS EC2, OCI, Hetzner}.

---

## 3b. Expand pool — volume resized at provider (press, cloud API + ansible)

When an existing data volume is grown at the provider (e.g. 50 GB EBS → 400 GB), ZFS must
be told to use the new space. Hook into the existing resize flow rather than a new path.

### Mechanics
- Pool created with `autoexpand=on` (§3). Even so, the reliable trigger is explicit:
  `zpool online -e frappe-zfs-pool <dev>` per resized device → expands the vdev to the
  device's new size; pool capacity grows immediately, no downtime, no data move.
- ZFS uses **whole unpartitioned disks** → **no `growpart`/`resize2fs`** (that's the ext4
  path). The ZFS branch skips both.
- Multiple resized vdevs → `zpool online -e` each.

### Reuse `IncreaseDiskSizeJob`  (`press_job/jobs/increase_disk_size.py`)
- `increase_disk_size` task: cloud API resize stays as-is
  (`calculated_increase_disk_size`).
- Branch the post-resize tail on `enable_zfs`:
  - **ext4 (today):** `extend_ec2_volume` role (`growpart` + `resize2fs`).
  - **ZFS (new):** run new role `zfs_pool_expand` instead → `zpool online -e`.
- `wait_for_partition_to_resize_for_aws_ec2` (:34) gates on the `Extend EC2 Volume` play;
  add a sibling gate on the new ZFS expand play (or generalize the play-name lookup).

### New role `press/playbooks/roles/zfs_pool_expand/tasks/main.yml`
```yaml
- name: Resolve resized device(s)     # lsblk by volume_id
- name: Expand vdev                    # zpool online -e frappe-zfs-pool {{ device }}
- name: Verify                         # zpool list frappe-zfs-pool -> SIZE grew
```
Idempotent: `zpool online -e` is a no-op if already at full size.

---

## 4. `ZFS Snapshot` doctype (press)

`press/press/doctype/zfs_snapshot/` — `.json`, `.py`, `.js`, `test_zfs_snapshot.py`,
`README.md`. Model on `server_snapshot.py` and `virtual_disk_snapshot`.

### Fields
```
server (Dynamic Link), server_type (Link: DocType, Server|Database Server)
cluster (Link)
dataset            Data        # frappe-zfs-pool/mariadb | /benches
snapshot_name      Data        # <iso-timestamp>
s3_bucket, s3_key  Data
snapshot_type      Select      # Full | Incremental
parent_snapshot    Link ZFS Snapshot   # null when Full
base_retained      Check       # on-server @snap still held
size               Int (bytes)
consistent         Check
locked             Check
expire_at          Datetime
status             Select      # Pending|Processing|Completed|Failure|Unavailable
agent_job          Link Agent Job
traceback          Text
```

### `zfs_snapshot.py` — `class ZFSSnapshot(Document)`
- `dashboard_fields`, `get_list_query`, `get_doc` — copy shape from `ServerSnapshot`.
- `validate()`: server has `enable_zfs`; VM in allowed status.
- `before_insert()`: resolve `dataset`/`mountpoint` from `server_type`; pick
  `snapshot_type` + `parent_snapshot`:
  - latest `Completed` snapshot for (server, dataset) with `base_retained=1` and age within
    `full_interval_days` → Incremental against it; else Full.
- `after_insert()`: `create_agent_snapshot_job()`.
- `create_agent_snapshot_job()`: build `dataset@snapshot_name`, resolve base from
  `parent_snapshot`, call new `Agent.zfs_snapshot(...)` (§6). Store `agent_job`.
- `@dashboard_whitelist restore_to_server(...)` → creates `ZFS Snapshot Recovery` (§7).
- `lock/unlock/delete_snapshot/_sync` — mirror ServerSnapshot (delete = remove S3 object +
  `zfs destroy` held snap via agent; status→Unavailable).
- subscription hooks (`_create_subscription`/`_disable_subscription`) — reuse
  `Server Snapshot Plan` or add `ZFS Snapshot Plan` (decide w/ billing; default: reuse).
- module funcs: `take_zfs_snapshots()`, `expire_snapshots()`, `sync_ongoing()`,
  `delete_failed()`.

### Restore chain helper
```python
def restore_chain(self) -> list[str]:
    """Walk parent_snapshot to nearest Full; return ordered [full_key, ...inc_keys]."""
```

---

## 5. `Press Job: ZFS Snapshot` (press)

> Snapshot orchestration is a **Press Job** (consistency stop/start of services), but the
> S3 transport itself is an **Agent Job** kicked from inside it. (Press Job = server-level
> orchestration; Agent Job = the long S3 stream, pollable.)

### `press/press/doctype/press_job/jobs/zfs_snapshot.py` — `class ZFSSnapshotJob(PressJob)`
Mirror `snapshot_disk.py`:
```python
@flow
def execute(self):
    self.verify_virtual_machine_status()
    if self.is_consistent_snapshot:
        stop docker (Server) / mariadb (Database Server)
    self.flush_file_system_buffers()      # sync
    self.create_zfs_snapshot_and_upload() # zfs snapshot is atomic+instant
    # services resume here (on_press_job_success); S3 send runs off the frozen snapshot
```
- `create_zfs_snapshot_and_upload`: triggers the agent snapshot job; the Agent Job callback
  (§6) flips `ZFS Snapshot.status` and records `size`/`s3_key`/`base_retained`.
- `on_press_job_success/failure`: resume services; on failure mark snapshot Failure + clean
  partial S3 object.

### Register
- `press/fixtures/press_job_type.json`: add `{"name":"ZFS Snapshot",...}` (empty steps,
  like "Snapshot Disk"; steps come from `@task`).
- `press_job.py:77` `job_class_map`: `"ZFS Snapshot": ZFSSnapshotJob` + import (:52 style).

---

## 6. Agent glue (press side)

### `press/agent.py` — new methods on `Agent` (mirror `upload_binlogs_to_s3` :1619)
```python
def zfs_snapshot(self, dataset, snapshot_name, base_snapshot, cluster):
    offsite = self._get_offsite_backup_config(cluster, backups_path=self.server)
    return self.create_agent_job("ZFS Snapshot", "/server/zfs/snapshot",
        data={"dataset": dataset, "snapshot_name": snapshot_name,
              "base_snapshot": base_snapshot, "offsite": offsite})

def zfs_restore(self, target_dataset, s3_keys, cluster):
    offsite = self._get_offsite_backup_config(cluster, backups_path=self.server)
    return self.create_agent_job("ZFS Restore", "/server/zfs/restore",
        data={"target_dataset": target_dataset, "s3_keys": s3_keys, "offsite": offsite})

def zfs_destroy_dataset(self, dataset):
    return self.create_agent_job("ZFS Destroy Dataset", "/server/zfs/pool/destroy-dataset",
        data={"dataset": dataset})
```

### `press/fixtures/agent_job_type.json`
Add `ZFS Snapshot` (`/server/zfs/snapshot`), `ZFS Restore` (`/server/zfs/restore`),
`ZFS Destroy Dataset` — with step rows (pattern at line 2672 "Upload Binlogs To S3").

### Callbacks
`press/press/doctype/agent_job/agent_job.py` (or `agent_job_callbacks` dispatcher): on
`ZFS Snapshot` success → set `ZFS Snapshot` `status=Completed`, `size`, `s3_key`,
`base_retained=1`; on failure → `Failure`. On `ZFS Restore` success → advance recovery (§7).

---

## 7. Restore — both modes

### `ZFS Snapshot Recovery` doctype  `press/press/doctype/zfs_snapshot_recovery/`
Model on `server_snapshot_recovery`. Fields: `snapshot` (Link), `mode`
(New Server | In-Place), `target_server`, `new_server`, `status`, `recovery_job`.

### `ZFSSnapshot.restore_to_server(mode, ...)`
- **New server**: provision VM with blank data volumes → `enable_zfs` → `zfs_pool` role
  creates empty pool over current volumes → `Agent.zfs_restore(dataset, restore_chain())`
  → bind mounts → activate. Capacity pre-flight: `sum(volume sizes) ≥ snapshot.size`,
  else throw.
- **In-place**: behind `Press Settings.enable_zfs_recovery` toggle + JS confirm dialog →
  stop docker/mariadb → `Agent.zfs_destroy_dataset(dataset)` →
  `Agent.zfs_restore(dataset, restore_chain())` → restart. Destructive.

### Capacity validation helper (press, no agent round-trip)
Sum attached data-volume sizes from `Virtual Machine.volumes` vs `snapshot.size`.

### Cross-VM / cross-provider portability
The "new server" target can be **any** VM — different provider, region, instance type,
volume count/size. The stream is logical, so this is the same code path; just don't pin
the target to the source's cluster. Enables Hetzner→AWS, region moves, clone-to-new-team.

Constraints to enforce/validate:
- **ZFS present + pool created** on target (the `zfs_pool` role handles it at setup).
- **Capacity** ≥ `snapshot.size` (pre-flight above).
- **ZFS feature-flag compat** — older-receiving-newer fails. Pin agent ZFS version in the
  role so streams stay receivable fleet-wide.
- **S3 creds resolve against the snapshot's OWN bucket**, not the target cluster default.
  ⚠️ `_get_offsite_backup_config` currently resolves by target cluster — for cross-cluster
  restore, pass `snapshot.s3_bucket` + creds that can read it. Add a `restore`-specific
  offsite resolver keyed on the snapshot, not the destination server.
- **Type match** — `mariadb` dataset → Database Server only; `benches` → Server only.
  Enforce via `snapshot.server_type` + `dataset`.
- **Cross-arch caveat (data content, not ZFS):** `benches` contains `/var/lib/docker` =
  arch-specific images → x86 snapshot restored on arm64 = wrong-arch images (needs rebuild).
  `mariadb` data is arch-neutral → restores fine across arch. Block/ warn on cross-arch
  app-server restore; allow cross-arch DB restore.

---

## 8. Scheduling — `press/hooks.py`

Add to `scheduler_events` (near existing snapshot entries :286–297, :356–373):
- `take_zfs_snapshots` — interval (mirror `snapshot_aws_servers` cadence).
- `zfs_snapshot.expire_snapshots`, `.sync_ongoing`, `.delete_failed`.
Add perms: `get_permission_query_conditions` (:132) + `has_permission` (:166) for
`ZFS Snapshot` and `ZFS Snapshot Recovery`.

---

## 9. Dashboard (press, `dashboard/src/`)

- Server / Database Server detail: ZFS toggle (read-only reflection of `enable_zfs`) +
  "Snapshots" tab listing `ZFS Snapshot` (reuse Server Snapshot list component).
- `ZFS Snapshot` actions: Restore (mode picker), Lock/Unlock, Delete.
- Cluster settings: `enable_zfs` checkbox.

---

## 10. Tests

- `test_zfs_snapshot.py`: full-vs-incremental selection; chain walk
  (`restore_chain` order); capacity validation throw; `enable_zfs` requires data volume;
  lock blocks delete; consistent-snapshot stops/starts services. Mock `Agent` HTTP only;
  real DB records. `tearDown` → `frappe.db.rollback()`.
- Restore: assert correct ordered `s3_keys` passed to `Agent.zfs_restore`; in-place gated
  by settings toggle (assert throw when disabled).
- Setup-time: unit-test `set_default_mount_points` emits binds-only when `enable_zfs`.

---

## Build order (sliceable)

1. **Flags + validation** (§2) — cheap, unblocks everything.
2. **Pool setup** (§3) — `zfs_pool` role + server.py wiring. Verify on a Hetzner test server.
3. **Grow/expand pool** (§3a, §3b) — `Add ZFS Volume` job + `zfs_pool_extend` role; resize branch in `IncreaseDiskSizeJob` + `zfs_pool_expand` role. Independent of snapshots; land right after pool setup.
4. **ZFS Snapshot doctype + Press Job + agent methods + agent routes** (§4–6) — core.
5. **Scheduling/sync/expire** (§8).
6. **Restore: new server** (§7) — topology-independent path.
7. **Restore: in-place** (§7) — destructive, gated.
8. **Dashboard + tests** (§9–10) alongside each slice.

## Open items to confirm with billing/ops
- Reuse `Server Snapshot Plan` or new `ZFS Snapshot Plan`?
- `full_interval_days` + retention counts (full/incremental) defaults.
- zstd level / multipart part size in agent (perf tuning).
