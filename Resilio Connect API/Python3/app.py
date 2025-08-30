#!/usr/bin/env python3
"""
app.py — Create a Resilio Connect Sync Job for a given Show/Shot/Artist.

UX:
  $ python app.py
  Show: TST
  Shot: TST_010_0010
  Artist: Matthew
  ...prints preview...
  Proceed? [Y/n]:

Or non-interactive:
  $ python app.py --show TST --shot TST_010_0010 --artist Matthew

Configuration:
  - Environment:
      RESILIO_URL   = https://your-console.example.com   (no trailing slash)
      RESILIO_TOKEN = <MC API token>
"""

import os
import sys
import re
import json
import argparse
from typing import Dict, Any, Optional

import yaml
from api import ApiBaseCommands
from errors import ApiError


# ---------- Utilities ----------

def load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        sys.exit(f"[ERROR] Config YAML not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def env_or_die(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        sys.exit(f"[ERROR] Missing environment variable: {name}")
    return val


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create a Resilio Connect Sync Job for a Show/Shot/Artist.")
    p.add_argument("--config", default="artists.yaml", help="Path to YAML config (default: artists.yaml)")
    p.add_argument("--show", help="Show code (e.g., TST)")
    p.add_argument("--shot", help="Shot name (e.g., TST_010_0010)")
    p.add_argument("--artist", help="Artist name (must exist in YAML)")
    p.add_argument("--dry-run", action="store_true", help="Print payload and exit without calling API")
    p.add_argument("--yes", "-y", action="store_true", help="Skip interactive confirmation")
    return p.parse_args()


def prompt_if_missing(v: Optional[str], label: str) -> str:
    if v:
        return v.strip()
    return input(f"{label}: ").strip()


def validate_show(show: str) -> str:
    if not show or not re.match(r"^[A-Za-z0-9]+$", show):
        sys.exit("[ERROR] Show must be alphanumeric (e.g., TST).")
    return show


def validate_shot(shot: str) -> str:
    # Accepts TST_010_0010 style; tweak pattern if your naming varies.
    if not shot or not re.match(r"^[A-Za-z0-9]+_[0-9]{3}_[0-9]{4}$", shot):
        sys.exit("[ERROR] Shot must look like TST_010_0010.")
    return shot


def choose_location(cfg: Dict[str, Any], artist: str) -> str:
    artists = cfg.get("artists", {})
    if artist not in artists:
        sys.exit(f"[ERROR] Artist '{artist}' not found in YAML 'artists' mapping.")
    return str(artists[artist])


def get_location(cfg: Dict[str, Any], location_key: str) -> Dict[str, Any]:
    locs = cfg.get("locations", {})
    if location_key not in locs:
        sys.exit(f"[ERROR] Location '{location_key}' not found in YAML 'locations'.")
    loc = locs[location_key]
    for k in ("agent_id", "root", "os"):
        if k not in loc:
            sys.exit(f"[ERROR] Location '{location_key}' missing '{k}' in YAML.")
    return loc


def get_local(cfg: Dict[str, Any]) -> Dict[str, Any]:
    local = cfg.get("local")
    if not local:
        sys.exit("[ERROR] 'local' section missing in YAML.")
    for k in ("agent_id", "root", "os"):
        if k not in local:
            sys.exit(f"[ERROR] 'local' missing '{k}' in YAML.")
    return local


def build_paths(cfg: Dict[str, Any], show: str, shot: str,
                src_root: str, dst_root: str) -> Dict[str, str]:
    paths = cfg.get("paths", {})
    rel_template = paths.get("relative_vfx")
    if not rel_template:
        sys.exit("[ERROR] 'paths.relative_vfx' missing in YAML.")

    # Replace both tokens
    rel = rel_template.replace("${SHOW}", show).replace("${SHOT}", shot)

    src_path = f"{src_root.rstrip('/')}/{rel}"
    dst_path = f"{dst_root.rstrip('/')}/{rel}"
    return {"source": src_path, "destination": dst_path, "rel": rel}


def job_name(shot: str, artist: str, location_key: str) -> str:
    return f"SYNC:{shot}:{artist}:{location_key}"


# ---------- Enhanced API class extending existing base ----------

class ResilioSyncAPI(ApiBaseCommands):
    """
    Extended Resilio API for sync job operations.
    Builds on the existing ApiBaseCommands structure.
    """

    def __init__(self, base_url: str, token: str, verify: bool = False):
        super().__init__(base_url, token, verify)

    def find_job_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Find a job by name from the jobs list.
        """
        try:
            jobs = self._get_jobs()
            for j in jobs:
                if j.get("name") == name:
                    return j
            return None
        except ApiError as e:
            print(f"[WARN] find_job_by_name failed ({e}); continuing as if not found.")
            return None

    def create_sync_job(self,
                        name: str,
                        src_agent_id: str,
                        src_path: str,
                        dst_agent_id: str,
                        dst_path: str,
                        direction: str = "bidirectional",
                        profile_id: Optional[str] = None,
                        priority: Optional[str] = None,
                        metadata: Optional[Dict[str, Any]] = None,
                        ignore_patterns: Optional[list] = None) -> Dict[str, Any]:
        """
        Create a Sync Job using the existing API structure.
        For sync jobs, we create groups for source and destination agents.
        """

        # Create temporary groups for the sync job
        src_group_name = f"TEMP_SRC_{name}_{src_agent_id}"
        dst_group_name = f"TEMP_DST_{name}_{dst_agent_id}"

        try:
            # Create source group
            src_group_attrs = {
                'name': src_group_name,
                'description': f'Temporary source group for sync job {name}',
                'agents': [{'id': int(src_agent_id)}]
            }
            src_group_id = self._create_group(src_group_attrs)

            # Create destination group
            dst_group_attrs = {
                'name': dst_group_name,
                'description': f'Temporary destination group for sync job {name}',
                'agents': [{'id': int(dst_agent_id)}]
            }
            dst_group_id = self._create_group(dst_group_attrs)

            # Determine permissions based on direction
            if direction == "bidirectional":
                src_permission = "rw"
                dst_permission = "rw"
            else:  # one_way (source to destination)
                src_permission = "rw"
                dst_permission = "rw"  # destination needs write to receive files

            # Create the sync job with groups
            groups_data = [
                {
                    'id': src_group_id,
                    'path': {
                        'linux': src_path,
                        'win': src_path,
                        'osx': src_path
                    },
                    'permission': src_permission
                },
                {
                    'id': dst_group_id,
                    'path': {
                        'linux': dst_path,
                        'win': dst_path,
                        'osx': dst_path
                    },
                    'permission': dst_permission
                }
            ]

            job_attrs = {
                'name': name,
                'type': 'sync',
                'description': f'Sync job for {metadata.get("show", "")}/{metadata.get("shot", "")} - {metadata.get("artist", "")}',
                'groups': groups_data
            }

            # Add optional attributes
            if metadata:
                job_attrs['metadata'] = metadata
            if ignore_patterns:
                job_attrs['ignore_patterns'] = ignore_patterns

            job_id = self._create_job(job_attrs)

            return {
                'id': job_id,
                'name': name,
                'src_group_id': src_group_id,
                'dst_group_id': dst_group_id
            }

        except ApiError as e:
            # Clean up any created groups on failure
            try:
                if 'src_group_id' in locals():
                    self._delete_group(src_group_id)
                if 'dst_group_id' in locals():
                    self._delete_group(dst_group_id)
            except ApiError:
                pass  # Ignore cleanup errors
            raise e

    def start_job(self, job_id: str) -> int:
        """
        Start a job by creating a job run.
        Returns the job run ID.
        """
        try:
            run_attrs = {"job_id": int(job_id)}
            job_run_id = self._create_job_run(run_attrs)
            return job_run_id
        except ApiError as e:
            raise ApiError(f"Failed to start job {job_id}: {e}")

    def cleanup_temp_groups(self, job_info: Dict[str, Any]):
        """
        Clean up temporary groups created for a sync job.
        Call this if you want to clean up after job completion.
        """
        try:
            if 'src_group_id' in job_info:
                self._delete_group(job_info['src_group_id'])
            if 'dst_group_id' in job_info:
                self._delete_group(job_info['dst_group_id'])
        except ApiError as e:
            print(f"[WARN] Failed to clean up temporary groups: {e}")


# ---------- Main flow ----------

def main():
    args = parse_args()
    cfg = load_yaml(args.config)

    show = validate_show(prompt_if_missing(args.show, "Show"))
    shot = validate_shot(prompt_if_missing(args.shot, "Shot"))
    artist = prompt_if_missing(args.artist, "Artist")

    location_key = choose_location(cfg, artist)
    loc = get_location(cfg, location_key)
    local = get_local(cfg)

    paths = build_paths(
        cfg=cfg,
        show=show,
        shot=shot,
        src_root=local["root"],
        dst_root=loc["root"],
    )

    name = job_name(shot, artist, location_key)

    defaults = cfg.get("defaults", {}) or {}
    direction = defaults.get("sync_direction", "bidirectional")
    profile_id = defaults.get("profile_id")
    priority = defaults.get("priority")
    ignore_patterns = defaults.get("ignore_patterns", None)

    # Preview
    print("\n--- Preview ---")
    print(f"SHOW:           {show}")
    print(f"SHOT:           {shot}")
    print(f"Artist:         {artist}")
    print(f"Location key:   {location_key}")
    print(f"Job name:       {name}")
    print(f"Direction:      {direction}")
    if profile_id:
        print(f"Profile ID:     {profile_id}")
    if priority:
        print(f"Priority:       {priority}")
    print(f"Source agent:   {local['agent_id']}  [{local['os']}]")
    print(f"Source path:    {paths['source']}")
    print(f"Dest agent:     {loc['agent_id']}    [{loc['os']}]")
    print(f"Dest path:      {paths['destination']}")
    if ignore_patterns:
        print(f"Ignore:         {ignore_patterns}")
    print("---------------\n")

    if args.dry_run:
        print("[DRY-RUN] No API calls made.")
        return

    if not args.yes:
        proceed = input("Proceed to create (or reuse) and start job? [Y/n]: ").strip().lower()
        if proceed and proceed not in ("y", "yes"):
            print("Aborted.")
            return

    base_url = env_or_die("RESILIO_URL")
    token = env_or_die("RESILIO_TOKEN")

    # Use the integrated API class
    api = ResilioSyncAPI(base_url, token, verify=False)

    # Idempotency: try to find by name first
    existing = api.find_job_by_name(name)
    if existing:
        job_id = str(existing.get("id"))
        if not job_id:
            sys.exit("[ERROR] Found job but it has no 'id' field; check API schema.")
        print(f"[INFO] Job already exists: {name} (id={job_id}). Starting it...")

        try:
            job_run_id = api.start_job(job_id)
            print(f"[OK] Job started with run ID: {job_run_id}")
        except ApiError as e:
            sys.exit(f"[ERROR] Failed to start existing job: {e}")
        return

    # Attach metadata to help future automation/waterfall from ShotGrid
    metadata = {
        "artist": artist,
        "location_key": location_key,
        "show": show,
        "shot": shot,
        "rel": paths["rel"]
    }

    # Create job
    try:
        created = api.create_sync_job(
            name=name,
            src_agent_id=str(local["agent_id"]),
            src_path=paths["source"],
            dst_agent_id=str(loc["agent_id"]),
            dst_path=paths["destination"],
            direction=direction,
            profile_id=profile_id,
            priority=priority,
            metadata=metadata,
            ignore_patterns=ignore_patterns
        )
    except ApiError as e:
        sys.exit(f"[ERROR] Create job failed: {e}")

    job_id = str(created.get("id", "")).strip()
    if not job_id:
        sys.exit(f"[ERROR] Create job returned no 'id'. Response was: {created}")

    print(f"[OK] Job created: {name} (id={job_id}). Starting it...")
    try:
        job_run_id = api.start_job(job_id)
        print(f"[OK] Job started with run ID: {job_run_id}")
    except ApiError as e:
        sys.exit(f"[ERROR] Start job failed: {e}")

    # Optional: Clean up temporary groups after some time
    # You might want to do this in a separate cleanup script or after job completion
    # api.cleanup_temp_groups(created)


if __name__ == "__main__":
    main()
