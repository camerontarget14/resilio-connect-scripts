# File: resilio-connect-scripts/Resilio Connect API/Python3/shotgrid-status-webhooks-firebase/functions/resilio_sync.py
"""
Resilio Connect sync job integration for Firebase Functions
"""
import os
import re
import yaml
from typing import Dict, Any, Optional
from api import ApiBaseCommands
from errors import ApiError


class ResilioSyncAPI(ApiBaseCommands):
    """
    Extended Resilio API for sync job operations.
    Builds on the existing ApiBaseCommands structure.
    """

    def __init__(self, base_url: str, token: str, verify: bool = False):
        super().__init__(base_url, token, verify)

    def find_job_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find a job by name from the jobs list."""
        try:
            jobs = self._get_jobs()
            for j in jobs:
                if j.get("name") == name:
                    return j
            return None
        except ApiError:
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
        """Create a Sync Job using the existing API structure."""

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
            src_permission = "rw"
            dst_permission = "rw"

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
        """Start a job by creating a job run."""
        try:
            run_attrs = {"job_id": int(job_id)}
            job_run_id = self._create_job_run(run_attrs)
            return job_run_id
        except ApiError as e:
            raise ApiError(f"Failed to start job {job_id}: {e}")


class ArtistSyncManager:
    """Manages artist sync job creation using Resilio Connect API."""

    def __init__(self, config_path: str = "artists.yaml"):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def validate_show(self, show: str) -> str:
        """Validate show format."""
        if not show or not re.match(r"^[A-Za-z0-9]+$", show):
            raise ValueError(f"Show must be alphanumeric, got: {show}")
        return show

    def validate_shot(self, shot: str) -> str:
        """Validate shot format."""
        if not shot or not re.match(r"^[A-Za-z0-9]+_[0-9]{3}_[0-9]{4}$", shot):
            raise ValueError(f"Shot must look like TST_010_0010, got: {shot}")
        return shot

    def choose_location(self, artist: str) -> str:
        """Get location key for artist."""
        artists = self.config.get("artists", {})
        if artist not in artists:
            raise ValueError(f"Artist '{artist}' not found in configuration")
        return str(artists[artist])

    def get_location(self, location_key: str) -> Dict[str, Any]:
        """Get location configuration."""
        locs = self.config.get("locations", {})
        if location_key not in locs:
            raise ValueError(f"Location '{location_key}' not found in configuration")
        loc = locs[location_key]
        for k in ("agent_id", "root", "os"):
            if k not in loc:
                raise ValueError(f"Location '{location_key}' missing '{k}' in configuration")
        return loc

    def get_local(self) -> Dict[str, Any]:
        """Get local configuration."""
        local = self.config.get("local")
        if not local:
            raise ValueError("'local' section missing in configuration")
        for k in ("agent_id", "root", "os"):
            if k not in local:
                raise ValueError(f"'local' missing '{k}' in configuration")
        return local

    def build_paths(self, show: str, shot: str, src_root: str, dst_root: str) -> Dict[str, str]:
        """Build source and destination paths."""
        paths = self.config.get("paths", {})
        rel_template = paths.get("relative_vfx")
        if not rel_template:
            raise ValueError("'paths.relative_vfx' missing in configuration")

        # Replace both tokens
        rel = rel_template.replace("${SHOW}", show).replace("${SHOT}", shot)

        src_path = f"{src_root.rstrip('/')}/{rel}"
        dst_path = f"{dst_root.rstrip('/')}/{rel}"
        return {"source": src_path, "destination": dst_path, "rel": rel}

    def job_name(self, shot: str, artist: str, location_key: str) -> str:
        """Generate job name."""
        return f"SYNC:{shot}:{artist}:{location_key}"

    def create_artist_sync_job(self, show: str, shot: str, artist: str,
                              resilio_url: str, resilio_token: str) -> Dict[str, Any]:
        """Create a sync job for an artist assignment."""

        # Validate inputs
        show = self.validate_show(show)
        shot = self.validate_shot(shot)

        # Get configuration
        location_key = self.choose_location(artist)
        loc = self.get_location(location_key)
        local = self.get_local()

        # Build paths
        paths = self.build_paths(
            show=show,
            shot=shot,
            src_root=local["root"],
            dst_root=loc["root"],
        )

        name = self.job_name(shot, artist, location_key)

        # Get defaults
        defaults = self.config.get("defaults", {}) or {}
        direction = defaults.get("sync_direction", "bidirectional")
        profile_id = defaults.get("profile_id")
        priority = defaults.get("priority")
        ignore_patterns = defaults.get("ignore_patterns", None)

        # Initialize Resilio API
        api = ResilioSyncAPI(resilio_url, resilio_token, verify=False)

        # Check if job already exists (idempotency)
        existing = api.find_job_by_name(name)
        if existing:
            job_id = str(existing.get("id"))
            job_run_id = api.start_job(job_id)
            return {
                "job_id": job_id,
                "job_run_id": job_run_id,
                "name": name,
                "status": "restarted_existing",
                "show": show,
                "shot": shot,
                "artist": artist,
                "location": location_key
            }

        # Create metadata
        metadata = {
            "artist": artist,
            "location_key": location_key,
            "show": show,
            "shot": shot,
            "rel": paths["rel"]
        }

        # Create job
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

        job_id = str(created.get("id", ""))
        job_run_id = api.start_job(job_id)

        return {
            "job_id": job_id,
            "job_run_id": job_run_id,
            "name": name,
            "status": "created_and_started",
            "show": show,
            "shot": shot,
            "artist": artist,
            "location": location_key,
            "paths": paths
        }
