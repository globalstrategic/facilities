#!/usr/bin/env python3
"""
Schema Migration Utility

Migrates existing facility JSON files to support new EntityIdentity integration fields:
- ei_facility_id: Optional EntityIdentity facility ID
- commodities.chemical_formula: Optional chemical formula/symbol
- commodities.category: Optional metal category classification

Preserves all existing data and creates backups before modification.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class SchemaMigrator:
    """Migrate facility JSON files to new schema version."""

    def __init__(self, facilities_dir: Path = None, backup: bool = True):
        """Initialize migrator.

        Args:
            facilities_dir: Root directory containing country subdirectories
            backup: Whether to create backups before migration
        """
        self.facilities_dir = facilities_dir or Path("facilities")
        self.backup = backup
        self.stats = {
            "total": 0,
            "migrated": 0,
            "skipped": 0,
            "errors": 0,
            "backups_created": 0
        }

    def migrate_all(self, dry_run: bool = False) -> Dict:
        """Migrate all facilities in the database.

        Args:
            dry_run: If True, only report what would be changed without modifying files

        Returns:
            Dictionary with migration statistics
        """
        print(f"Starting migration (dry_run={dry_run})...")
        print(f"Facilities directory: {self.facilities_dir}")

        if not self.facilities_dir.exists():
            raise ValueError(f"Facilities directory not found: {self.facilities_dir}")

        # Find all facility JSON files
        facility_files = list(self.facilities_dir.glob("*/*.json"))
        self.stats["total"] = len(facility_files)

        print(f"Found {len(facility_files)} facility files")

        for fac_file in facility_files:
            try:
                self._migrate_file(fac_file, dry_run)
            except Exception as e:
                print(f"Error migrating {fac_file}: {e}")
                self.stats["errors"] += 1

        # Print summary
        print("\n" + "="*60)
        print("Migration Summary")
        print("="*60)
        print(f"Total facilities: {self.stats['total']}")
        print(f"Migrated: {self.stats['migrated']}")
        print(f"Skipped (no changes): {self.stats['skipped']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"Backups created: {self.stats['backups_created']}")

        return self.stats

    def _migrate_file(self, file_path: Path, dry_run: bool = False) -> bool:
        """Migrate a single facility file.

        Args:
            file_path: Path to facility JSON file
            dry_run: If True, don't modify the file

        Returns:
            True if migration was needed, False if file already up to date
        """
        # Load existing facility
        with open(file_path, 'r') as f:
            facility = json.load(f)

        # Check if migration needed
        needs_migration, changes = self._check_migration_needed(facility)

        if not needs_migration:
            self.stats["skipped"] += 1
            return False

        if dry_run:
            print(f"[DRY RUN] Would migrate {file_path.name}:")
            for change in changes:
                print(f"  - {change}")
            self.stats["migrated"] += 1
            return True

        # Create backup if enabled
        if self.backup:
            self._create_backup(file_path)
            self.stats["backups_created"] += 1

        # Apply migration
        migrated = self._apply_migration(facility)

        # Write updated file
        with open(file_path, 'w') as f:
            json.dump(migrated, f, indent=2)

        print(f"Migrated {file_path.name}")
        self.stats["migrated"] += 1
        return True

    def _check_migration_needed(self, facility: Dict) -> Tuple[bool, List[str]]:
        """Check if facility needs migration.

        Args:
            facility: Facility dictionary

        Returns:
            Tuple of (needs_migration, list_of_changes)
        """
        changes = []

        # Check for ei_facility_id field
        if "ei_facility_id" not in facility:
            changes.append("Add ei_facility_id field")

        # Check commodities for new fields
        if "commodities" in facility:
            for i, commodity in enumerate(facility["commodities"]):
                if "chemical_formula" not in commodity:
                    changes.append(f"Add chemical_formula to commodity {i}: {commodity.get('metal')}")
                if "category" not in commodity:
                    changes.append(f"Add category to commodity {i}: {commodity.get('metal')}")

        return len(changes) > 0, changes

    def _apply_migration(self, facility: Dict) -> Dict:
        """Apply migration to facility dictionary.

        Args:
            facility: Original facility dictionary

        Returns:
            Migrated facility dictionary
        """
        migrated = facility.copy()

        # Add ei_facility_id if missing
        if "ei_facility_id" not in migrated:
            migrated["ei_facility_id"] = None

        # Migrate commodities
        if "commodities" in migrated:
            migrated_commodities = []
            for commodity in migrated["commodities"]:
                migrated_commodity = commodity.copy()

                # Add chemical_formula if missing
                if "chemical_formula" not in migrated_commodity:
                    migrated_commodity["chemical_formula"] = None

                # Add category if missing
                if "category" not in migrated_commodity:
                    migrated_commodity["category"] = None

                migrated_commodities.append(migrated_commodity)

            migrated["commodities"] = migrated_commodities

        return migrated

    def _create_backup(self, file_path: Path) -> Path:
        """Create backup of file before migration.

        Args:
            file_path: Path to file to backup

        Returns:
            Path to backup file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = file_path.parent / "backups"
        backup_dir.mkdir(exist_ok=True)

        backup_path = backup_dir / f"{file_path.stem}_backup_{timestamp}.json"
        shutil.copy2(file_path, backup_path)

        return backup_path

    def migrate_single(self, facility_id: str, dry_run: bool = False) -> bool:
        """Migrate a single facility by ID.

        Args:
            facility_id: Facility ID to migrate
            dry_run: If True, don't modify the file

        Returns:
            True if migration succeeded
        """
        # Find facility file
        facility_files = list(self.facilities_dir.glob(f"*/{facility_id}.json"))

        if not facility_files:
            raise ValueError(f"Facility not found: {facility_id}")

        if len(facility_files) > 1:
            raise ValueError(f"Multiple facilities found with ID: {facility_id}")

        return self._migrate_file(facility_files[0], dry_run)


def main():
    """CLI entry point for schema migration."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Migrate facility JSON files to new schema version"
    )
    parser.add_argument(
        "--facilities-dir",
        type=Path,
        default=Path("facilities"),
        help="Root directory containing facility files"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating backup files"
    )
    parser.add_argument(
        "--facility-id",
        type=str,
        help="Migrate only a specific facility ID"
    )

    args = parser.parse_args()

    migrator = SchemaMigrator(
        facilities_dir=args.facilities_dir,
        backup=not args.no_backup
    )

    if args.facility_id:
        # Migrate single facility
        success = migrator.migrate_single(args.facility_id, dry_run=args.dry_run)
        if success:
            print(f"Successfully migrated {args.facility_id}")
        else:
            print(f"{args.facility_id} already up to date")
    else:
        # Migrate all facilities
        stats = migrator.migrate_all(dry_run=args.dry_run)

        # Exit with error code if any errors occurred
        if stats["errors"] > 0:
            exit(1)


if __name__ == "__main__":
    main()
