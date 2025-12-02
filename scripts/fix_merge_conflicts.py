#!/usr/bin/env python3
"""
Fix git merge conflicts in facility JSON files.

Automatically resolves merge conflicts by taking the HEAD version (current branch).
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple


def find_conflict_files(facilities_dir: Path) -> List[Path]:
    """Find all JSON files with merge conflict markers."""
    conflict_files = []

    for json_file in facilities_dir.rglob('*.json'):
        try:
            content = json_file.read_text(encoding='utf-8')
            # Check for any conflict marker
            if any(marker in content for marker in ['<<<<<<< HEAD', '=======\n', '>>>>>>>']):
                conflict_files.append(json_file)
        except Exception as e:
            print(f"Warning: Could not read {json_file}: {e}", file=sys.stderr)

    return conflict_files


def resolve_conflicts(content: str, strategy: str = 'ours') -> Tuple[str, int]:
    """
    Resolve merge conflicts in content.

    Args:
        content: File content with conflict markers
        strategy: 'ours' (take HEAD), 'theirs' (take incoming), or 'both' (merge)

    Returns:
        Tuple of (resolved content, number of conflicts resolved)
    """
    conflicts_resolved = 0

    # Pattern 1: Complete conflict blocks
    # <<<<<<< HEAD
    # ... ours ...
    # =======
    # ... theirs ...
    # >>>>>>> commit_hash
    complete_pattern = re.compile(
        r'<<<<<<< HEAD\n(.*?)\n=======\n(.*?)\n>>>>>>> [a-f0-9]+\n',
        re.DOTALL
    )

    # Pattern 2: Incomplete conflict blocks (missing HEAD marker)
    # =======
    # ... theirs ...
    # >>>>>>> commit_hash
    incomplete_pattern = re.compile(
        r'=======\n(.*?)\n>>>>>>> [a-f0-9]+\n',
        re.DOTALL
    )

    def replace_complete_conflict(match):
        nonlocal conflicts_resolved
        conflicts_resolved += 1

        ours = match.group(1)
        theirs = match.group(2)

        if strategy == 'ours':
            return ours + '\n'
        elif strategy == 'theirs':
            return theirs + '\n'
        elif strategy == 'both':
            # Try to merge intelligently
            # For notes fields, concatenate with a separator
            if '"notes":' in ours and '"notes":' in theirs:
                # Extract the note values
                ours_note = re.search(r'"notes":\s*"([^"]*)"', ours)
                theirs_note = re.search(r'"notes":\s*"([^"]*)"', theirs)

                if ours_note and theirs_note:
                    combined = f'"notes": "{ours_note.group(1)} | {theirs_note.group(1)}"'
                    return combined + '\n'

            # Default: take ours
            return ours + '\n'
        else:
            return ours + '\n'

    def replace_incomplete_conflict(match):
        nonlocal conflicts_resolved
        conflicts_resolved += 1
        # For incomplete conflicts, just remove the markers and theirs content
        # (we're essentially keeping what was before the =======)
        return ''

    # First resolve complete conflicts
    resolved = complete_pattern.sub(replace_complete_conflict, content)
    # Then resolve incomplete conflicts
    resolved = incomplete_pattern.sub(replace_incomplete_conflict, resolved)

    return resolved, conflicts_resolved


def fix_file(file_path: Path, strategy: str = 'ours', dry_run: bool = False) -> Tuple[bool, int]:
    """
    Fix merge conflicts in a single file.

    Returns:
        Tuple of (success, conflicts_resolved)
    """
    try:
        content = file_path.read_text(encoding='utf-8')
        resolved, count = resolve_conflicts(content, strategy)

        if count == 0:
            return True, 0

        if not dry_run:
            file_path.write_text(resolved, encoding='utf-8')

        return True, count
    except Exception as e:
        print(f"Error fixing {file_path}: {e}", file=sys.stderr)
        return False, 0


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Fix git merge conflicts in facility JSON files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Resolution strategies:
  ours   - Take the HEAD version (current branch) [default]
  theirs - Take the incoming version (merged branch)
  both   - Try to merge both versions intelligently

Examples:
  # Dry run to see what would be fixed
  python scripts/fix_merge_conflicts.py --dry-run

  # Fix all conflicts using HEAD version
  python scripts/fix_merge_conflicts.py

  # Fix using incoming version
  python scripts/fix_merge_conflicts.py --strategy theirs
        """
    )

    parser.add_argument(
        '--strategy',
        choices=['ours', 'theirs', 'both'],
        default='ours',
        help='Conflict resolution strategy (default: ours)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )

    args = parser.parse_args()

    # Find all files with conflicts
    facilities_dir = Path(__file__).parent.parent / 'facilities'
    conflict_files = find_conflict_files(facilities_dir)

    if not conflict_files:
        print("[OK] No merge conflicts found!")
        return 0

    print(f"Found {len(conflict_files)} files with merge conflicts")

    if args.dry_run:
        print("\n[DRY RUN] Would fix the following files:")
        for file_path in conflict_files:
            print(f"  - {file_path.relative_to(facilities_dir)}")
        print(f"\nRun without --dry-run to apply fixes")
        return 0

    # Fix each file
    total_conflicts = 0
    fixed_files = 0

    for file_path in conflict_files:
        success, count = fix_file(file_path, strategy=args.strategy, dry_run=False)
        if success and count > 0:
            fixed_files += 1
            total_conflicts += count
            print(f"Fixed {count} conflict(s) in {file_path.name}")

    print(f"\n[OK] Fixed {total_conflicts} conflicts in {fixed_files} files")
    print(f"     Strategy used: {args.strategy}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
