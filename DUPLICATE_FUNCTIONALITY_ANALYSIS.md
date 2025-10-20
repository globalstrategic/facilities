# Duplicate Functionality Analysis

**Date**: 2025-10-20
**Repository**: facilities/
**Analysis**: Identification of duplicate code and functionality across the codebase

---

## Summary

The repository contains **significant code duplication** across multiple scripts, with an estimated **~400-500 lines of duplicated functionality** that should be consolidated into shared utilities.

---

## 1. Country Normalization (HIGH PRIORITY)

### Current State

**Three different implementations** doing the same thing:

| Location | Function | Lines | Usage |
|----------|----------|-------|-------|
| `import_from_report.py` | `find_country_code()` | 494-532 | Import pipeline |
| `import_from_report.py` | `detect_country_from_filename()` | 1097-1120 | Filename parsing |
| `utils/country_utils.py` | `normalize_country_to_iso3()` | 22-111 | **✓ Canonical** |

### Problem

- `import_from_report.py` has **2 separate country functions** (~60 lines)
- `country_utils.py` provides a **unified solution** but isn't used in `import_from_report.py`
- Both approaches use EntityIdentity + pycountry, but with different logic

### Code Comparison

**import_from_report.py** (lines 494-532):
```python
def find_country_code(country_input: str) -> Tuple[str, str]:
    """Find country ISO codes using pycountry."""
    # Custom logic with manual lookups
    # Returns (iso2, iso3) tuple
```

**country_utils.py** (lines 22-111):
```python
def normalize_country_to_iso3(country_input: str) -> str:
    """Normalize any country input to ISO3 code."""
    # Uses EntityIdentity first, then pycountry fallback
    # More robust with fuzzy matching
    # Returns ISO3 only
```

### Solution

**Replace** `find_country_code()` and `detect_country_from_filename()` in `import_from_report.py` with:

```python
from utils.country_utils import normalize_country_to_iso3

# In import_from_report.py
country_iso3 = normalize_country_to_iso3(country_input)
```

**Impact**: Remove ~60 lines of duplicate code

---

## 2. Text Slugification (MEDIUM PRIORITY)

### Current State

**Two implementations** with slight differences:

| Location | Implementation | Removes Parentheticals? | Type |
|----------|---------------|------------------------|------|
| `import_from_report.py:91` | Standalone function | ✓ Yes | Global |
| `deep_research_integration.py:89` | Class method | ✗ No | Instance |

### Code Comparison

**import_from_report.py** (lines 91-99):
```python
def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r'\([^)]*\)', '', text)  # Removes parentheticals!
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')
```

**deep_research_integration.py** (lines 89-95):
```python
def slugify(self, text: str) -> str:
    """Convert text to URL-safe slug."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')
```

### Problem

- **Different behavior**: One removes `(Rustenburg)`, the other doesn't
- **Inconsistent IDs**: Same facility could get different IDs depending on which script processes it
- **Maintenance burden**: Bug fixes need to be applied twice

### Solution

**Create** `utils/text_utils.py`:

```python
def slugify(text: str, remove_parentheticals: bool = True) -> str:
    """Convert text to URL-safe slug.

    Args:
        text: Text to slugify
        remove_parentheticals: If True, remove text in parentheses

    Returns:
        Slugified text

    Example:
        >>> slugify("Karee Mine (Rustenburg)")
        'karee-mine'
        >>> slugify("Karee Mine (Rustenburg)", remove_parentheticals=False)
        'karee-mine-rustenburg'
    """
    if not text:
        return ""
    text = text.lower().strip()

    if remove_parentheticals:
        text = re.sub(r'\([^)]*\)', '', text)

    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')
```

**Impact**: Remove ~14 lines of duplicate code, ensure consistency

---

## 3. Company Resolution (PARTIALLY COMPLETE)

### Current State

**✅ MIGRATED**: `import_from_report.py` now uses Phase 2 pattern (company_mentions only, no resolution)
**⚠️ NEEDS MIGRATION**: `deep_research_integration.py` still uses direct EntityIdentity access

| Location | Approach | Status | Notes |
|----------|----------|--------|-------|
| `import_from_report.py` | Phase 2: Extract mentions only | ✅ Migrated | No resolution during import |
| `enrich_companies.py` | Via `CompanyResolver` | ✅ Current | Batch enrichment with quality gates |
| `deep_research_integration.py:129` | Direct EntityIdentity calls | ⚠️ Legacy | Needs migration to CompanyResolver |

### Code Comparison

**✅ import_from_report.py** (CURRENT - Phase 2 pattern):
```python
# Phase 1: Extract mentions only (NO resolution)
company_mentions.append({
    "name": company_name,
    "role": "owner",
    "source": source_name,
    "confidence": 0.60,
    "evidence": "Extracted from Group Names column"
})

# Resolution happens later in Phase 2 via enrich_companies.py
```

**⚠️ deep_research_integration.py** (LEGACY - needs migration):
```python
def resolve_company(self, company_name: str, country: Optional[str] = None) -> Dict:
    """Resolve company name - LEGACY APPROACH."""
    # Direct EntityIdentity usage
    # No quality gates
    # Should be migrated to CompanyResolver
```

### Solution

**Refactor** `deep_research_integration.py` to use `CompanyResolver`:

```python
from utils.company_resolver import CompanyResolver

class DeepResearchIntegrator:
    def __init__(self):
        # Use CompanyResolver instead of direct EntityIdentity
        self.resolver = CompanyResolver.from_config(
            str(ROOT / "config" / "gate_config.json"),
            profile="moderate"
        )
```

**Impact**:
- ✅ `import_from_report.py` already migrated to Phase 2 pattern
- ⚠️ `deep_research_integration.py` still needs migration
- Remove ~40 lines of duplicate resolution logic
- Consistent resolution across all pipelines

---

## 4. Metal Normalization (MEDIUM PRIORITY)

### Current State

**Inline dictionary** in `import_from_report.py` with no dedicated utility:

| Location | Implementation | Lines |
|----------|---------------|-------|
| `import_from_report.py:60-70` | `METAL_NORMALIZE_MAP` dict | Global constant |
| `import_from_report.py:584-587` | `normalize_metal()` function | Uses dict |

### Code

```python
METAL_NORMALIZE_MAP = {
    "aluminium": "aluminum",
    "ferronickel": "nickel",
    "ferromanganese": "manganese",
    "chromite": "chromium",
    "pgm": "platinum",
    "pge": "platinum",
    # ... 20+ more mappings
}

def normalize_metal(metal: str) -> str:
    metal_lower = metal.lower().strip()
    return METAL_NORMALIZE_MAP.get(metal_lower, metal_lower)
```

### Problem

- **Not reusable**: Other scripts can't access this mapping
- **Incomplete**: Doesn't use EntityIdentity's `metal_identifier()` which is more comprehensive
- **No chemical formulas**: Doesn't provide chemical formulas or categories

### Solution

**Create** `utils/metal_utils.py`:

```python
from entityidentity import metal_identifier

# Fallback map for common variations
METAL_ALIASES = {
    "aluminium": "aluminum",
    "pgm": "platinum",
    # ... other aliases
}

def normalize_metal(metal: str) -> Dict:
    """Normalize metal name with chemical formula and category.

    Uses EntityIdentity's metal_identifier() with fallback to alias map.

    Returns:
        {
            "metal": "copper",
            "chemical_formula": "Cu",
            "category": "base_metal"
        }
    """
    # Try EntityIdentity first
    try:
        result = metal_identifier(metal)
        if result:
            return result
    except:
        pass

    # Fallback to alias map
    metal_lower = metal.lower().strip()
    normalized = METAL_ALIASES.get(metal_lower, metal_lower)

    return {
        "metal": normalized,
        "chemical_formula": None,
        "category": None
    }
```

**Impact**:
- Centralize metal normalization
- Leverage EntityIdentity's comprehensive database
- Enable chemical formula extraction

---

## 5. Facility Load/Save (LOW PRIORITY)

### Current State

**Two different implementations**:

| Location | Functions | Backup | Usage |
|----------|-----------|--------|-------|
| `import_from_report.py:1052` | `write_facilities()` | ✗ No | Batch import |
| `deep_research_integration.py:97,107` | `load_facility()`, `save_facility()` | ✓ Yes | Research updates |

### Code Comparison

**import_from_report.py** (line 1052):
```python
def write_facilities(facilities: List[Dict], country_dir_name: str) -> int:
    """Write list of facilities to JSON files."""
    # Batch writes
    # No backup
    # Returns count
```

**deep_research_integration.py** (lines 97-127):
```python
def load_facility(self, facility_id: str, country_iso3: str) -> Optional[Dict]:
    """Load a facility JSON file."""
    # Single load
    # Error handling

def save_facility(self, facility: Dict, backup: bool = True):
    """Save updated facility JSON file."""
    # Single save
    # Optional backup with timestamp
    # Error handling
```

### Problem

- **Different interfaces**: One is batch, one is single-facility
- **No backup in import**: Import pipeline doesn't preserve originals
- **Inconsistent error handling**

### Solution

**Create** `utils/facility_io.py`:

```python
def load_facility(facility_id: str, country_iso3: str) -> Optional[Dict]:
    """Load a single facility JSON file."""
    # Canonical single-facility loader

def save_facility(facility: Dict, backup: bool = True):
    """Save a facility JSON file with optional backup."""
    # Canonical single-facility saver

def write_facilities(facilities: List[Dict], country_dir: str, backup: bool = False) -> int:
    """Write multiple facilities in batch."""
    # Batch wrapper around save_facility()
```

**Impact**:
- Consolidate I/O operations
- Consistent backup behavior
- Easier testing and error handling

---

## 6. Migration/Backfill Scripts (LOW PRIORITY)

### Current State

**Four scripts** with potential overlap (~1,241 lines total):

| Script | Lines | Purpose | Still Needed? |
|--------|-------|---------|---------------|
| `backfill_mentions.py` | 440 | Extract company_mentions from facilities | ✓ Yes (Phase 1→2) |
| `verify_backfill.py` | 185 | Verify backfill completeness | ✓ Yes (QA) |
| `migrate_legacy_fields.py` | 235 | Field-level schema migration | ? Maybe archived |
| `full_migration.py` | 381 | Complete legacy migration | ? Maybe archived |

### Problem

- **Unclear if still used**: Migration scripts may be one-time operations
- **No documentation**: README doesn't explain when to use each
- **Potential overlap**: All touch facility JSONs

### Solution

**Archive or consolidate**:

1. If migration is complete, move `migrate_legacy_fields.py` and `full_migration.py` to `migration/` directory
2. Add comments indicating they're for historical reference only
3. Update documentation to clarify which scripts are active vs archived

**Impact**: Clarify codebase, prevent confusion

---

## Summary of Duplications

| Issue | Scripts Affected | Duplicate Lines | Status | Effort Remaining |
|-------|-----------------|-----------------|--------|------------------|
| Country normalization | `import_from_report.py`, `country_utils.py` | ~60 | ⚠️ Partial | 1 hour |
| Company resolution | `deep_research_integration.py`, ~~`import_from_report.py`~~ | ~40 | ✅ Migrated (import) | 2 hours (research) |
| Slugification | `import_from_report.py`, `deep_research_integration.py` | ~14 | 🔴 Needed | 30 min |
| Metal normalization | `import_from_report.py` (should use utility) | ~30 | 🔴 Needed | 1 hour |
| Facility I/O | `import_from_report.py`, `deep_research_integration.py` | ~50 | 🔴 Needed | 1-2 hours |
| Migration scripts | 4 scripts | ~1,241 (archive?) | 🔴 Needed | 30 min |

**Progress**:
- ✅ `import_from_report.py` migrated to Phase 2 pattern (no company resolution)
- ⚠️ `deep_research_integration.py` still needs migration
- **Total Duplicate Code Remaining**: ~395 lines (excluding migration scripts)

---

## Refactoring Recommendations

### Phase 1: High Priority (4-6 hours)

1. **Consolidate country normalization**
   - Remove `find_country_code()` from `import_from_report.py`
   - Use `country_utils.normalize_country_to_iso3()` everywhere
   - Test import pipeline thoroughly

2. **Unify company resolution**
   - Refactor `deep_research_integration.py` to use `CompanyResolver`
   - Migrate from direct EntityIdentity calls to wrapper pattern
   - Add quality gates to research pipeline
   - Test resolution consistency

### Phase 2: Medium Priority (2-3 hours)

3. **Create shared utilities**
   - `utils/text_utils.py` for `slugify()`
   - `utils/metal_utils.py` for metal normalization
   - Update all scripts to use shared utilities

### Phase 3: Low Priority (2-3 hours)

4. **Consolidate facility I/O**
   - Create `utils/facility_io.py`
   - Standardize load/save operations
   - Add consistent backup behavior

5. **Clean up migration scripts**
   - Archive one-time migration scripts
   - Document which scripts are active
   - Add deprecation warnings if needed

### Testing Strategy

For each refactoring:
1. **Add unit tests** for the utility before refactoring
2. **Test current behavior** (snapshot existing outputs)
3. **Refactor script** to use utility
4. **Test new behavior** (compare outputs, should match)
5. **Update documentation**

---

## Benefits of Refactoring

1. **Reduced code**: ~435 fewer lines to maintain
2. **Consistency**: Same logic used everywhere
3. **Quality**: Bug fixes benefit all scripts
4. **Testability**: Utilities easier to unit test
5. **Maintainability**: Single source of truth
6. **Performance**: Shared caches (e.g., CompanyResolver)

---

## Risks & Mitigation

| Risk | Mitigation |
|------|------------|
| Breaking existing imports | Comprehensive testing, snapshot outputs before/after |
| Different behavior in edge cases | Unit tests for edge cases, gradual rollout |
| Performance regression | Benchmark before/after, optimize if needed |
| Disrupting active development | Coordinate with team, use feature branches |

---

## Conclusion

The repository has **significant but manageable code duplication**, primarily in:
- Country/metal normalization (~90 lines)
- Company resolution (~40 lines)
- Text utilities (~14 lines)
- Facility I/O (~50 lines)

**Recommended approach**: Phase 1 refactoring (high priority items) would eliminate the most problematic duplications and improve code quality with relatively low risk.

**Timeline**: ~8-12 hours for complete refactoring across all phases.
