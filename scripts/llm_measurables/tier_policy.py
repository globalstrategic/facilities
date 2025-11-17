"""
Tier Policy - Deterministic FCS → Tier → Count Mapping

Maps Facility Consequentiality Score (FCS) to query tiers with exact measurable counts
and cadences. Implements the business rules for tiered facility monitoring.

Tier Policy:
- Tier 4 (Platinum): FCS ≥ 90 → 500 measurables, daily
- Tier 3 (Gold): FCS 70-89 → 100 measurables, 3×/week
- Tier 2 (Silver): FCS 40-69 → 50 measurables, weekly
- Tier 1 (Bronze): FCS < 40 → 5 measurables, monthly

Usage:
    from scripts.llm_measurables.tier_policy import compute_tier, TIER_TARGETS, TIER_CADENCE

    tier = compute_tier(fcs=75.3)
    # Returns: 3
    count = TIER_TARGETS[tier]
    # Returns: 100
    cadence = TIER_CADENCE[tier]
    # Returns: "3x_week"
"""

from typing import Dict

# ============================================================================
# Tier Configuration
# ============================================================================

TIER_TARGETS: Dict[int, int] = {
    1: 5,      # Bronze: 5 measurables
    2: 50,     # Silver: 50 measurables
    3: 100,    # Gold: 100 measurables
    4: 500     # Platinum: 500 measurables
}

TIER_CADENCE: Dict[int, str] = {
    1: "monthly",     # Bronze: 1st of month
    2: "weekly",      # Silver: Monday
    3: "3x_week",     # Gold: Mon/Wed/Fri
    4: "daily"        # Platinum: Every day
}

TIER_FCS_THRESHOLDS: Dict[int, float] = {
    1: 0.0,    # Bronze: FCS < 40
    2: 40.0,   # Silver: FCS 40-69
    3: 70.0,   # Gold: FCS 70-89
    4: 90.0    # Platinum: FCS ≥ 90
}


# ============================================================================
# Core Functions
# ============================================================================

def compute_tier(fcs: float) -> int:
    """
    Map FCS to tier (1-4).

    Business rules:
    - FCS ≥ 90: Tier 4 (Platinum)
    - FCS 70-89: Tier 3 (Gold)
    - FCS 40-69: Tier 2 (Silver)
    - FCS < 40: Tier 1 (Bronze)

    Args:
        fcs: Facility Consequentiality Score (0-100)

    Returns:
        Tier number (1-4)

    Examples:
        >>> compute_tier(95.5)
        4
        >>> compute_tier(75.3)
        3
        >>> compute_tier(45.0)
        2
        >>> compute_tier(25.8)
        1
    """
    if fcs >= 90.0:
        return 4
    elif fcs >= 70.0:
        return 3
    elif fcs >= 40.0:
        return 2
    else:
        return 1


def get_tier_target_count(tier: int) -> int:
    """
    Get target measurable count for a tier.

    Args:
        tier: Tier number (1-4)

    Returns:
        Target measurable count

    Raises:
        ValueError: If tier is not 1-4
    """
    if tier not in TIER_TARGETS:
        raise ValueError(f"Invalid tier: {tier}. Must be 1-4.")
    return TIER_TARGETS[tier]


def get_tier_cadence(tier: int) -> str:
    """
    Get query cadence for a tier.

    Args:
        tier: Tier number (1-4)

    Returns:
        Cadence string (monthly, weekly, 3x_week, daily)

    Raises:
        ValueError: If tier is not 1-4
    """
    if tier not in TIER_CADENCE:
        raise ValueError(f"Invalid tier: {tier}. Must be 1-4.")
    return TIER_CADENCE[tier]


def tier_info(fcs: float) -> Dict[str, any]:
    """
    Get complete tier information for a given FCS.

    Args:
        fcs: Facility Consequentiality Score (0-100)

    Returns:
        Dict with tier, target_count, cadence, queries_per_month

    Example:
        >>> tier_info(75.3)
        {
            'fcs': 75.3,
            'tier': 3,
            'tier_name': 'Gold',
            'target_count': 100,
            'cadence': '3x_week',
            'queries_per_month': 12
        }
    """
    tier = compute_tier(fcs)
    target_count = TIER_TARGETS[tier]
    cadence = TIER_CADENCE[tier]

    # Compute queries per month based on cadence
    queries_per_month_map = {
        "daily": 30,
        "3x_week": 12,      # ~13 in reality, use 12 for conservative estimate
        "weekly": 4,
        "monthly": 1
    }

    tier_names = {
        1: "Bronze",
        2: "Silver",
        3: "Gold",
        4: "Platinum"
    }

    return {
        "fcs": fcs,
        "tier": tier,
        "tier_name": tier_names[tier],
        "target_count": target_count,
        "cadence": cadence,
        "queries_per_month": queries_per_month_map[cadence]
    }


if __name__ == "__main__":
    import doctest
    doctest.testmod()

    # Demo
    print("Tier Policy Demo")
    print("=" * 60)

    test_fcs_values = [95.5, 85.2, 75.3, 55.0, 45.0, 25.8, 10.0]

    for fcs in test_fcs_values:
        info = tier_info(fcs)
        print(f"\nFCS {fcs:5.1f} → Tier {info['tier']} ({info['tier_name']:8s})")
        print(f"  Target: {info['target_count']:3d} measurables")
        print(f"  Cadence: {info['cadence']:10s}")
        print(f"  Queries/month: {info['queries_per_month']:2d}")

    print("\n" + "=" * 60)
    print("\nTier Distribution Summary:")
    print(f"  Tier 4 (Platinum): FCS ≥ 90  → {TIER_TARGETS[4]:3d} measurables, {TIER_CADENCE[4]}")
    print(f"  Tier 3 (Gold):     FCS 70-89 → {TIER_TARGETS[3]:3d} measurables, {TIER_CADENCE[3]}")
    print(f"  Tier 2 (Silver):   FCS 40-69 → {TIER_TARGETS[2]:3d} measurables, {TIER_CADENCE[2]}")
    print(f"  Tier 1 (Bronze):   FCS < 40  → {TIER_TARGETS[1]:3d} measurables, {TIER_CADENCE[1]}")
