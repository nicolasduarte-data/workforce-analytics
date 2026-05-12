"""
HR dataset generator — synthetic but realistically messy.

Produces three CSVs in ../data/raw/:
  - positions.csv    (position catalog: roles, salary bands, headcount targets)
  - employees.csv    (employee master: names, depts, salaries, managers)
  - hr_tickets.csv   (service tickets: categories, dates, assigned_to)

The messiness is HR-specific and deliberate — see inject_messiness() for the
eight patterns this simulates.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

# --- reproducibility --------------------------------------------------------
# Seeding both Faker AND Python's random module means every run produces the
# same dataset. Critical for a portfolio project: reviewers who clone the repo
# see exactly the same numbers we describe in the README.
SEED = 42
fake = Faker()
Faker.seed(SEED)
random.seed(SEED)  # Python's random module needs its own seed — Faker.seed() doesn't cover it.

# Locations reflect a US-centered company with some remote + international presence.
# Hardcoded (not Faker) because real orgs cluster in specific cities — not scattered randomly.
LOCATIONS = ["Austin", "New York", "San Francisco", "Remote-US", "Remote-EU", "London", "Toronto"]
LOCATION_WEIGHTS = [0.20, 0.18, 0.15, 0.25, 0.08, 0.08, 0.06]  # sums to 1.0

# Canonical country code per location. Will be made inconsistent in messiness pass.
LOCATION_TO_COUNTRY = {
    "Austin": "US", "New York": "US", "San Francisco": "US", "Remote-US": "US",
    "London": "UK", "Remote-EU": "UK", "Toronto": "CA",
}

# Reference date — the "today" of our synthetic world. Everything dates from here.
TODAY = date(2025, 12, 31)

# Output folder, resolved relative to THIS file (not the caller's CWD).
# Using __file__ makes the script runnable from anywhere — no "cd first" footgun.
OUT_DIR = Path(__file__).parent.parent / "data" / "raw"


# --- positions table --------------------------------------------------------
# A Position is a ROLE, not a person. Multiple employees can hold the same
# position_id (e.g., 40 people are "Software Engineer II"). This separation
# of role-vs-person is how real HRIS systems model org structure.
@dataclass(frozen=True)
class Position:
    position_id: str
    dept: str
    level: str          # IC1, IC2, Senior, Staff, Manager, Director
    band_min: int       # annual salary floor (USD)
    band_max: int       # annual salary ceiling
    headcount_target: int


# Hand-curated catalog. 18 positions across 8 departments.
# Bands reflect realistic 2025 US tech-company compensation at mid-tier companies.
# Using frozen dataclass + explicit catalog = this data is DESIGN, not generation.
POSITIONS: list[Position] = [
    # Executive layer — the top of the tree. These are the legitimate NULL-manager cases.
    Position("EXEC",     "Executive",        "Director", 220_000,  400_000,  3),  # CEO + 2 VPs
    # Engineering — ~45% of headcount (realistic for a tech company)
    Position("ENG-IC1",  "Engineering",      "IC1",       85_000,  110_000, 35),
    Position("ENG-IC2",  "Engineering",      "IC2",      110_000,  140_000, 65),
    Position("ENG-SR",   "Engineering",      "Senior",   140_000,  180_000, 48),
    Position("ENG-STAFF","Engineering",      "Staff",    180_000,  230_000, 15),
    Position("ENG-MGR",  "Engineering",      "Manager",  170_000,  210_000, 18),
    # Product — small relative to Eng (classic tech ratio, ~1:4)
    Position("PM-IC2",   "Product",          "IC2",      115_000,  145_000, 20),
    Position("PM-SR",    "Product",          "Senior",   145_000,  180_000, 15),
    Position("PM-MGR",   "Product",          "Manager",  175_000,  215_000,  6),
    # Data
    Position("DATA-IC2", "Data",             "IC2",      110_000,  140_000, 20),
    Position("DATA-SR",  "Data",             "Senior",   140_000,  175_000, 12),
    Position("DATA-MGR", "Data",             "Manager",  160_000,  200_000,  3),
    # Customer Success — second largest, typical for B2B SaaS
    Position("CS-IC1",   "Customer Success", "IC1",       65_000,   85_000, 32),
    Position("CS-IC2",   "Customer Success", "IC2",       85_000,  110_000, 28),
    Position("CS-MGR",   "Customer Success", "Manager",  120_000,  150_000,  8),
    # Sales
    Position("SALES-IC2","Sales",            "IC2",       90_000,  120_000, 33),
    Position("SALES-SR", "Sales",            "Senior",   120_000,  160_000, 18),
    Position("SALES-MGR","Sales",            "Manager",  150_000,  190_000,  5),
    # HR, Finance, Marketing — thin back-office depts now get proper managers
    Position("HR-IC2",   "HR",               "IC2",       75_000,   95_000,  9),
    Position("HR-MGR",   "HR",               "Manager",  110_000,  140_000,  2),
    Position("FIN-IC2",  "Finance",          "IC2",       85_000,  110_000,  8),
    Position("FIN-MGR",  "Finance",          "Manager",  130_000,  165_000,  2),
    Position("MKT-IC2",  "Marketing",        "IC2",       80_000,  105_000, 12),
    Position("MKT-MGR",  "Marketing",        "Manager",  125_000,  155_000,  2),
]

# --- baked-in analytical signals --------------------------------------------
# These are the FINDINGS the notebook is supposed to uncover. They're baseline
# truth, not messiness. Messiness obscures things; signals are what analysis
# recovers. Keeping them separate matters.

# Comp equity signal: 4 depts are systematically underpaid (mode biased toward
# band_min). Matches the README narrative ("4 departments >12% below midpoint").
LOW_PAID_DEPTS = {"HR", "Finance", "Customer Success", "Marketing"}

# Resolution time per category: mode, max (triangular distribution, min=1).
# HR Technology is the slow-resolution bucket (matches README: "3× slower").
# Policy & Benefits is the fast bucket. This asymmetry creates the SQL finding.
CATEGORY_RESOLUTION = {
    "HR Technology":          (30, 120),   # complex system issues, long tail
    "Policy & Benefits":      ( 3,  30),   # fast lookups
    "Payroll":                ( 5,  60),
    "Onboarding":             ( 7,  45),
    "Learning & Development": (15,  90),
    "Employee Relations":     (20,  90),
    "Offboarding":            (10,  30),
    "Compensation":           (14,  60),
}

# Admin-error NULL rate at baseline — mimics sloppy data entry.
BASELINE_MGR_NULL_RATE = 0.04


def build_positions_df() -> pd.DataFrame:
    """Convert the POSITIONS catalog into a DataFrame with an effective_date column.

    effective_date is the SCD2 (Slowly Changing Dimension type 2) hook — it lets
    us later add a mid-year rebanding row to simulate the 'bands changed mid-year'
    messiness pattern from the spec. For now, all rows share the same date.
    """
    df = pd.DataFrame([p.__dict__ for p in POSITIONS])
    df["effective_date"] = "2025-01-01"
    return df


# --- employees table --------------------------------------------------------
# Two-pass generation:
#   Pass 1: generate all Manager-level employees (they have no manager themselves)
#   Pass 2: generate ICs, each assigned a random Manager FROM THE SAME DEPT
#
# Why this matters: foreign keys require the referenced row to exist first.
# If you generate ICs first, there's no manager to point to. This pattern —
# generate the "parent" entities before the "child" ones — shows up in every
# relational data generator.

def _random_hire_date(tenure_years_max: int = 6) -> date:
    """Pick a hire date between `tenure_years_max` years ago and today.

    Uses a triangular distribution skewed toward recent dates — real companies
    have more recent hires than old ones because of attrition. A uniform
    distribution would overrepresent long-tenured employees.
    """
    days_back = int(random.triangular(0, tenure_years_max * 365, tenure_years_max * 100))
    return TODAY - timedelta(days=days_back)


def _random_salary(band_min: int, band_max: int, dept: str) -> int:
    """Draw a salary from a band. Mode is midpoint EXCEPT for LOW_PAID_DEPTS.

    LOW_PAID_DEPTS have mode biased toward the 25th percentile of the band —
    this is the comp equity SIGNAL the analysis notebook is supposed to surface.
    Rounded to nearest $500 because no real HR system stores $87,342.17.
    """
    if dept in LOW_PAID_DEPTS:
        # Uniform draw from bottom 10% of band. Triangular can't push median far
        # enough below midpoint on narrow bands — uniform-near-floor does.
        upper = band_min + (band_max - band_min) * 0.10
        raw = random.uniform(band_min, upper)
    else:
        # Fair comp: triangular centered at midpoint, so median sits near midpoint.
        mode = (band_min + band_max) / 2
        raw = random.triangular(band_min, band_max, mode)
    return int(round(raw / 500) * 500)


def _make_employee_row(emp_id: str, position: Position, manager_id: str | None) -> dict:
    """Build a single employee record as a dict. dict → DataFrame in one step at the end."""
    first = fake.first_name()
    last = fake.last_name()
    location = random.choices(LOCATIONS, weights=LOCATION_WEIGHTS, k=1)[0]
    return {
        "employee_id": emp_id,
        "first_name": first,
        "last_name": last,
        # lowercase + replace spaces — realistic corporate email pattern
        "email": f"{first.lower()}.{last.lower()}@fakeco.com".replace(" ", ""),
        "position_id": position.position_id,
        "dept": position.dept,
        "location": location,
        "country": LOCATION_TO_COUNTRY[location],
        "hire_date": _random_hire_date(),
        "status": "Active",              # most are Active; we'll flip some to Terminated in messiness pass
        "termination_date": None,
        "salary": _random_salary(position.band_min, position.band_max, position.dept),
        "manager_id": manager_id,
    }


def build_employees_df() -> pd.DataFrame:
    """Generate employees using a three-tier pattern: Executives → Managers → ICs.

    Tier 1 (Executives): NULL manager_id — the legitimate top of the tree.
    Tier 2 (Managers):   report up to a random Executive (cross-dept is fine;
                         VPs often manage leaders across multiple departments).
    Tier 3 (ICs):        report to a random Manager IN THEIR OWN DEPT.

    After generation we apply two hygiene steps:
      1. Inject ~4% admin-error NULLs on random non-Executive employees.
      2. Shuffle the rows so the output doesn't start with a wall of Execs.
    """
    employees: list[dict] = []
    emp_counter = 1

    def next_id() -> str:
        nonlocal emp_counter
        eid = f"EMP-{emp_counter:05d}"
        emp_counter += 1
        return eid

    # --- Pass 1: Executives (no manager above them) --------------------------
    executive_ids: list[str] = []
    for pos in POSITIONS:
        if pos.dept != "Executive":
            continue
        for _ in range(pos.headcount_target):
            eid = next_id()
            employees.append(_make_employee_row(eid, pos, manager_id=None))
            executive_ids.append(eid)

    # --- Pass 2: Managers (report to a random Exec) --------------------------
    manager_ids_by_dept: dict[str, list[str]] = {}
    for pos in POSITIONS:
        if pos.level != "Manager":
            continue
        for _ in range(pos.headcount_target):
            eid = next_id()
            mgr = random.choice(executive_ids)
            employees.append(_make_employee_row(eid, pos, manager_id=mgr))
            manager_ids_by_dept.setdefault(pos.dept, []).append(eid)

    # --- Pass 3: ICs (report to a same-dept Manager) -------------------------
    for pos in POSITIONS:
        if pos.level in ("Manager", "Director") or pos.dept == "Executive":
            continue
        available_managers = manager_ids_by_dept.get(pos.dept, [])
        for _ in range(pos.headcount_target):
            eid = next_id()
            mgr = random.choice(available_managers) if available_managers else None
            employees.append(_make_employee_row(eid, pos, manager_id=mgr))

    df = pd.DataFrame(employees)

    # --- Admin-error NULL manager_id on ~4% of non-Executive employees -------
    # Mimics data-entry sloppiness: a few records where the admin forgot to set
    # the reporting relationship. Excluded: Executives (legitimately NULL already).
    non_exec = df[df["dept"] != "Executive"].index
    n_null = int(len(non_exec) * BASELINE_MGR_NULL_RATE)
    null_idx = random.sample(list(non_exec), k=n_null)
    df.loc[null_idx, "manager_id"] = None

    # --- Shuffle rows so output doesn't start with Execs ---------------------
    # sample(frac=1) = shuffle all rows. reset_index drops the now-scrambled
    # index. random_state ties this shuffle to our global seed for reproducibility.
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

    return df


# --- hr_tickets table -------------------------------------------------------
# Tickets are ServiceNow-style HR service records. Each one:
#   - Was raised by an employee (FK: employee_id)
#   - Falls into a category (Payroll, Benefits, HR Tech, etc.)
#   - Has an open_date and (usually) a close_date
#   - Is assigned to someone on the HR team
#
# The category VOLUMES are deliberately uneven — real HR ops show a Pareto
# distribution (HR Tech and Payroll dominate). Uniform distribution would
# betray the generator.

TICKET_CATEGORIES_WEIGHTED = [
    ("HR Technology",          0.25),  # access requests, system bugs — always the biggest bucket
    ("Payroll",                0.18),  # paycheck issues, tax questions
    ("Policy & Benefits",      0.15),  # PTO, healthcare questions
    ("Onboarding",             0.12),  # new-hire paperwork
    ("Learning & Development", 0.10),  # training requests
    ("Employee Relations",     0.08),  # sensitive — conflicts, complaints
    ("Offboarding",            0.07),  # exit paperwork
    ("Compensation",           0.05),  # the smallest bucket but highest-friction
]

# Status distribution: most tickets close; a small tail stays open/in-progress.
TICKET_STATUS_WEIGHTED = [("Closed", 0.85), ("In Progress", 0.12), ("Open", 0.03)]

TICKET_TOTAL = 2_800  # target volume from the project spec


def _pick_weighted(choices: list[tuple[str, float]]) -> str:
    """Pick one item from a [(label, weight), ...] list using the weights."""
    labels, weights = zip(*choices)
    return random.choices(labels, weights=weights, k=1)[0]


def _random_open_date(window_days: int = 365) -> date:
    """Ticket opened within the last `window_days` (default: 1 year back from TODAY)."""
    return TODAY - timedelta(days=random.randint(0, window_days))


def _random_resolution_days(category: str) -> int:
    """Days between open and close, varying by category.

    CATEGORY_RESOLUTION defines (mode, max) per category. Triangular distribution
    with min=1 produces right-skewed durations — the long tail is where SLA
    breaches live. HR Technology has mode=30 (slow), Policy & Benefits has mode=3
    (fast). That asymmetry is the finding the SQL showcase surfaces.
    """
    mode, max_days = CATEGORY_RESOLUTION[category]
    return int(random.triangular(1, max_days, mode))


def build_tickets_df(employees_df: pd.DataFrame) -> pd.DataFrame:
    """Generate hr_tickets referencing the already-built employees table.

    Why take employees_df as a parameter instead of calling build_employees_df()
    internally: we want ONE source of truth for employees. If tickets regenerated
    their own employees, ticket.employee_id wouldn't match employees.employee_id.
    """
    # Only Active employees can raise tickets (in the baseline — messiness will
    # break this invariant later). We also need assignees — draw from HR dept.
    active_employee_ids = employees_df.loc[
        employees_df["status"] == "Active", "employee_id"
    ].tolist()
    hr_team_ids = employees_df.loc[
        employees_df["dept"] == "HR", "employee_id"
    ].tolist()

    # Valid position IDs for ticket references (only role-linked ticket categories).
    all_position_ids = [p.position_id for p in POSITIONS]

    tickets: list[dict] = []
    for i in range(1, TICKET_TOTAL + 1):
        open_dt = _random_open_date()
        status = _pick_weighted(TICKET_STATUS_WEIGHTED)
        category = _pick_weighted(TICKET_CATEGORIES_WEIGHTED)

        # position_id is only populated when the category is role-related.
        # Policy/Payroll tickets don't reference a position — Onboarding/Comp do.
        if category in {"Onboarding", "Offboarding", "Compensation", "Learning & Development"}:
            position_id = random.choice(all_position_ids)
        else:
            position_id = None

        # Only Closed tickets have a close_date. Open/In Progress: None.
        if status == "Closed":
            close_dt = open_dt + timedelta(days=_random_resolution_days(category))
            # Guard: a closed ticket's close_date could fall after TODAY in our
            # simulated world. Cap it at TODAY to stay realistic.
            if close_dt > TODAY:
                close_dt = TODAY
        else:
            close_dt = None

        tickets.append({
            "ticket_id": f"TKT-{i:06d}",                # TKT-000001 ... TKT-002800
            "employee_id": random.choice(active_employee_ids),
            "position_id": position_id,
            "category": category,
            "status": status,
            "open_date": open_dt,
            "close_date": close_dt,
            "assigned_to": random.choice(hr_team_ids) if hr_team_ids else None,
            # Faker's .sentence() produces plausible one-liners — good enough
            # for resolution notes. Real ones would be longer, but this keeps
            # the CSV readable and the analysis focused on structured fields.
            "resolution_notes": fake.sentence(nb_words=8) if status == "Closed" else None,
        })

    return pd.DataFrame(tickets)


# --- messiness injection ----------------------------------------------------
# Eight patterns, each a realistic HR data quality failure mode. Each is a
# TARGETED mutation: small percentage, recognizable pattern, recoverable in
# analysis. Random noise would be cheap; these are domain-authentic.

PHANTOM_POSITION_IDS = ["DEL-ROLE-017", "DEL-ROLE-023", "DEL-ROLE-041"]

LEGACY_CATEGORY_MAP = {
    "HR Technology":          "HR Tech",
    "Policy & Benefits":      "Benefits",
    "Learning & Development": "L&D",
    "Employee Relations":     "ER",
}

COUNTRY_VARIANTS = {
    "US": ["US", "USA", "United States", "U.S."],
    "UK": ["UK", "GB", "United Kingdom", "England"],
    "CA": ["CA", "CAN", "Canada"],
}


def inject_messiness(
    positions_df: pd.DataFrame,
    employees_df: pd.DataFrame,
    tickets_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply all 8 HR-specific messiness patterns. Returns mutated copies.

    Operates on COPIES so the originals stay pristine — useful when debugging
    because you can diff 'clean vs messy' to see exactly what was injected.
    """
    pos = positions_df.copy()
    emp = employees_df.copy()
    tix = tickets_df.copy()

    # --- (1) Orphan position_ids in tickets -----------------------------------
    # Simulates: positions were deleted during an org restructure but tickets
    # referencing them weren't cleaned up. ~15% of ticket position_ids become
    # phantom IDs that don't exist in the positions catalog.
    has_pos = tix[tix["position_id"].notna()].index
    n_orphan = int(len(has_pos) * 0.15)
    orphan_idx = random.sample(list(has_pos), k=n_orphan)
    tix.loc[orphan_idx, "position_id"] = [random.choice(PHANTOM_POSITION_IDS) for _ in range(n_orphan)]

    # --- (2) Rehire duplicates -----------------------------------------------
    # ~2% of employees "rehired" — generates a new row with SAME first+last name,
    # new employee_id, later hire_date. Duplicate detection belongs in the
    # cleaning notebook (match on first_name+last_name, keep most recent).
    n_rehire = int(len(emp) * 0.02)
    rehire_sample = emp.sample(n=n_rehire, random_state=SEED)
    next_id_num = int(emp["employee_id"].str.replace("EMP-", "").astype(int).max()) + 1
    rehire_rows = []
    for _, original in rehire_sample.iterrows():
        rehire = original.copy()
        rehire["employee_id"] = f"EMP-{next_id_num:05d}"
        rehire["hire_date"] = TODAY - timedelta(days=random.randint(30, 365))
        next_id_num += 1
        rehire_rows.append(rehire)
    emp = pd.concat([emp, pd.DataFrame(rehire_rows)], ignore_index=True)

    # --- (3) Band range changed mid-year (SCD2) -------------------------------
    # 3 positions got rebanded on 2025-07-01 — same position_id, new band ranges,
    # new effective_date. Analyst must pick the ROW WITH LATEST effective_date
    # per position_id when joining. A join without this logic produces wrong answers.
    rebanded_ids = random.sample([p.position_id for p in POSITIONS], k=3)
    new_rows = []
    for pid in rebanded_ids:
        original_row = pos[pos["position_id"] == pid].iloc[0].copy()
        # Rebanding shifts the band up ~8% — typical cost-of-living adjustment.
        original_row["band_min"] = int(original_row["band_min"] * 1.08)
        original_row["band_max"] = int(original_row["band_max"] * 1.08)
        original_row["effective_date"] = "2025-07-01"
        new_rows.append(original_row)
    pos = pd.concat([pos, pd.DataFrame(new_rows)], ignore_index=True)

    # --- (4) Legacy ticket category names -------------------------------------
    # ~2% of tickets use old taxonomy labels ("HR Tech" instead of "HR Technology").
    # Cleaning = mapping old → new. A careless GROUP BY category produces split
    # counts until the taxonomy is unified.
    n_legacy = int(len(tix) * 0.02)
    legacy_idx = random.sample(list(tix.index), k=n_legacy)
    for idx in legacy_idx:
        current = tix.at[idx, "category"]
        if current in LEGACY_CATEGORY_MAP:
            tix.at[idx, "category"] = LEGACY_CATEGORY_MAP[current]

    # --- (5) Inconsistent country codes ---------------------------------------
    # ~20% of employees get a non-canonical country code variant. Cleaning =
    # normalization to ISO codes. Classic cross-system-integration artifact.
    n_inconsistent = int(len(emp) * 0.20)
    inconsistent_idx = random.sample(list(emp.index), k=n_inconsistent)
    for idx in inconsistent_idx:
        canonical = emp.at[idx, "country"]
        if canonical in COUNTRY_VARIANTS:
            emp.at[idx, "country"] = random.choice(COUNTRY_VARIANTS[canonical])

    # --- (6) close_date before open_date --------------------------------------
    # Data entry errors: ~3% of closed tickets have their dates inverted. Common
    # in manually-maintained ticketing systems. Cleaning = WHERE close >= open.
    closed = tix[tix["status"] == "Closed"].index
    n_bad_dates = int(len(closed) * 0.03)
    bad_date_idx = random.sample(list(closed), k=n_bad_dates)
    for idx in bad_date_idx:
        # Swap: close becomes earlier than open.
        tix.at[idx, "close_date"] = tix.at[idx, "open_date"] - timedelta(days=random.randint(1, 14))

    # --- (7) NULL manager_id injection (7% MORE on top of baseline 4%) --------
    # Total NULL will land around 11%. Baseline was "admin errors"; this layer
    # simulates a broader data hygiene failure following an org restructure.
    # Excludes Executives and rows already NULL.
    eligible = emp[(emp["dept"] != "Executive") & emp["manager_id"].notna()].index
    n_more_null = int(len(emp) * 0.07)
    more_null_idx = random.sample(list(eligible), k=n_more_null)
    emp.loc[more_null_idx, "manager_id"] = None

    # --- (8) Salary outliers --------------------------------------------------
    # ~2% of employees have a salary WAY out of their band — off-cycle
    # adjustments or plain data entry errors. Outlier detection (IQR / z-score)
    # is an expected cleaning step in the notebook.
    n_outliers = int(len(emp) * 0.02)
    outlier_idx = random.sample(list(emp.index), k=n_outliers)
    for idx in outlier_idx:
        # 50/50 above or below band. Magnitude: 30-60% of current salary.
        factor = random.uniform(0.30, 0.60)
        direction = random.choice([-1, 1])
        emp.at[idx, "salary"] = int(emp.at[idx, "salary"] * (1 + direction * factor))

    return pos, emp, tix


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Build clean baseline tables (the "ground truth").
    positions_df = build_positions_df()
    employees_df = build_employees_df()
    tickets_df = build_tickets_df(employees_df)

    # Step 2: Inject HR-specific messiness on top. Returns new copies.
    positions_df, employees_df, tickets_df = inject_messiness(
        positions_df, employees_df, tickets_df
    )

    # Step 3: Write the messy raw CSVs. These are what the notebook INGESTS —
    # the clean DataFrames never hit disk. That's the whole point: the notebook
    # has to earn the clean version.
    positions_df.to_csv(OUT_DIR / "positions.csv", index=False)
    print(f"Wrote positions.csv   — {len(positions_df)} rows")
    employees_df.to_csv(OUT_DIR / "employees.csv", index=False)
    print(f"Wrote employees.csv   — {len(employees_df)} rows")
    tickets_df.to_csv(OUT_DIR / "hr_tickets.csv", index=False)
    print(f"Wrote hr_tickets.csv  — {len(tickets_df)} rows")
