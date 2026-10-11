"""Synthetic 96-well ELISpot export, its parser, and a deterministic QC step.

The format is invented for this demo. Layout: rows A to H are eight donors; columns 1-3 are the
negative control (medium), 4-6 antigen 1, 7-9 antigen 2, 10-12 the positive control (mitogen).
Thresholds are illustrative, not clinical guidance.
"""
from __future__ import annotations

import csv
import io
import random
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

ROWS = "ABCDEFGH"
COLS = range(1, 13)
WELLS = [f"{r}{c}" for r in ROWS for c in COLS]
CONDITIONS = {1: "NEG", 2: "NEG", 3: "NEG", 4: "AG1", 5: "AG1", 6: "AG1",
              7: "AG2", 8: "AG2", 9: "AG2", 10: "POS", 11: "POS", 12: "POS"}
TNTC = "TNTC"
TNTC_AT = 500          # counts at or above this read as too numerous to count
LLOQ = 10              # spots per well; below this a net response is not quantifiable
MAX_CV = 0.30          # replicate CV above this fails QC (when the mean is above LLOQ)
MAX_BACKGROUND = 50    # negative control mean above this fails the donor
MIN_POSITIVE_CTRL = 100


class MalformedExport(ValueError):
    pass


@dataclass
class Meta:
    plate_id: str
    instrument_id: str
    exported_at: datetime | None


def make_export(plate_id: str, seed: int, instrument_id: str = "RDR-01",
                exported_at: datetime | None = None, tweak: dict[str, int] | None = None) -> bytes:
    """Deterministic synthetic export. `tweak` overrides spot counts for given wells (a re-export)."""
    rng = random.Random(seed)
    exported_at = exported_at or datetime(2026, 10, 12, 18, 10, tzinfo=timezone.utc) + timedelta(minutes=seed % 600)
    out = io.StringIO()
    out.write("# Synthetic ELISpot reader export (demo format, not a real instrument)\n")
    out.write(f"# InstrumentID: {instrument_id}\n# PlateID: {plate_id}\n# Assay: IFN-g ELISpot\n")
    out.write(f"# ExportedAt: {exported_at.isoformat().replace('+00:00', 'Z')}\n")
    w = csv.writer(out, lineterminator="\n")
    w.writerow(["Well", "SampleID", "Condition", "SpotCount"])
    for r_i, r in enumerate(ROWS):
        donor = f"D-{plate_id[-4:]}-{r_i + 1:02d}"
        background = rng.choice([2, 4, 6, 8, 12, 20, 70 if rng.random() < 0.08 else 5])
        ag1 = background + rng.choice([0, 1, 3, 15, 40, 120, 9])
        ag2 = background + rng.choice([0, 2, 25, 60, 7, 700 if rng.random() < 0.1 else 30])
        pos = rng.choice([300, 450, 800, 60 if rng.random() < 0.08 else 350])
        noisy = rng.random() < 0.1
        for c in COLS:
            cond = CONDITIONS[c]
            base = {"NEG": background, "AG1": ag1, "AG2": ag2, "POS": pos}[cond]
            spread = 0.6 if (noisy and cond == "AG1") else 0.12
            count = max(0, int(round(base * (1 + rng.uniform(-spread, spread)))))
            well = f"{r}{c}"
            if tweak and well in tweak:
                count = tweak[well]
            w.writerow([well, donor, cond, TNTC if count >= TNTC_AT else count])
    out.write(f"# END wells={len(WELLS)}\n")
    return out.getvalue().encode()


def read_meta(content: bytes) -> Meta | None:
    """Header fields only. None if the header is unreadable."""
    try:
        text = content[:2048].decode("utf-8")
    except UnicodeDecodeError:
        return None
    head = {}
    for line in text.splitlines():
        if line.startswith("# ") and ":" in line:
            k, v = line[2:].split(":", 1)
            head[k.strip()] = v.strip()
    if "PlateID" not in head or "InstrumentID" not in head:
        return None
    exported = None
    if head.get("ExportedAt"):
        try:
            exported = datetime.fromisoformat(head["ExportedAt"].replace("Z", "+00:00"))
        except ValueError:
            exported = None
    return Meta(head["PlateID"], head["InstrumentID"], exported)


def parse(content: bytes) -> list[dict]:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as e:
        raise MalformedExport("not UTF-8 text") from e
    lines = text.splitlines()
    if not lines or not lines[-1].startswith("# END"):
        raise MalformedExport("no end marker (file incomplete or not this format)")
    body = [ln for ln in lines if ln and not ln.startswith("#")]
    rows = list(csv.DictReader(body))
    if not rows or set(rows[0]) != {"Well", "SampleID", "Condition", "SpotCount"}:
        raise MalformedExport("unexpected columns")
    wells = [r["Well"] for r in rows]
    if sorted(wells) != sorted(WELLS) or len(set(wells)) != 96:
        raise MalformedExport(f"expected 96 unique wells A1..H12, got {len(set(wells))}")
    for r in rows:
        v = r["SpotCount"]
        if v != TNTC and not v.isdigit():
            raise MalformedExport(f"bad spot count {v!r} in {r['Well']}")
    return rows


def _num(v: str) -> float:
    return float(TNTC_AT) if v == TNTC else float(v)


def _cv(xs: list[float]) -> float:
    m = statistics.mean(xs)
    return statistics.pstdev(xs) / m if m else 0.0


def qc(rows: list[dict]) -> dict:
    """Per donor and antigen: Positive, Negative, Indeterminate, or QC Fail. Pure function of the rows."""
    by = {}
    for r in rows:
        by.setdefault((r["SampleID"], r["Condition"]), []).append(r["SpotCount"])
    calls = {}
    for donor in sorted({k[0] for k in by}):
        neg = [_num(v) for v in by[(donor, "NEG")]]
        pos_raw = by[(donor, "POS")]
        pos = [_num(v) for v in pos_raw]
        donor_fail = None
        if statistics.mean(neg) > MAX_BACKGROUND:
            donor_fail = "high background"
        elif TNTC not in pos_raw and statistics.mean(pos) < MIN_POSITIVE_CTRL:
            donor_fail = "weak positive control"
        for ag in ("AG1", "AG2"):
            raw = by[(donor, ag)]
            xs = [_num(v) for v in raw]
            mean_ag, mean_neg = statistics.mean(xs), statistics.mean(neg)
            if donor_fail:
                verdict, why = "QC Fail", donor_fail
            elif TNTC in raw:
                verdict, why = "Indeterminate", "TNTC, rerun at lower cell density"
            elif mean_ag >= LLOQ and _cv(xs) > MAX_CV:
                verdict, why = "QC Fail", f"replicate CV {_cv(xs):.0%}"
            else:
                net, ratio = mean_ag - mean_neg, mean_ag / max(mean_neg, 1.0)
                if net >= LLOQ and ratio >= 2:
                    verdict, why = "Positive", f"net {net:.0f} spots, {ratio:.1f}x background"
                elif net < LLOQ / 2 or ratio < 1.5:
                    verdict, why = "Negative", f"net {net:.0f} spots"
                else:
                    verdict, why = "Indeterminate", f"net {net:.0f} spots, near LLOQ"
            calls[f"{donor}|{ag}"] = {"verdict": verdict, "why": why}
    wells = []
    for r in rows:
        cond = r["Condition"]
        if cond in ("AG1", "AG2"):
            verdict = calls[f"{r['SampleID']}|{cond}"]["verdict"]
        else:
            verdict = "control"
        wells.append({"well": r["Well"], "sample_id": r["SampleID"], "condition": cond,
                      "spot_count": r["SpotCount"], "verdict": verdict})
    tally = {}
    for c in calls.values():
        tally[c["verdict"]] = tally.get(c["verdict"], 0) + 1
    return {"calls": calls, "wells": wells, "tally": tally}
