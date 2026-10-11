"""Fast unit tests for the parser and QC math (no services needed). The system tests are scripts/chaos.py."""
import hashlib

import pytest

from opp import elispot


def test_export_is_deterministic():
    a, b = elispot.make_export("PLT-0001", 1), elispot.make_export("PLT-0001", 1)
    assert hashlib.sha256(a).digest() == hashlib.sha256(b).digest()


def test_parse_96_wells_and_meta():
    c = elispot.make_export("PLT-0042", 42)
    assert len(elispot.parse(c)) == 96
    m = elispot.read_meta(c)
    assert (m.plate_id, m.instrument_id) == ("PLT-0042", "RDR-01")


def test_truncated_file_is_malformed():
    c = elispot.make_export("PLT-0001", 1)
    with pytest.raises(elispot.MalformedExport):
        elispot.parse(c[: len(c) // 2])


def test_bad_well_is_malformed():
    c = elispot.make_export("PLT-0001", 1).replace(b"\nC7,", b"\nC7x,")
    with pytest.raises(elispot.MalformedExport):
        elispot.parse(c)


def rows(neg, ag, pos, donor="D-1"):
    out = []
    for cond, vals in (("NEG", neg), ("AG1", ag), ("AG2", ag), ("POS", pos)):
        out += [{"Well": "x", "SampleID": donor, "Condition": cond, "SpotCount": str(v)} for v in vals]
    return out


@pytest.mark.parametrize("neg,ag,pos,want", [
    ([5, 5, 5], [40, 42, 38], [300, 310, 290], "Positive"),
    ([5, 5, 5], [6, 5, 7], [300, 310, 290], "Negative"),
    ([5, 5, 5], [13, 14, 13], [300, 310, 290], "Indeterminate"),
    ([5, 5, 5], [10, 60, 30], [300, 310, 290], "QC Fail"),          # replicate CV too high
    ([80, 75, 85], [200, 210, 190], [300, 310, 290], "QC Fail"),    # high background
    ([5, 5, 5], [40, 42, 38], [40, 50, 45], "QC Fail"),             # weak positive control
    ([5, 5, 5], ["TNTC", "TNTC", 480], [300, 310, 290], "Indeterminate"),
])
def test_qc_verdicts(neg, ag, pos, want):
    assert elispot.qc(rows(neg, ag, pos))["calls"]["D-1|AG1"]["verdict"] == want
