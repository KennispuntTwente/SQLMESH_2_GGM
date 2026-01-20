#!/usr/bin/env python3
"""
Generate a small, self-contained synthetic dataset that matches the staging tables
referenced by the silver models in models/silver/*.sql.

Outputs a folder of CSV files with lowercased column names that can be loaded
into a dev database to test the dlt + SQLMesh pipeline without a real source system.


Tables produced (minimal columns used by queries):
- szclient(clientnr, ind_gezag)
- wvbesl(besluitnr, clientnr)
- wvind_b(besluitnr, volgnr_ind, clientnr, dd_begin, dd_eind, volume, status_indicatie, kode_regeling)
- szregel(kode_regeling, omschryving)
- wvdos(besluitnr, volgnr_ind, uniek, kode_reden_einde_voorz)
- abc_refcod(code, domein, omschrijving)
- szukhis(uniekwvdos, bedrag, verslagnr)
- szwerker(kode_werker, naam, kode_instan, e_mail, ind_geslacht, toelichting, telefoon)

Usage:
  python scripts/synthetic/generate_synthetic_data.py --out data/synthetic --rows 5 --seed 42
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import random
import csv
from datetime import datetime, timedelta
from typing import Sequence


@dataclass
class GenConfig:
    rows: int = 10
    seed: int | None = 123


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_csv(
    path: Path, header: Sequence[str], rows: Sequence[Sequence[object]]
) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def generate(out_dir: Path, cfg: GenConfig) -> None:
    if cfg.seed is not None:
        random.seed(cfg.seed)

    _ensure_dir(out_dir)

    # Simple helpers
    def rand_date(start: datetime, days: int) -> datetime:
        return start + timedelta(days=random.randint(0, days))

    def email_for(name: str) -> str:
        return name.lower().replace(" ", ".") + "@example.org"

    # Base dimensions
    n_clients = max(2, cfg.rows)
    n_besluiten = max(2, cfg.rows)

    # szclient
    szclient_rows: list[list[object]] = []
    for i in range(1, n_clients + 1):
        ind_gezag = random.choice([0, 1])
        szclient_rows.append([i, ind_gezag])
    _write_csv(
        out_dir / "szclient.csv",
        ["clientnr", "ind_gezag"],
        szclient_rows,
    )

    # szregel (laws) – include JEUGDWET and another one
    szregel_rows = [
        [1, "JEUGDWET"],
        [2, "WMO"],
    ]
    _write_csv(out_dir / "szregel.csv", ["kode_regeling", "omschryving"], szregel_rows)

    # abc_refcod – reasons per domain
    abc_rows = [
        ["EINDE_JZG", "JZG_REDEN_EINDE_PRODUCT", "Einde JZG"],
        ["EINDE_WMO", "WVRTEIND", "Einde WMO"],
    ]
    _write_csv(out_dir / "abc_refcod.csv", ["code", "domein", "omschrijving"], abc_rows)

    # wvbesl (decisions) per client
    wvbesl_rows: list[list[object]] = []
    besluit_ids = []
    for i in range(1, n_besluiten + 1):
        client = random.randint(1, n_clients)
        besluitnr = 1000 + i
        besluit_ids.append((besluitnr, client))
        wvbesl_rows.append([besluitnr, client])
    _write_csv(out_dir / "wvbesl.csv", ["besluitnr", "clientnr"], wvbesl_rows)

    # wvind_b (indications) – link to wvbesl and szregel
    start0 = datetime(2024, 1, 1)
    wvind_rows: list[list[object]] = []
    for besluitnr, client in besluit_ids:
        for volgnr_ind in (1, 2):
            d0 = rand_date(start0, 60)
            d1 = d0 + timedelta(days=random.randint(14, 120))
            volume = random.choice([1, 5, 10, 20])
            status = random.choice(["ACTIEF", "GESLOTEN"])  # free text
            kode_regeling = random.choice([1, 2])
            wvind_rows.append(
                [
                    besluitnr,
                    volgnr_ind,
                    client,
                    d0.strftime("%Y-%m-%dT%H:%M:%S"),
                    d1.strftime("%Y-%m-%dT%H:%M:%S"),
                    volume,
                    status,
                    kode_regeling,
                ]
            )
    _write_csv(
        out_dir / "wvind_b.csv",
        [
            "besluitnr",
            "volgnr_ind",
            "clientnr",
            "dd_begin",
            "dd_eind",
            "volume",
            "status_indicatie",
            "kode_regeling",
        ],
        wvind_rows,
    )

    # wvdos – per besluit/volg pair, provide uniek + reason code matching abc_refcod
    wvdos_rows: list[list[object]] = []
    for besluitnr, _client in besluit_ids:
        for volgnr_ind in (1, 2):
            # Numeric unique ID: besluitnr * 10 + volgnr_ind (GGM DDL expects INTEGER)
            uniek = besluitnr * 10 + volgnr_ind
            # pick code to match szregel‑dependent join later; using both domains here
            reden_code = random.choice(["EINDE_JZG", "EINDE_WMO"])
            wvdos_rows.append([besluitnr, volgnr_ind, uniek, reden_code])
    _write_csv(
        out_dir / "wvdos.csv",
        ["besluitnr", "volgnr_ind", "uniek", "kode_reden_einde_voorz"],
        wvdos_rows,
    )

    # szukhis – generated with uniekwvdos MATCHING wvdos.uniek so Declaratieregel
    # produces joined rows and exercises the PK logic added to the mapping.
    szukhis_rows: list[list[object]] = []
    vers = 1
    for besluitnr, _client in besluit_ids:
        for volgnr_ind in (1, 2):
            # Match numeric pattern used in wvdos for deterministic PK derivation
            uniek = besluitnr * 10 + volgnr_ind
            bedrag = random.choice([25.0, 50.5, 75.25])
            szukhis_rows.append([uniek, bedrag, vers])
            vers += 1
    _write_csv(
        out_dir / "szukhis.csv", ["uniekwvdos", "bedrag", "verslagnr"], szukhis_rows
    )

    # szwerker – a few staff members
    names = [
        "Jan Jansen",
        "Piet Pieters",
        "Anna de Vries",
        "Sanne Bakker",
        "Luka Dev",
    ]
    szwerker_rows: list[list[object]] = []
    for i, nm in enumerate(names, start=1):
        szwerker_rows.append(
            [
                i,
                nm,
                random.choice(["JWG", "WMO", "BBZ"]),
                email_for(nm),
                random.choice(["M", "V", "X"]),
                "synthetic",
                f"06-{random.randint(10000000, 99999999)}",
            ]
        )
    _write_csv(
        out_dir / "szwerker.csv",
        [
            "kode_werker",
            "naam",
            "kode_instan",
            "e_mail",
            "ind_geslacht",
            "toelichting",
            "telefoon",
        ],
        szwerker_rows,
    )

    print(f"✔ Wrote synthetic CSVs -> {out_dir.resolve()}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate synthetic CSV dataset for staging tables"
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("data/synthetic"),
        help="Output directory for CSVs",
    )
    ap.add_argument(
        "--rows", type=int, default=10, help="Approximate size (clients/decisions)"
    )
    ap.add_argument(
        "--seed", type=int, default=123, help="Random seed for reproducibility"
    )
    args = ap.parse_args()

    cfg = GenConfig(rows=args.rows, seed=args.seed)
    generate(args.out, cfg)


if __name__ == "__main__":
    main()
