#!/usr/bin/env python3
"""Build a source snapshot of the current implementation and strictly validate isolated replays."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
SCENARIOS = {
    "normal": (1, False, 0, 1),
    "flood": (20, True, 0, 1),
    "bulk96": (1, False, 96, 1),
    "duplicate-finals": (20, True, 0, 100),
    "soak": (1, False, 0, 1),
}


def run(cmd, **kwargs):
    return subprocess.run(list(map(str, cmd)), check=True, **kwargs)


def prepare(root, java_home):
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise SystemExit("--prepare requires an empty work directory")
    for folder in ("source", "classes", "harness", "lib", "results", "jfr"):
        (root / folder).mkdir()
    shutil.copy2(REPO / "pom.xml", root / "source/pom.xml")
    shutil.copytree(REPO / "src/main", root / "source/src/main")
    shutil.copy2(HERE / "src/ReplayHarness.java", root / "harness/ReplayHarness.java")
    manifest = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted((root / "source").rglob("*")) if p.is_file()}
    manifest["harness/ReplayHarness.java"] = hashlib.sha256((root / "harness/ReplayHarness.java").read_bytes()).hexdigest()
    (root / "source-manifest.json").write_text(json.dumps(manifest, indent=2))
    with (root / "build.log").open("w") as log:
        run(["mvn", "-B", "-DskipTests", "compile", "dependency:build-classpath",
             "-Dmdep.includeScope=test", f"-Dmdep.outputFile={root}/classpath.txt"],
            cwd=root / "source", env=dict(os.environ, JAVA_HOME=str(java_home)),
            stdout=log, stderr=subprocess.STDOUT)
    for index, name in enumerate((root / "classpath.txt").read_text().strip().split(os.pathsep)):
        path = Path(name)
        shutil.copy2(path, root / "lib" / f"{index:03d}-{path.name}")
    cp = os.pathsep.join(map(str, [root / "source/target/classes", root / "lib/*"]))
    run([java_home / "bin/javac", "-proc:none", "-cp", cp, "-d", root / "classes",
         root / "harness/ReplayHarness.java"])


def validate(data):
    for row in data["rounds"]:
        expected = row["expected_close_messages"]
        assert row["handled_closes"] == expected, row
        assert row["ingress_receivedFinal"] == expected, row
        assert row["ingress_processedFinal"] == expected, row
        assert row["ingress_receivedForming"] == (row["ingress_processedForming"]
            + row["ingress_coalescedForming"] + row["ingress_staleForming"]), row
        for key in ("missing_finals", "wrong_final_values", "wrong_latest_forming", "executor_drops",
                    "bulk_unfinalized", "ingress_failures", "ingress_queuedFinal", "ingress_queuedForming"):
            assert row[key] == 0, (key, row)
        retention = row.get("retention")
        if retention:
            assert retention["orphan_versions"] == 0, row
            assert retention["orphan_final_flags"] == 0, row
            assert retention["max_versions_per_series"] <= retention["max_bars_per_series"], row
            assert retention["max_bars_per_series"] <= data["history"] + 50, row


def main():
    if not __debug__:
        raise SystemExit("Run without python -O: strict validation uses assertions")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-home", default=os.environ.get("JAVA_HOME"), required=not os.environ.get("JAVA_HOME"))
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--scenario", choices=SCENARIOS, action="append")
    parser.add_argument("--warmups", type=int, default=20)
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--soak-rounds", type=int, default=1200)
    args = parser.parse_args()
    if args.runs < 1 or args.warmups < 0 or args.soak_rounds < 1:
        parser.error("runs/soak-rounds must be positive and warmups nonnegative")
    java_home, root = Path(args.java_home).resolve(), args.work_dir.resolve()
    version = subprocess.check_output([java_home / "bin/java", "-version"], stderr=subprocess.STDOUT, text=True)
    if 'version "21.' not in version:
        raise SystemExit(f"JDK 21 required: {version}")
    if args.prepare:
        prepare(root, java_home)
    if not (root / "source/target/classes").is_dir():
        raise SystemExit("Run --prepare first")
    summary_path = root / "results/summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    for name in args.scenario or ["normal", "flood", "bulk96", "duplicate-finals"]:
        updates, flood, waiters, copies = SCENARIOS[name]
        count = args.soak_rounds if name == "soak" else args.runs
        cp = os.pathsep.join(str(root / p) for p in ["classes", "source/target/classes", "lib/*"])
        cmd = [java_home / "bin/java", "-Xms1g", "-Xmx2g", "-XX:ActiveProcessorCount=2",
               "-XX:FlightRecorderOptions=stackdepth=128", "-cp", cp, f"-Dvariant=current-{name}",
               f"-Dwarmups={args.warmups}", f"-Druns={count}", f"-Dupdates={updates}",
               f"-Dflood={str(flood).lower()}", f"-Dwaiters={waiters}", f"-DcloseCopies={copies}",
               f"-Dsoak={str(name == 'soak').lower()}", f"-Djfr={root}/jfr/{name}.jfr",
               "com.zx.quant.klineproxy.service.impl.ReplayHarness", root / "results" / f"{name}.json"]
        print(f"RUN {name}", flush=True)
        with (root / "results" / f"{name}.log").open("w") as log:
            run(cmd, stdout=log, stderr=subprocess.STDOUT, timeout=max(300, count * 2))
        data = json.loads((root / "results" / f"{name}.json").read_text())
        validate(data)
        measured = [row for row in data["rounds"] if not row["warmup"]]
        keys = ["cpu_ms", "close_done_max_ms", "bulk_done_max_ms", "ingress_processedFinal",
                "ingress_processedForming", "ingress_coalescedForming", "ingress_staleForming"]
        summary[name] = {key: {"min": min(r[key] for r in measured),
                              "median": statistics.median(r[key] for r in measured),
                              "max": max(r[key] for r in measured)} for key in keys}
        summary[name]["validated_rounds"] = len(data["rounds"])
        print(json.dumps(summary[name]), flush=True)
    (root / "results/summary.json").write_text(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
