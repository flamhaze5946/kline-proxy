#!/usr/bin/env python3
"""Isolated research replay against a pinned source commit; never starts the application."""
import argparse
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
BASE = "1025516aa9177bc6073284a37850ac85c32589d5"
SERVICE = "src/main/java/com/zx/quant/klineproxy/service/impl/AbstractKlineService.java"
SCENARIOS = {
    "baseline-warm": ("baseline", "fifo", 1, False, 0, 1),
    "cached-warm": ("optimized", "fifo", 1, False, 0, 1),
    "mailbox-warm": ("optimized", "mailbox", 1, False, 0, 1),
    "baseline-flood": ("baseline", "fifo", 20, True, 0, 1),
    "cached-flood": ("optimized", "fifo", 20, True, 0, 1),
    "mailbox-flood": ("optimized", "mailbox", 20, True, 0, 1),
    "baseline-bulk96-gated": ("baseline", "fifo", 1, False, 96, 1),
    "cached-bulk96-gated": ("optimized", "fifo", 1, False, 96, 1),
    "poll-bulk96-gated": ("poll", "fifo", 1, False, 96, 1),
    "mailbox-duplicate-finals": ("optimized", "mailbox", 20, True, 0, 5),
}


def run(cmd, **kwargs):
    return subprocess.run(list(map(str, cmd)), check=True, **kwargs)


def prepare(root, java_home):
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise SystemExit("--prepare requires an empty work directory")
    for name in ("baseline", "optimized", "poll", "classes", "lib", "results", "jfr"):
        (root / name).mkdir()
    with (root / "baseline.tar").open("wb") as archive:
        run(["git", "archive", BASE], cwd=REPO, stdout=archive)
    run(["tar", "-xf", root / "baseline.tar", "-C", root / "baseline"])
    env = dict(os.environ, JAVA_HOME=str(java_home))
    with (root / "build.log").open("w") as log:
        run(["mvn", "-B", "-DskipTests", "compile", "dependency:build-classpath",
             "-Dmdep.includeScope=test", f"-Dmdep.outputFile={root}/classpath.txt"],
            cwd=root / "baseline", env=env, stdout=log, stderr=subprocess.STDOUT)
    shutil.copytree(root / "baseline/target/classes", root / "baseline/classes")
    for index, name in enumerate((root / "classpath.txt").read_text().strip().split(os.pathsep)):
        path = Path(name)
        shutil.copy2(path, root / "lib" / f"{index:03d}-{path.name}")
    cp = os.pathsep.join(map(str, [root / "baseline/classes", root / "lib/*"]))
    javac = java_home / "bin/javac"
    for variant, source, patch in (("optimized", "baseline", "cached-stats.patch"),
                                   ("poll", "optimized", "poll-only.patch")):
        target = root / variant / SERVICE
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / source / SERVICE, target)
        run(["patch", "-p1", "-i", HERE / patch], cwd=root / variant)
        run([javac, "-cp", cp, "-d", root / variant / "classes", target])
    run([javac, "-proc:none", "-cp", cp, "-d", root / "classes",
         HERE / "src/ReplayHarness.java", HERE / "src/CorrectnessProbe.java"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-home", default=os.environ.get("JAVA_HOME"), required=not os.environ.get("JAVA_HOME"))
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    parser.add_argument("--warmups", type=int, default=12)
    parser.add_argument("--runs", type=int, default=10)
    args = parser.parse_args()
    java_home = Path(args.java_home).resolve()
    version = subprocess.check_output([java_home / "bin/java", "-version"], stderr=subprocess.STDOUT, text=True)
    if 'version "21.' not in version:
        raise SystemExit(f"JDK 21 required: {version}")
    root = args.work_dir.resolve() if args.work_dir else Path(tempfile.mkdtemp(prefix="kline-ingress-research-"))
    if args.prepare or not args.work_dir:
        prepare(root, java_home)
    if not (root / "classes").is_dir():
        raise SystemExit("Run --prepare first")
    print(f"Evidence directory: {root}", flush=True)
    summary = {}
    for name in args.scenario or SCENARIOS:
        variant, queue, updates, flood, waiters, copies = SCENARIOS[name]
        cp = os.pathsep.join(str(root / p) for p in ["classes", variant + "/classes", "baseline/classes", "lib/*"])
        cmd = [java_home / "bin/java", "-Xms1g", "-Xmx2g", "-XX:ActiveProcessorCount=2",
               "-XX:FlightRecorderOptions=stackdepth=128", "-cp", cp,
               f"-Dvariant={name}", f"-Dqueue={queue}", f"-Dwarmups={args.warmups}", f"-Druns={args.runs}",
               f"-Dupdates={updates}", f"-Dflood={str(flood).lower()}", f"-Dwaiters={waiters}",
               f"-DcloseCopies={copies}", f"-Djfr={root}/jfr/{name}.jfr",
               "com.zx.quant.klineproxy.service.impl.ReplayHarness", root / "results" / f"{name}.json"]
        print(f"RUN {name}", flush=True)
        with (root / "results" / f"{name}.log").open("w") as log:
            run(cmd, stdout=log, stderr=subprocess.STDOUT, timeout=240)
        data = json.loads((root / "results" / f"{name}.json").read_text())
        rows = [r for r in data["rounds"] if not r["warmup"]]
        keys = ["cpu_ms", "close_done_max_ms", "handled_closes", "missing_finals", "wrong_final_values",
                "wrong_latest_forming", "executor_drops", "merged_forming", "bulk_done_max_ms"]
        summary[name] = {k: {"min": min(r[k] for r in rows), "median": statistics.median(r[k] for r in rows),
                             "max": max(r[k] for r in rows)} for k in keys}
        print(json.dumps(summary[name]), flush=True)
    cp = os.pathsep.join(str(root / p) for p in ["classes", "baseline/classes", "lib/*"])
    run([java_home / "bin/java", "-cp", cp, "com.zx.quant.klineproxy.service.impl.CorrectnessProbe",
         root / "results/correctness.json"])
    (root / "results/summary.json").write_text(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
