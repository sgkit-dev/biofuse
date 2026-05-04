"""Aggregate runner results into JSON + Markdown."""

import dataclasses
import datetime
import json
import logging
import pathlib
import platform
import socket
import sys

from harness import tools

logger = logging.getLogger(__name__)


def write_reports(
    results_dir: pathlib.Path,
    runner_results: list[tools.RunnerResult],
    metadata: dict,
) -> None:
    """Write report.json and report.md into ``results_dir``."""
    results_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "metadata": metadata,
        "runners": [_runner_to_dict(r) for r in runner_results],
        "summary": {
            "all_passed": all(r.passed for r in runner_results),
            "num_runners": len(runner_results),
            "num_failed_runners": sum(1 for r in runner_results if not r.passed),
            "total_checks": sum(len(r.checks) for r in runner_results),
            "total_passed_checks": sum(r.num_passed for r in runner_results),
            "total_failed_checks": sum(r.num_failed for r in runner_results),
        },
    }
    json_path = results_dir / "report.json"
    json_path.write_text(json.dumps(payload, indent=2, default=str))
    md_path = results_dir / "report.md"
    md_path.write_text(_render_markdown(payload))


def _runner_to_dict(r: tools.RunnerResult) -> dict:
    d = dataclasses.asdict(r)
    d["num_passed"] = r.num_passed
    d["num_failed"] = r.num_failed
    return d


def _render_markdown(payload: dict) -> str:
    md: list[str] = []
    md.append("# biofuse fs_tests report")
    md.append("")
    meta = payload["metadata"]
    md.append(f"- Run started: {meta.get('started_at', '?')}")
    md.append(f"- Host: {meta.get('hostname', '?')} ({meta.get('platform', '?')})")
    md.append(f"- Python: {meta.get('python_version', '?')}")
    md.append(f"- biofuse commit: {meta.get('biofuse_commit', '?')}")
    if "tool_versions" in meta:
        md.append("- External tool versions:")
        for name, version in meta["tool_versions"].items():
            md.append(f"  - `{name}`: {version}")
    md.append("")

    summary = payload["summary"]
    overall = "PASS" if summary["all_passed"] else "FAIL"
    md.append(f"## Overall: **{overall}**")
    md.append("")
    md.append(
        f"{summary['total_passed_checks']} / {summary['total_checks']} checks passed "
        f"across {summary['num_runners']} runners."
    )
    md.append("")

    md.append("## Per-runner summary")
    md.append("")
    md.append("| Runner | Status | Passed | Failed | Duration | Notes |")
    md.append("|---|---|---|---|---|---|")
    for r in payload["runners"]:
        status = "SKIP" if r.get("skipped") else ("PASS" if r["passed"] else "FAIL")
        notes = r.get("skip_reason") if r.get("skipped") else r.get("summary", "")
        md.append(
            f"| {r['runner']} | {status} | {r['num_passed']} | {r['num_failed']} | "
            f"{r['duration_s']:.2f}s | {notes} |"
        )
    md.append("")

    for r in payload["runners"]:
        md.append(f"## Runner: `{r['runner']}`")
        md.append("")
        if r.get("skipped"):
            md.append(f"_skipped: {r.get('skip_reason')}_")
            md.append("")
            continue
        if not r["checks"]:
            md.append("_no individual checks recorded_")
            md.append("")
            continue
        md.append("| Check | Status | Duration | Detail |")
        md.append("|---|---|---|---|")
        for c in r["checks"]:
            status = "PASS" if c["passed"] else "FAIL"
            detail = (c.get("detail") or "").replace("|", "\\|")
            md.append(f"| {c['name']} | {status} | {c['duration_s']:.3f}s | {detail} |")
        md.append("")

    return "\n".join(md) + "\n"


def make_metadata(
    *,
    started_at: datetime.datetime,
    biofuse_commit: str,
    tool_versions: dict[str, str],
) -> dict:
    return {
        "started_at": started_at.isoformat(),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "biofuse_commit": biofuse_commit,
        "tool_versions": tool_versions,
    }
