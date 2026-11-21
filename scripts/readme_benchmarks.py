# /// script
# requires-python = ">=3.12"
# dependencies = ["matplotlib==3.11.2"]
# ///
"""Rewrite the README's benchmark paragraph, table and chart from one recorded results file.

    uv run scripts/readme_benchmarks.py benchmarks/results/readme-2026-11-16.txt

The file is what `scripts/benchmark-release` prints. A benchmark or a loop missing from it is an
error, so a run that half failed never reaches the README.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

LOOPS = ("asyncio", "uvloop", "rloop", "zuvloop")
COLORS = {"asyncio": "#9e9e9e", "uvloop": "#6c4cf0", "rloop": "#16a08f", "zuvloop": "#f07c00"}
# The name a benchmark prints, its README label, and whether the chart shows it.
ROWS = (
    ("call_soon", "`call_soon`", True),
    ("call_soon_args", "`call_soon` with arguments", True),
    ("call_soon_threadsafe", "`call_soon_threadsafe`", False),
    ("timer_schedule_cancel", "timer schedule + cancel", True),
    ("timer_rounds", "completed timer rounds", False),
    ("timer_due_batch", "prebuilt due timer batch", False),
    ("ready_with_io", "ready chain with 250 idle connections", True),
    ("stream", "bulk stream", False),
    ("echo_1kb", "echo round trips, 1 KiB", True),
    ("plaintext", "uvicorn, plaintext", True),
    ("10kb", "uvicorn, 10 KiB body", False),
    ("aiohttp server", "aiohttp server", True),
    ("aiohttp client", "aiohttp client", False),
    ("getaddrinfo", "`getaddrinfo`, numeric host", False),
)
REGION = re.compile(r"(?<=<!-- benchmarks -->\n).*(?=<!-- /benchmarks -->)", re.DOTALL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rewrite the README benchmarks from a recorded results file.")
    parser.add_argument("results", type=Path)
    parser.add_argument("--readme", type=Path, default=Path("README.md"))
    parser.add_argument("--chart", type=Path, default=Path("docs/assets/performance.png"))
    arguments = parser.parse_args()
    recorded = arguments.results.read_text()

    def field(pattern: str) -> tuple[str, ...]:
        match = re.search(pattern, recorded, re.MULTILINE)
        if match is None:
            raise SystemExit(f"{arguments.results}: no line matches {pattern!r}")
        return match.groups()

    vm_size, cpu, vcpus = field(r"^Azure (\S+) VM: (.+) with (\d+) vCPUs$")
    (system,) = field(r"^(.+), kernel ")
    uvloop, rloop = field(r"^uvloop (\S+), rloop (\S+)$")
    python, libuv = field(r"^python (\S+)\s+libuv (\S+)")

    measured: dict[str, dict[str, tuple[float, str]]] = {}
    for block in re.finditer(r"^(\S[^\n]*)\n((?:  \w+ +[\d,]+ \S+[^\n]*\n)+)", recorded, re.MULTILINE):
        measured[block[1]] = {
            loop: (float(value.replace(",", "")), unit)
            for loop, value, unit in re.findall(r"^  (\w+) +([\d,]+) (\S+)", block[2], re.MULTILINE)
        }
    for name, _, _ in ROWS:
        for loop in LOOPS:
            if loop not in measured.get(name, {}):
                raise SystemExit(f"{arguments.results}: no {loop} result for {name}")

    def throughput(name: str, loop: str) -> str:
        value, unit = measured[name][loop]
        if unit == "MiB/s":
            return f"{value / 1024:.1f} GiB/s"
        number = f"{value / 1e6:.2f}M" if value >= 1e6 else f"{value / 1e3:.1f}k"
        return f"{number} req/s" if unit == "req/s" else f"{number}/s"

    table = ["| Benchmark | " + " | ".join(LOOPS) + " |", "| --- |" + " ---: |" * len(LOOPS)]
    for name, label, _ in ROWS:
        best = max(LOOPS, key=lambda loop: measured[name][loop][0])
        cells = [f"**{throughput(name, loop)}**" if loop == best else throughput(name, loop) for loop in LOOPS]
        table.append(f"| {label} | " + " | ".join(cells) + " |")

    region = f"""\
Throughput relative to stock asyncio (higher is better), measured with the suite in
`benchmarks/` on an Azure `{vm_size}` VM: an {cpu} with {vcpus} vCPUs,
{system}, CPython {python}, and libuv {libuv}. The comparison uses uvloop {uvloop} and rloop {rloop}.
Each result is the median of seven interleaved in-process runs or five interleaved HTTP runs. The
chart shows selected benchmarks, with a separate scale for each panel. Labels show absolute
throughput. See the [recorded results and commands]({arguments.results.as_posix()}).

The VM size is isolated: no other tenant shares its hardware, and nothing else ran during the
measurements. Check the recorded run-to-run variation before treating small differences as meaningful.

{"\n".join(table)}
"""
    readme = arguments.readme.read_text()
    if REGION.search(readme) is None:
        raise SystemExit(f"{arguments.readme}: no <!-- benchmarks --> region")
    arguments.readme.write_text(REGION.sub(lambda _: region, readme))

    charted = [(name, label) for name, label, shown in ROWS if shown]
    figure, panels = plt.subplots(4, 2, figsize=(20.8, 17.6), dpi=100)
    figure.suptitle("zuvloop vs rloop vs uvloop vs asyncio", fontsize=38, y=0.975)
    figure.text(
        0.5,
        0.93,
        f"CPython {python} · {cpu.removeprefix('Intel ')} · Throughput relative to asyncio (higher is better)",
        ha="center",
        fontsize=20,
        color="#444444",
    )
    for panel, (name, label) in zip(panels.flat, charted):
        baseline = measured[name]["asyncio"][0]
        ratios = [measured[name][loop][0] / baseline for loop in LOOPS]
        bars = panel.barh(LOOPS, ratios, height=0.65, color=[COLORS[loop] for loop in LOOPS])
        panel.bar_label(bars, labels=[throughput(name, loop) for loop in LOOPS], padding=8, fontsize=16)
        panel.axvline(1, color="#444444", linestyle="--", linewidth=1.5, zorder=0)
        panel.invert_yaxis()
        panel.set_xlim(0, max(ratios) * 1.45)
        panel.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(3, integer=True))
        panel.xaxis.set_major_formatter("{x:.0f}x")
        panel.tick_params(length=0, labelsize=16)
        panel.spines[:].set_visible(False)
        title = label.replace("`", "")
        if not label.startswith("`"):
            title = title[0].upper() + title[1:]
        panel.set_title(title, loc="left", fontsize=20, weight="bold")
    notes = panels.flat[len(charted)]
    notes.axis("off")
    notes.text(
        0.02,
        0.5,
        "Each panel has its own scale.\nBar labels show absolute throughput.\n\n"
        "rloop logs aiohttp disconnect errors.\nSee the README for details and full results.",
        va="center",
        fontsize=18,
        color="#555555",
        linespacing=1.9,
    )
    figure.subplots_adjust(left=0.095, right=0.93, top=0.87, bottom=0.05, hspace=0.75, wspace=0.42)
    figure.savefig(arguments.chart)


if __name__ == "__main__":
    main()
