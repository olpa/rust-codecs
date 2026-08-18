#!/usr/bin/env python3
"""Stress test the json-enc codec (crate/cli) against a small hand-written
Python oracle for JSON string-content escaping.

For each generated input, checks:
  1. `cargo run -- --writers json-enc` matches the Python oracle.
  2. `cargo run -- --readers json-enc` matches the writer-side result.

There's no `json-dec` yet, so unlike stress_test_base64.py there's no
round-trip-through-decode check (yet) -- add it once json-dec exists.

Unlike base64, uniform-random bytes don't stress this codec very hard:
only `"`, `\\`, and control bytes 0x00-0x1F trigger any special handling,
so a uniform byte has only ~13% odds of doing anything interesting. Test
cases here come in three flavors:
  - Uniform random bytes for lengths 0..20 (cheap baseline, seeded like
    stress_test_base64.py).
  - Escape-heavy random bytes: the same lengths plus the 64KiB-scratch-
    buffer-straddling boundary lengths, drawn from an alphabet biased
    toward escape-triggering bytes, to force many
    literal<->escape/pending_literal/pending_escape transitions per
    input.
  - Fixed adversarial cases: all-literal, all-2-byte-escape,
    all-6-byte-escape, alternating literal/escape, a long literal run
    landing an escape right at a chunk boundary, and invalid UTF-8
    interleaved with escapes.

Every check runs once per `--engine` value (default: both `copy` and
`stream`, the cli's two copy paths -- see `cli --help`), so a codec bug
that only one engine exercises still gets caught.

Usage:
  ./stress_test_json.py
  ./stress_test_json.py --seed 42
  ./stress_test_json.py --only escape-heavy -v
  ./stress_test_json.py --hex-input 48656c6c6f0a22
  ./stress_test_json.py --engine stream
"""

import argparse
import random
import subprocess
import sys
from pathlib import Path

BOUNDARY_LENGTHS = [
    21, 25, 30, 33, 45, 63, 64, 65, 100, 128, 255, 256, 257, 1000, 4095,
    4096, 4097, 65535, 65536, 65537,
]

SHORT_ESCAPES = {
    0x22: rb'\"',
    0x5c: rb'\\',
    0x08: rb'\b',
    0x09: rb'\t',
    0x0a: rb'\n',
    0x0c: rb'\f',
    0x0d: rb'\r',
}

# Bytes that trigger *some* form of escaping (short or \u00XX).
ESCAPE_TRIGGER_BYTES = [0x22, 0x5c] + list(range(0x00, 0x20))


def oracle(data: bytes) -> bytes:
    """Reference JSON string-content escaping, byte-oriented (no UTF-8
    validity assumed): '"' and '\\' and 0x00-0x1F get escaped, every
    other byte (including >= 0x80) passes through unchanged."""
    out = bytearray()
    for b in data:
        if b in SHORT_ESCAPES:
            out.extend(SHORT_ESCAPES[b])
        elif b < 0x20:
            out.extend(f"\\u{b:04x}".encode("ascii"))
        else:
            out.append(b)
    return bytes(out)


def cli_cmd(args, engine):
    return ["cargo", "run", "-q", "-p", "cli", "--", "--engine", engine, *args]


def run(cmd, input_bytes, cwd):
    proc = subprocess.run(cmd, input=input_bytes, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} exited {proc.returncode}: {proc.stderr.decode(errors='replace')}")
    return proc.stdout


def describe(data, max_len=64):
    hex_str = data[:max_len].hex()
    suffix = "..." if len(data) > max_len else ""
    return f"len={len(data)} hex={hex_str}{suffix}"


def check(name, actual, expected, data, failures):
    if actual != expected:
        failures.append(
            f"{name} mismatch for input ({describe(data)})\n"
            f"  expected: {describe(expected)}\n"
            f"  actual:   {describe(actual)}"
        )
        return False
    return True


def test_one(crate_dir, data, verbose, engines):
    failures = []

    for engine in engines:
        enc_writer = run(cli_cmd(["--writers", "json-enc"], engine), data, cwd=crate_dir)
        expected = oracle(data)
        check(f"[{engine}] writer-encode vs oracle", enc_writer, expected, data, failures)

        enc_reader = run(cli_cmd(["--readers", "json-enc"], engine), data, cwd=crate_dir)
        check(f"[{engine}] reader-encode vs writer-encode", enc_reader, enc_writer, data, failures)

    if verbose and not failures:
        print(f"  ok: {describe(data)}")

    return failures


def uniform_random_cases(seed):
    cases = []
    for length in range(0, 21):
        rng = random.Random(f"{seed}:uniform:{length}")
        cases.append((f"uniform length={length}", rng.randbytes(length)))
    return cases


def escape_heavy_cases(seed):
    alphabet = list(range(256)) + ESCAPE_TRIGGER_BYTES * 5
    lengths = list(range(0, 21)) + BOUNDARY_LENGTHS
    cases = []
    for length in lengths:
        rng = random.Random(f"{seed}:escape-heavy:{length}")
        data = bytes(rng.choices(alphabet, k=length))
        cases.append((f"escape-heavy length={length}", data))
    return cases


def fixed_adversarial_cases():
    cases = [
        ("all-literal", b"A" * 5000),
        ("all-quote-escape", b'"' * 5000),
        ("all-backslash-escape", b"\\" * 5000),
        ("alternating-literal-escape", (b'A"') * 2500),
        ("all-low-control-bytes", bytes(range(0, 0x20)) * 300),
        ("invalid-utf8-with-escapes", b"\xff\xfe\"\xfe\xff\\\x80\x01"),
        ("multibyte-utf8-literal", "é😀".encode() * 500),
    ]
    for n in (4095, 4096, 4097, 65535, 65536, 65537):
        cases.append((f"literal-run-{n}-then-newline", b"A" * n + b"\n"))
        cases.append((f"literal-run-{n}-then-u-escape", b"A" * n + b"\x01"))
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seed", type=int, default=0, help="seed for random input generation (default: 0)")
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="only run cases whose label contains this substring",
    )
    parser.add_argument("--hex-input", type=str, default=None, help="test this exact hex-encoded input instead of generated cases")
    parser.add_argument(
        "--engine",
        choices=["copy", "stream", "both"],
        default="both",
        help="which cli --engine value(s) to test (default: both)",
    )
    parser.add_argument(
        "--crate-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "crate",
        help="workspace directory containing the cli package's Cargo.toml (default: ./crate next to this script)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="print each passing case, not just failures")
    args = parser.parse_args()

    engines = ["copy", "stream"] if args.engine == "both" else [args.engine]

    crate_dir = args.crate_dir
    print(f"building cli (cargo build -q -p cli) in {crate_dir} ...")
    build = subprocess.run(["cargo", "build", "-q", "-p", "cli"], cwd=crate_dir)
    if build.returncode != 0:
        print("error: cargo build failed", file=sys.stderr)
        return 1

    if args.hex_input is not None:
        cases = [("hex-input", bytes.fromhex(args.hex_input))]
    else:
        cases = uniform_random_cases(args.seed) + escape_heavy_cases(args.seed) + fixed_adversarial_cases()
        if args.only:
            cases = [(label, data) for label, data in cases if args.only in label]

    total_failures = []
    for label, data in cases:
        print(f"testing {label} ...")
        failures = test_one(crate_dir, data, args.verbose, engines)
        if failures:
            print(f"  FAILED ({len(failures)} check(s)):")
            for f in failures:
                print(f"    - {f}")
        total_failures.extend(failures)

    print()
    print(f"{len(cases)} case(s) tested, {len(total_failures)} failure(s)")
    return 1 if total_failures else 0


if __name__ == "__main__":
    sys.exit(main())
