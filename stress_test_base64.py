#!/usr/bin/env python3
"""Stress test the base64-enc/base64-dec codecs (crate/cli) against the
system `base64` command.

For each generated input, checks:
  1. `cargo run -- --writers base64-enc` matches `base64 -w 0` on the same
     input.
  2. `cargo run -- --readers base64-enc` matches the writer-side result.
  3. Decoding that encoded output via `--readers base64-dec` and via
     `--writers base64-dec` both reproduce the original input, and agree
     with each other.

Inputs are random bytes for every length 0..20, plus a fixed set of
larger lengths chosen to straddle the codec's internal boundaries (the
3-byte/4-char base64 group, and the 64KiB scratch buffer used by
CodecReader/CodecWriter). Each length's bytes are drawn from a Random
seeded with f"{seed}:{length}", so a single failing length can be
reproduced in isolation with --seed and --only-length.

Usage:
  ./stress_test_base64.py
  ./stress_test_base64.py --seed 42
  ./stress_test_base64.py --only-length 17 -v
  ./stress_test_base64.py --hex-input 48656c6c6f
"""

import argparse
import random
import shutil
import subprocess
import sys
from pathlib import Path

DEFAULT_EXTRA_LENGTHS = [
    21, 25, 30, 33, 45, 63, 64, 65, 100, 128, 255, 256, 257, 1000, 4095,
    4096, 4097, 65535, 65536, 65537,
]


def cli_cmd(args):
    return ["cargo", "run", "-q", "-p", "cli", "--", *args]


def run(cmd, input_bytes, cwd):
    proc = subprocess.run(cmd, input=input_bytes, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} exited {proc.returncode}: {proc.stderr.decode(errors='replace')}")
    return proc.stdout


def system_base64_encode(data):
    return run(["base64", "-w", "0"], data, cwd=None).rstrip(b"\n")


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


def test_one(crate_dir, data, verbose):
    failures = []

    enc_writer = run(cli_cmd(["--writers", "base64-enc"]), data, cwd=crate_dir)
    sys_enc = system_base64_encode(data)
    check("writer-encode vs system base64", enc_writer, sys_enc, data, failures)

    enc_reader = run(cli_cmd(["--readers", "base64-enc"]), data, cwd=crate_dir)
    check("reader-encode vs writer-encode", enc_reader, enc_writer, data, failures)

    dec_reader = run(cli_cmd(["--readers", "base64-dec"]), enc_writer, cwd=crate_dir)
    check("reader-decode vs original", dec_reader, data, data, failures)

    dec_writer = run(cli_cmd(["--writers", "base64-dec"]), enc_writer, cwd=crate_dir)
    check("writer-decode vs original", dec_writer, data, data, failures)

    check("reader-decode vs writer-decode", dec_reader, dec_writer, data, failures)

    if verbose and not failures:
        print(f"  ok: {describe(data)}")

    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seed", type=int, default=0, help="seed for random input generation (default: 0)")
    parser.add_argument(
        "--extra-lengths",
        type=int,
        nargs="*",
        default=DEFAULT_EXTRA_LENGTHS,
        help="extra input lengths to test beyond 0..20 (default: boundary-straddling sizes)",
    )
    parser.add_argument("--only-length", type=int, default=None, help="test only this one input length")
    parser.add_argument("--hex-input", type=str, default=None, help="test this exact hex-encoded input instead of random data")
    parser.add_argument(
        "--crate-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "crate",
        help="workspace directory containing the cli package's Cargo.toml (default: ./crate next to this script)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="print each passing case, not just failures")
    args = parser.parse_args()

    if shutil.which("base64") is None:
        print("error: no `base64` command found on PATH", file=sys.stderr)
        return 1

    crate_dir = args.crate_dir
    print(f"building cli (cargo build -q -p cli) in {crate_dir} ...")
    build = subprocess.run(["cargo", "build", "-q", "-p", "cli"], cwd=crate_dir)
    if build.returncode != 0:
        print("error: cargo build failed", file=sys.stderr)
        return 1

    if args.hex_input is not None:
        cases = [("hex-input", bytes.fromhex(args.hex_input))]
    else:
        lengths = [args.only_length] if args.only_length is not None else list(range(0, 21)) + args.extra_lengths
        cases = []
        for length in lengths:
            rng = random.Random(f"{args.seed}:{length}")
            cases.append((f"length={length}", rng.randbytes(length)))

    total_failures = []
    for label, data in cases:
        print(f"testing {label} ...")
        failures = test_one(crate_dir, data, args.verbose)
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
