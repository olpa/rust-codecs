#!/usr/bin/env python3
"""
Generate an asciinema screencast showing tmux with two vertical panes.
Left pane: user types "Hello!" into the rot13 CLI, which writes to a named pipe
Right pane: cat reads from the named pipe, showing letters one by one
"""

import json
import time
import subprocess
import os
import sys
import random
import fcntl
import select
import tempfile
from pathlib import Path

CLI_PATH = "/home/olpa/rust-codecs/crate/target/release/cli"
CHARS_PER_SECOND = 20


class ScreencastGenerator:
    def __init__(self, width=80, height=24):
        self.width = width
        self.height = height
        self.events = []
        self.start_time = time.time()

    def add_output(self, text, delay=None):
        if delay is None:
            delay = 1.0 / CHARS_PER_SECOND
        if delay > 0:
            time.sleep(delay)
        elapsed = time.time() - self.start_time
        self.events.append([elapsed, "o", text])

    def type_text(self, text, min_delay=0.08, max_delay=0.18):
        for char in text:
            delay = random.uniform(min_delay, max_delay)
            self.add_output(char, delay)

    def wait(self, duration):
        time.sleep(duration)

    def save(self, filename):
        header = {
            "version": 2,
            "width": self.width,
            "height": self.height,
            "timestamp": int(time.time()),
            "env": {
                "SHELL": "/bin/bash",
                "TERM": "xterm-256color"
            }
        }
        with open(filename, 'w') as f:
            f.write(json.dumps(header) + '\n')
            for event in self.events:
                f.write(json.dumps(event) + '\n')
        print(f"Screencast saved to: {filename}")


SPLIT_COL = 30   # vertical separator column
WIDTH = 60
HEIGHT = 12


def draw_initial_layout(gen):
    """Draw tmux-like layout with vertical split at column 30."""
    gen.add_output("\x1b[?1049h", 0)
    gen.add_output("\x1b[H\x1b[2J", 0)
    gen.add_output("\x1b[?25h", 0)

    for i in range(1, HEIGHT + 1):
        gen.add_output(f"\x1b[{i};1H\x1b[K", 0)

    # Draw vertical separator
    for i in range(1, HEIGHT):
        gen.add_output(f"\x1b[{i};{SPLIT_COL}H│", 0)

    # Status bar
    gen.add_output(f"\x1b[{HEIGHT};1H\x1b[30m\x1b[42m", 0)
    status = "[0] 0:codecs-cli* 1:cat            \"rot13-demo\" Nov-15"
    gen.add_output(status.ljust(WIDTH - 1), 0)
    gen.add_output("\x1b[m", 0)


def write_at(gen, row, col, text, max_col, delay=0):
    """Write text at position, respecting max_col boundary."""
    for ch in text:
        if col >= max_col:
            break
        gen.add_output(f"\x1b[{row};{col}H{ch}", delay)
        col += 1
    return col


def run_screencast():
    script_dir = Path(__file__).parent.absolute()
    pipe_path = script_dir / "example.pipe"

    # Remove stale pipe if present
    if pipe_path.exists():
        pipe_path.unlink()
    os.mkfifo(str(pipe_path))

    try:
        gen = ScreencastGenerator(width=WIDTH, height=HEIGHT)

        draw_initial_layout(gen)
        gen.wait(0.1)

        # --- Left pane: show prompt and command being "typed" ---
        left_prompt = "$ "
        left_cmd = f"cli --writers rot13 --no-buffering > example.pipe"

        left_row = 1
        left_col = 1
        left_max = SPLIT_COL  # exclusive: columns 1..SPLIT_COL-1

        write_at(gen, left_row, left_col, left_prompt, left_max, delay=0)
        left_col += len(left_prompt)

        # Type the command (5x faster than typing Hello)
        for ch in left_cmd:
            delay = random.uniform(0.014, 0.032)
            gen.add_output(f"\x1b[{left_row};{left_col}H{ch}", delay)
            left_col += 1
            if left_col >= left_max:
                left_row += 1
                left_col = 1

        gen.wait(0.4)
        # Simulate Enter — move to next line
        left_row += 1
        left_col = 1
        gen.add_output(f"\x1b[{left_row};{left_col}H", 0)

        # --- Right pane: show cat command ---
        right_prompt = "$ "
        right_cmd = "cat example.pipe"

        right_row = 1
        right_col = SPLIT_COL + 1
        right_max = WIDTH + 1  # exclusive

        # One empty prompt before the cat command
        for _ in range(1):
            write_at(gen, right_row, SPLIT_COL + 1, "$ ", right_max, delay=0)
            right_row += 1

        right_col = SPLIT_COL + 1
        write_at(gen, right_row, right_col, right_prompt, right_max, delay=0)
        right_col += len(right_prompt)

        for ch in right_cmd:
            delay = random.uniform(0.014, 0.032)
            gen.add_output(f"\x1b[{right_row};{right_col}H{ch}", delay)
            right_col += 1

        gen.wait(0.4)
        right_row += 1
        right_col = SPLIT_COL + 1
        gen.add_output(f"\x1b[{right_row};{right_col}H", 0)

        gen.wait(0.5)

        # --- Start the actual processes ---
        print("Starting cat process (reading from pipe)...")
        cat_process = subprocess.Popen(
            ["cat", str(pipe_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )

        print("Starting rot13 CLI process (writing to pipe)...")
        cli_process = subprocess.Popen(
            [CLI_PATH, "--writers", "rot13", "--no-buffering"],
            stdin=subprocess.PIPE,
            stdout=open(str(pipe_path), 'wb', buffering=0),
            stderr=subprocess.PIPE,
            bufsize=0,
        )

        # Make cat stdout non-blocking
        flags = fcntl.fcntl(cat_process.stdout, fcntl.F_GETFL)
        fcntl.fcntl(cat_process.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        gen.wait(0.3)

        # --- Simulate user typing "Hello!\n" into left pane ---
        # Two blank Enters before Hello to add visual separation
        for _ in range(2):
            cli_process.stdin.write(b"\n")
            cli_process.stdin.flush()
            left_row += 1
            gen.add_output(f"\x1b[{left_row};1H", 0.2)

        input_text = "Hello!\n"
        left_input_row = left_row
        left_input_col = left_col

        print("Simulating user typing...")
        for ch in input_text:
            # Type delay (skip for newline)
            delay = random.uniform(0.12, 0.22) if ch != '\n' else 0.1

            if ch == '\n':
                # Show Enter press as newline in pane
                left_input_row += 1
                left_input_col = 1
                gen.add_output(f"\x1b[{left_input_row};{left_input_col}H", delay)
            else:
                gen.add_output(f"\x1b[{left_input_row};{left_input_col}H{ch}", delay)
                left_input_col += 1

            # Send the actual byte to the CLI
            cli_process.stdin.write(ch.encode('utf-8'))
            cli_process.stdin.flush()

            # After each char sent, check if cat has output
            time.sleep(0.05)
            try:
                ready, _, _ = select.select([cat_process.stdout], [], [], 0)
                if ready:
                    data = cat_process.stdout.read()
                    if data:
                        out_str = data.decode('utf-8', errors='replace')
                        for out_ch in out_str:
                            if out_ch == '\n':
                                right_row += 1
                                right_col = SPLIT_COL + 1
                                gen.add_output(f"\x1b[{right_row};{right_col}H", 0)
                            else:
                                if right_col < WIDTH + 1:
                                    gen.add_output(
                                        f"\x1b[{right_row};{right_col}H\x1b[1;32m{out_ch}\x1b[m", 0
                                    )
                                    right_col += 1
            except (BlockingIOError, OSError):
                pass

        # Close CLI stdin — signals EOF
        cli_process.stdin.close()

        # Collect any remaining output from cat
        print("Collecting remaining output...")
        time.sleep(0.3)
        fcntl.fcntl(cat_process.stdout, fcntl.F_SETFL, flags)  # back to blocking
        try:
            remaining = cat_process.stdout.read()
            if remaining:
                out_str = remaining.decode('utf-8', errors='replace')
                for out_ch in out_str:
                    if out_ch == '\n':
                        right_row += 1
                        right_col = SPLIT_COL + 1
                        gen.add_output(f"\x1b[{right_row};{right_col}H", 0.05)
                    else:
                        if right_col < WIDTH + 1:
                            gen.add_output(
                                f"\x1b[{right_row};{right_col}H\x1b[1;32m{out_ch}\x1b[m", 0.05
                            )
                            right_col += 1
        except OSError:
            pass

        cli_process.wait()
        cat_process.wait()

        gen.wait(2.0)

        output_file = script_dir / "generated.cast"
        gen.save(str(output_file))
        return 0

    finally:
        if pipe_path.exists():
            pipe_path.unlink()


if __name__ == "__main__":
    try:
        sys.exit(run_screencast())
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
