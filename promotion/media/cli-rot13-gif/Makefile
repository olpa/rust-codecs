# agg: https://github.com/asciinema/agg
# Install (musl, no glibc dependency):
#   curl -L https://github.com/asciinema/agg/releases/download/v1.9.0/agg-x86_64-unknown-linux-musl -o ~/.local/bin/agg
#   chmod +x ~/.local/bin/agg

all: rot13_screencast.gif

generated.cast: ./generate_screencast.py
	./generate_screencast.py

rot13_screencast.gif: generated.cast
	agg --theme kanagawa-dragon $< $@
