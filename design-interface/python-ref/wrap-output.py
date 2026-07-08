import sys
import codecs
import rot13_codec  # noqa: F401  (registers the "my-rot13" codec)

with open("input-hello.txt", "rb") as f:
    plain = f.read()

writer = codecs.getwriter("my-rot13")(sys.stdout.buffer)
writer.write(plain)
