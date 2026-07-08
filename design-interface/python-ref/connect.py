import sys
import shutil
import codecs
import rot13_codec  # noqa: F401  (registers the "my-rot13" codec)

with open("encoded-hello.txt", "rb") as inp:
    reader = codecs.getreader("my-rot13")(inp)
    shutil.copyfileobj(reader, sys.stdout.buffer)
