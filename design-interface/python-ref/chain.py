import sys
import shutil
import codecs
import rot13_codec  # noqa: F401  (registers the "my-rot13" codec)

with open("input-hello.txt", "rb") as inp:
    reader1 = codecs.getreader("my-rot13")(inp)
    reader2 = codecs.getreader("my-rot13")(reader1)
    reader3 = codecs.getreader("my-rot13")(reader2)
    reader4 = codecs.getreader("my-rot13")(reader3)
    shutil.copyfileobj(reader4, sys.stdout.buffer)
