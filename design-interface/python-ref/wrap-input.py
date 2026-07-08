import codecs
import rot13_codec  # noqa: F401  (registers the "my-rot13" codec)

with open("encoded-hello.txt", "rb") as raw:
    reader = codecs.getreader("my-rot13")(raw)
    print(reader.read())
