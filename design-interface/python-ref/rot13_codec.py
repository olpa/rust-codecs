import codecs

_FROM = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_TO = b"NOPQRSTUVWXYZABCDEFGHIJKLMnopqrstuvwxyzabcdefghijklm"
_TABLE = bytes.maketrans(_FROM, _TO)


def rot13(data: bytes) -> bytes:
    return data.translate(_TABLE)


class Codec(codecs.Codec):
    def encode(self, input, errors="strict"):
        return rot13(input), len(input)

    def decode(self, input, errors="strict"):
        return rot13(input), len(input)


class IncrementalEncoder(codecs.IncrementalEncoder):
    def encode(self, input, final=False):
        return rot13(input)


class IncrementalDecoder(codecs.IncrementalDecoder):
    def decode(self, input, final=False):
        return rot13(input)


class StreamReader(Codec, codecs.StreamReader):
    charbuffertype = bytes


class StreamWriter(Codec, codecs.StreamWriter):
    pass


def search_function(name):
    if name != "my_rot13":
        return None
    return codecs.CodecInfo(
        name="my_rot13",
        encode=Codec().encode,
        decode=Codec().decode,
        incrementalencoder=IncrementalEncoder,
        incrementaldecoder=IncrementalDecoder,
        streamreader=StreamReader,
        streamwriter=StreamWriter,
    )


codecs.register(search_function)
