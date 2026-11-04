//! Sample third-party tokio codecs, used as examples.

#[cfg(test)]
mod tests {
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::tokio::test_support::{
        three_strings_input, LengthPrefixedStringTokioDecoder, NullSink,
    };
    use crate::tokio::TokioDecoderCodec;
    use crate::{stream_to_stream, BoundaryAwareCodec};

    /// Wrap a tokio `Decoder` with `TokioDecoderCodec`, then read its
    /// objects through the codec interface: three strings, then nothing.
    ///
    /// Each object is one logical stream. The adapter writes no bytes, so
    /// the sink can ignore its input. The object comes out of `take_item`.
    #[test]
    fn read_three_strings_and_then_nothing() {
        let input = three_strings_input();
        let mut source = SliceSource::new(&input);
        let mut sink = NullSink::default();

        // A tokio decoder, wrapped by our adapter.
        let mut adapter = TokioDecoderCodec::new(LengthPrefixedStringTokioDecoder {});

        // Run one logical stream and return the decoded object. `&mut dyn`
        // lets the closure keep the adapter for `take_item`.
        let mut next_object = || {
            let codec: &mut dyn BoundaryAwareCodec = &mut adapter;
            stream_to_stream(&mut source, codec, &mut sink).unwrap();
            adapter.take_item()
        };

        assert_eq!(next_object(), Some("hello".to_string()));
        assert_eq!(next_object(), Some("tokio".to_string()));
        assert_eq!(next_object(), Some("world".to_string()));
        assert_eq!(next_object(), None);
    }
}
