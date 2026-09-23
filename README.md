# NRTM 🌐 Parser 🦀

`nrtm-parser` is a client library to work with the Near Real Time Mirroring protocol versions 2 and 3 event streams. It is not, in itself, a client, but you can build clients with it.

It can be plugged into a source of NRTMv2/3 updates, and will provide structured objects describing the updates, with the raw RPSL string attached.

It has support for parsing in a synchronous fashion, when for instance you already have all the data collected in a file.

It also has support for parsing directly from `tokio` asynchronously readable objects (i.e TCP Streams for example). You can enable this capability via the `async-streaming` feature.

## MSRV
Minimum supported rust version is 1.97.1

## Dependencies
This crate is built on:
- the [Pest parser](https://crates.io/crates/pest) crate
- mixed encoding detection and conversion is achieved through [chardetng](https://crates.io/crates/chardetng) and [encoding_rs](https://crates.io/crates/encoding_rs)
- [tokio](https://crates.io/crates/tokio), [tokio-util](https://crates.io/crates/tokio-util) and [futures-util](https://crates.io/crates/futures-util) for the async features

## Examples / How to hold it correctly 🔨
You can find some code examples in the integration tests:
- [single message parsing example](./tests/integration.rs)
- [stream parsing example](./tests/streaming_integration.rs)
