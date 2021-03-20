# RustyMQ

## Introduction
RustMQ is an IPC (inter-process communication) framework written using minimal dependencies for the Rust language. Intended to be easily extended and customized, but also provides its own implementations.

## Features
* Communication module (OSI 5 session layer) implementation for the following communication models:
  * Request-Reply (client-server) model
  * Publisher-Subscriber
  * Easily extendable with additional models
* Communication modules (OSI 1-4) implemented:
  * Only Network (IPv4 and IPv6) TCP and SSL is implemented at this point

## Stability
The library is a hobby project in development and only tested on Linux.
It should not be used in any production environment.

## Getting started
The library is **not yet published** on [crates.io](https://crates.io/) but it can be used as a crate locally with Cargo.

For examples see the cargo porojects in the [examples][examples] folder.
For documentation generate the documentation with `cargo doc --open --all-features`

## Contributing
There are no contribution guidelines yet, for now any contribution through pull requests are welcome.