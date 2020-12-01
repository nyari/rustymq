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
The library is still in development. It should not be used in any production environment yet.

## Getting started
The library is **not yet published** on [crates.io](https://crates.io/) but it can be used as a crate locally with Cargo.

For examples please see the documentation.

## Contributing
There are no contribution guidelines yet, for now any contribution through pull requests are welcome.