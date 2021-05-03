# RustyMQ

![Build status](https://travis-ci.org/nyari/rustymq.svg?branch=main)

## Introduction
RustMQ is an IPC (inter-process communication) framework written using minimal dependencies for the Rust language. Intended to be easily extended and customized, but also provides its own implementations.

## Features
* Communication module (OSI 5 session layer) implementation for the following communication models:
  * Request-Reply (client-server) model
  * Publisher-Subscriber
  * Easily extendable with additional models
* Communication modules (OSI 1-4) implemented:
  * Only Network (IPv4 and IPv6) TCP and SSL is implemented at this point

## [Documentation](https://nyari.github.io/rustymq-rustdoc/)
Documentation is available in _rustdoc_ form through the link above

## Getting started
* The library can be used by adding the following line to **```Cargo.toml```** 

  * ```rustymq = { git = "https://github.com/nyari/rustymq.git", branch = "main" }```

* The library is **not yet published** on [crates.io](https://crates.io/).

## Stability
The library is a hobby project in development and only tested on Linux.
It should not be used in production environment yet.

## Contributing
There are no contribution guidelines yet, for now any contribution through pull requests are welcome.