# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2024-02-12
### Added
- Validation that messages come from the correct type of senders.
- DoubleEndedIterator implementation for group iterator.
- Add support for non-determinism to services.
- Add an example of a basic file system.

## Changed
- Use select_all in primary and backup to process as many messages as possible.
- Switch request id to use UUID v7 instead of a counter.
 
## [0.6.0] - 2024-02-11
### Added
- Support for the recovery protocol.
- A local driver of the replication group to aid in testing.

### Changed
- Separated the role-specific logic into sub-modules.
- Updated backups to discard any message that is not a prepare or commit during normal operation.

## [0.5.0] - 2024-02-07
### Added
- Added the view change protocol

### Changed
- Rewrote some of the tests to improve re-usability.

### Removed
- Dead code of the previous implementation.

## [0.4.0] - 2024-02-05
### Changed
- Rewrote the entire replica and network from scratch modeled after Erlang's mailbox system.

## [0.3.0] - 2024-01-04
### Added
- Add a client struct and separate message processing by status and role.

## [0.2.0] - 2024-01-04
### Added
- Implementation of simplified VR without recovery.

## [0.1.0] - 2024-01-04
### Added
- Basic CI setup for automatic tagging and releases.
- Mostly-empty library crate.

[unreleased]: https://github.com/misalcedo/vr/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/misalcedo/vr/releases/tag/v0.1.0