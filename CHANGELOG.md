# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project **only** adheres to the following _(as defined at [Semantic Versioning](https://semver.org/spec/v2.0.0.html))_:

> Given a version number MAJOR.MINOR.PATCH, increment the:
> 
> - MAJOR version when you make incompatible API changes
> - MINOR version when you add functionality in a backward compatible manner
> - PATCH version when you make backward compatible bug fixes

## [5.0.2] - 2026-08-31

This release improves handling of logical paths containing single quotes by hex-escaping them for GenQuery.

### Changed

- Use fmtlib instead of Boost.Format (#189).
- Escape single quotes in logical path arguments for GenQuery (#191).

### Fixed

- Allow removal of logical paths containing single quotes (#184).

### Added

- Add build hook option for compiling against specific version of released iRODS packages (irods/irods_development_environment#165).
