# Versioning

Development-phase policy:

- Feature/docs/refactor commits do **not** bump versions or create tags.
- Cross-repository development uses Go pseudo-versions or `replace` directives.
- Tags are created only for integration baselines, as pre-releases:
  `vX.Y.Z-dev.N` (for example `v0.6.0-dev.1`), after tests and compatibility checks pass.
- Patch tags are reserved for critical fixes.
- `v1.0.0` starts API stability; 0.x may contain breaking changes.
- Existing tags are kept as history.

