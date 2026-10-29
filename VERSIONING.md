# Versioning

Development-phase policy:

- Feature/docs/refactor commits do **not** bump versions or create tags.
- Cross-repository development uses Go pseudo-versions or `replace` directives.
- **No tags are created during development.** Consumers use commit pseudo-versions.
- The first tag will be a stable `v1.0.0` after API/behavior stabilization.
- `v1.0.0` starts API stability; 0.x may contain breaking changes.
- Existing tags are kept as history.

