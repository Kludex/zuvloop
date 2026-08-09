# Rules

1. `Any` should be avoided in the Python type hints, and `object` is only marginally better — both are poor choices. Use the proper, most precise type possible.
2. A performance change lands as two PRs: first the benchmark on its own, then the change itself, so the effect is visible against the recorded baseline.
