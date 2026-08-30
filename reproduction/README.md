# Reproduction

## Offline

The result artifacts under esults/ contain the canonical
observations used for the published findings.

The most important reconstruction artifact is:

esults/raw/reconstruction_validation_result.json

## Live

Scripts under scripts/probes/ are read-only public API experiments.

Some require:

- Python
- grpc
- protobuf
- the corresponding public Flint API protobuf definitions

Scripts under scripts/analysis/ are intended for offline analysis
where the required result artifacts are already present.

## Important

The repository does not claim that every live probe can execute from
a dependency-free clone.

That limitation is intentional and documented.

No transactions were submitted during the research.
