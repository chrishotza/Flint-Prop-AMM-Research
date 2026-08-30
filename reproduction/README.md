# Reproduction

This repository contains an independent, read-only research package documenting observable behavior of Flint's public-facing market-data infrastructure.

No transactions were submitted during the research.

## Offline Reproduction

The repository includes canonical result artifacts under:

`results/`

These artifacts contain the observations and analysis outputs used to support the findings documented in:

- `research/findings.md`
- `research/methodology.md`
- `research/limitations.md`

The principal reconstruction-validation artifact is:

`results/raw/reconstruction_validation_result.json`

That result records three update-driven validation cases in which an observable L2 book was reconstructed from an initial snapshot plus sequential updates and compared against independently captured reference snapshots.

The validated result was:

- 3 validation cases
- 3 validated cases
- 3 exact matches

Supporting reconstruction evidence is available in:

- `results/raw/l2_update_events.jsonl`
- `results/raw/reconstructed_l2_book_result.json`
- `results/raw/reconstruction_validation_result.json`

The broader evidence package is available under:

- `results/raw/`
- `results/summaries/`

## Live Public API Experiments

Scripts under:

`scripts/probes/`

are live, read-only experiments against Flint's public-facing API.

The probes cover areas including:

- L2/L3 stream semantics
- subscription and resubscription behavior
- persistent L2 updates
- L2 update capture
- external quote comparison
- public maker-level L3 information

No transaction is submitted by these research probes.

## Runtime Requirements

Depending on the probe, the live experiments may require:

- Python 3
- gRPC
- protobuf
- the generated protobuf modules corresponding to the public Flint API environment

Some probes import:

- `api_pb2`
- `api_pb2_grpc`

Those generated API definitions are not included as private laboratory artifacts in this repository.

Therefore, the live probes should be understood as environment-dependent research scripts rather than dependency-free standalone programs.

## Reconstruction Validation

The reconstruction validation is based on a persistent L2 stream.

The procedure is:

```text
Initial L2 snapshot
        +
Sequential L2 updates
        ↓
Reconstructed observable book
        ↓
Independent reference snapshot
        ↓
Exact comparison
