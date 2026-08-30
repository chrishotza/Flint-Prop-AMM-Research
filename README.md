# Flint Prop AMM Research

Independent read-only research into Flint's observable
market-data, liquidity, execution, and multi-maker infrastructure
on Solana.

No transactions were submitted.

## Key result

The strongest experiment reconstructed an observable L2 book from
an initial snapshot and sequential updates.

Three independent update-driven validation cases matched fresh
reference snapshots at the corresponding update_id.

## Scope

This repository documents observable public API behavior.

It does not claim access to undocumented internal Flint
implementation.

Synthetic execution results are simulations over observed books,
not real trade executions.

Private maker fill/markout endpoints were not used as evidence
because they required organization credentials.

## Contents

- esearch/ — methodology, findings, limitations
- scripts/analysis/ — offline analysis
- scripts/probes/ — live public API experiments
- scripts/validation/ — reconstruction validation
- esults/ — canonical research artifacts
- eproduction/ — reproduction notes
