# Flint Prop AMM Research

Independent read-only research into Flint's observable market-data,
liquidity, execution, and multi-maker infrastructure on Solana.

**No transactions were submitted during the research.**

## Key Result

The strongest reconstruction experiment used an observable L2
snapshot followed by sequential L2 updates to reconstruct the
observable book state.

In the v4.12 validation, three independent update-driven cases
produced exact matches against fresh reference snapshots at the
corresponding `update_id`.

This validates the reconstruction procedure for the tested
USDT/USDC cases. It does not establish universal behavior across
all markets, sessions, or possible feed gaps.

## What Was Investigated

The research focused on observable public behavior across several
areas:

- L2/L3 stream semantics
- observable liquidity and depth
- synthetic execution capacity
- external quote comparison
- L2 state reconstruction
- independent reconstruction validation
- public maker-level information

## Findings

### L2/L3 Market Data

New L2/L3 subscriptions produced an initial snapshot.

A persistent L2 subscription was also observed producing subsequent
`l2_update` events.

Observed updates carried metadata and price/size level information.

### L2 State Reconstruction

Sequential L2 updates were applied to an initial snapshot to
reconstruct the observable book.

The v4.12 validation produced:

- 3 validation cases
- 3 validated cases
- 3 exact matches

The exact-match result refers to equality of the observed price/size
levels used by the reconstruction procedure.

### Liquidity and Execution

The research measured observable book depth and synthetic fill
capacity across different order sizes.

In the sampled observations, execution capacity deteriorated as
synthetic order size increased.

These are simulations over observed public books, not results from
real submitted orders.

### External Quote Comparison

External Solana quote streams were compared with Flint observations
during a synchronized sample.

The synchronized sample contained only a limited number of usable
matches, so the results are presented as point observations rather
than evidence of systematic venue superiority.

### Maker-Level Information

Public L3 observations exposed a `maker_id` field associated with
observed book entries.

Private maker fill/markout endpoints were not used as evidence for
private fills because those endpoints required organization
credentials.

## Repository Structure

```text
research/
    methodology.md
    findings.md
    limitations.md
    evidence-matrix.md

scripts/
    probes/
    validation/

results/
    raw/
    summaries/

reproduction/
    README.md
