# Findings

## L2/L3

New L2/L3 subscriptions produced an initial observable snapshot.

Persistent L2 subscriptions were also observed producing subsequent
`l2_update` events.

The observed streams provided metadata and price/size level information.

## Subscription Semantics

Independent subscriptions repeatedly produced an initial snapshot of
the observable book state.

L2 and L3 initial snapshots were aligned by slot in most of the
observed cycles.

These observations describe public feed behavior and do not establish
the undocumented internal initialization mechanism of Flint.

## L2 Reconstruction

Observed sequential L2 updates were applied to an initial snapshot to
reconstruct the observable book.

The v4.12 validation produced:

- 3 validation cases
- 3 validated cases
- 3 exact matches

In each validated case, the reconstructed observable L2 book matched an
independently captured reference snapshot at the corresponding
`update_id`.

This validates the reconstruction procedure for the tested USDT/USDC
cases.

It does not establish universal reconstruction validity across all
markets, sessions, or possible feed gaps.

## Liquidity and Execution

The research measured observable book depth and synthetic execution
capacity across different order sizes.

The observed books showed substantial depth asymmetry in some snapshots.

In the sampled synthetic execution tests, observable fill capacity
decreased as requested order size increased.

These results are simulations over observed public books and are not
equivalent to fills from real submitted orders.

## External Comparison

External Solana quote streams were compared with Flint observations
during a synchronized measurement window.

The synchronized sample produced measurable differences between the
external quotes and Flint observations.

The usable synchronized sample was limited, so these observations are
presented as point comparisons rather than evidence of systematic
superiority or inferiority of Flint versus another venue.

## Maker Information

Public L3 observations exposed a `maker_id` field associated with
observed book entries.

The research does not identify the real operator behind a `maker_id`.

Private maker fill and markout endpoints required organization
credentials and were therefore not used as evidence for private
fills.

## Evidence Boundary

The findings above describe observable public API behavior during
finite measurement windows.

They do not establish undocumented internal matching, quoting, or
liquidity-management mechanisms.

No transactions were submitted during the research.
