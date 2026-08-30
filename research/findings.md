# Findings

## L2/L3

New subscriptions produced initial snapshots.

Persistent L2 streams also produced later update events.

## Reconstruction

Observed sequential L2 updates were applied to an initial
snapshot to reconstruct the observable book.

The v4.12 validation produced 3 exact matches against independent
snapshots at the same update_id.

## Liquidity

Observed books showed depth asymmetry and synthetic fill capacity
declined as requested size increased.

## External comparison

External quote observations differed measurably from Flint during
the synchronized sample, but the sample is limited.

## Maker information

Public L3 observations exposed maker_id fields.
