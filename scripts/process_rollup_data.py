
# | export
from ethereum_block_explorer.cryo_query import cryoTransform

import polars as pl
import jupyter_black

pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")
jupyter_black.load()


# SEQUENCER DATA IN TRANSACTIONS
cryo_transform = cryoTransform()

# Sequencer Tags
# https://dune.com/queries/3302607
sequencers_l2: dict[str] = {
    "sequencer_addresses": [
        "0xc1b634853cb333d3ad8663715b08f41a3aec47cc",
        "0x6887246668a3b87f54deb3b94ba47a6f63f32985",
        "0x9228624c3185fcbcf24c1c9db76d8bef5f5dad64",
        "0x6667961f5e9c98a76a48767522150889703ed77d",
        "0xcf2898225ed05be911d3709d9417e86e0b4cfc8f",
        "0x148ee7daf16574cd020afa34cc658f8f3fbd2800",
        "0x16d5783a96ab20c9157d7933ac236646b29589a4",
        "0x5050f69a9786f081509234f1a7f4684b5e5b76c9",
    ],
    "sequencer_names": [
        "arbitrum",
        "optimism",
        "linea",
        "mantle",
        "scroll",
        "polygon_zkevm",
        "starknet",
        "base",
    ],
}

sequencer_labels_lf: pl.LazyFrame = pl.from_dict(sequencers_l2).lazy()

# txs
txs_lf = pl.scan_parquet("data/raw/transactions/*.parquet")

# blocks
blocks_lf = pl.scan_parquet("data/raw/blocks/*.parquet").select(
    "author", "block_number", "timestamp", "gas_used", "base_fee_per_gas"
).sort(by="block_number").with_columns(
    (pl.col("base_fee_per_gas").rolling_mean(window_size=7200)).alias(
        "avg_base_fee_daily"
    ),
    (pl.col("base_fee_per_gas").rolling_mean(window_size=5)).alias(
        "avg_base_fee_minute"
    ),  # calculates rolling average over 5 blocks (1 minute)
)

# final df
tx_blocks_lf: pl.LazyFrame = (
    cryo_transform.extend_txs_blocks(txs_lf, blocks_lf)
    .join(
        sequencer_labels_lf,
        left_on="from_address",
        right_on="sequencer_addresses",
        how="left",
    )
    .drop("timestamp", "block_number_right")
    .filter(pl.col("sequencer_names").is_in(sequencers_l2["sequencer_names"]))
)

tx_blocks_lf.collect(streaming=True).write_parquet(
    "data/rollup_blobs_nov22_jan24.parquet")
# tx_blocks_lf.sink_parquet("data/rollup_blobs_nov22_jan24.parquet") # doesn't workwith .rolling_mean()
