from concurrent.futures import ThreadPoolExecutor
import os
import polars as pl
import re
from ethereum_block_explorer.cryo_query import cryoTransform

import jupyter_black

pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")
jupyter_black.load()


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


# Concurrently process rollup data with idempotent data check.
ct = cryoTransform()

directory_a: str = "data/raw/transactions"
directory_b: str = "data/raw/blocks"
new_directory = "data/rollups"

synced_files: dict[str] = ct.sync_filenames(
    directory_a=directory_a, directory_b=directory_b)


def process_file_pair(txs_file, blocks_file):
    """
    Processes a pair of transaction and block files.

    It joins transaction and block data, labels transactions with sequencer names,
    and saves the processed data to a new file in the rollups directory.

    Args:
        txs_file (str): Path to the transactions file.
        blocks_file (str): Path to the blocks file.
    """
    # Load transaction and block data as LazyFrames for efficient processing
    txs_lf = pl.scan_parquet(txs_file)
    blocks_lf = pl.scan_parquet(blocks_file)

    # Join transaction and block data, label them, and filter by sequencer names
    tx_blocks_lf = (
        ct.extend_txs_blocks(txs_lf, blocks_lf)
        .join(
            sequencer_labels_lf,
            left_on="from_address",
            right_on="sequencer_addresses",
            how="left",
        )
        .drop("timestamp", "block_number_right")
        .filter(pl.col("sequencer_names").is_in(sequencers_l2["sequencer_names"]))
    )

    # Create the rollups directory if it doesn't exist
    if not os.path.exists(new_directory):
        os.makedirs(new_directory)

    # Skip file processing if it already exists in the new directory
    block_range_match = re.search(r"__(\d+_to_\d+)", txs_file)
    if block_range_match:
        block_range = block_range_match.group(1)
        new_filename = f"{block_range}.parquet"

        full_path = os.path.join(new_directory, new_filename)
        if not os.path.exists(full_path):
            tx_blocks_lf.collect(streaming=True).write_parquet(full_path)
            print(f"File written: {full_path}")


# Check if the synced files from both directories are equal in number
if len(synced_files[directory_a]) == len(synced_files[directory_b]):
    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor() as executor:
        for i in range(len(synced_files[directory_a])):
            txs_file = directory_a + "/" + synced_files[directory_a][i]
            blocks_file = directory_b + "/" + synced_files[directory_b][i]

            # Process each pair of transaction and block files concurrently
            executor.submit(process_file_pair, txs_file, blocks_file)
else:
    # Output a message if the number of files in both directories does not match
    print("The synced file lists for the two directories are not of the same length.")
