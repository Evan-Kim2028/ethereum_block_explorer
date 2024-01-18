from ethereum_block_explorer.cryo_query import cryoTransform

import os
import polars as pl
import re
import jupyter_black

pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")
jupyter_black.load()


ct = cryoTransform()


directory_a: str = "data/raw/transactions"
directory_b: str = "data/raw/blocks"

synced_files: dict[str] = ct.sync_filenames(
    directory_a=directory_a, directory_b=directory_b)


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

# Check if the lists have the same length
if len(synced_files[directory_a]) == len(synced_files[directory_b]):
    # Assuming both lists in synced_files have the same length
    for i in range(len(synced_files[directory_a])):
        # load LazyFrames
        txs_file: str = directory_a + "/" + synced_files[directory_a][i]
        blocks_file: str = directory_b + "/" + synced_files[directory_b][i]

        txs_lf: pl.LazyFrame = pl.scan_parquet(txs_file)
        blocks_lf: pl.LazyFrame = pl.scan_parquet(blocks_file)

        # combine and save
        tx_blocks_lf: pl.LazyFrame = (
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

        # make a new directory to save the files
        new_directory = "data/rollups"
        if not os.path.exists(new_directory):
            os.makedirs(new_directory)

        # get block chunk identifier
        block_range_match = re.search(r"__(\d+_to_\d+)", txs_file)

        if block_range_match:
            block_range = block_range_match.group(1)
            new_filename = f"{block_range}.parquet"

            full_path = os.path.join(new_directory, new_filename)

            # Check if a file with this block range already exists
            if not os.path.exists(full_path):
                # Write the Parquet file if it does not exist
                tx_blocks_lf.collect(streaming=True).write_parquet(full_path)
                print(f"File written: {full_path}")
                print(f'writing parquet file: {full_path}')


else:
    print("The synced file lists for the two directories are not of the same length.")
