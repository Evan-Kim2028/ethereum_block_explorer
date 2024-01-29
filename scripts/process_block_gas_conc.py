from concurrent.futures import ThreadPoolExecutor
import os
import polars as pl
import re
from ethereum_block_explorer.cryo_query import cryoTransform
import jupyter_black

# Set formatting configurations
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")
jupyter_black.load()

# Initialize cryoTransform object for processing rollup data
ct = cryoTransform()

# Define directory path for raw block data and the new directory for processed data
directory_b: str = "data/raw/blocks"
new_directory = "data/blocks_pricing"

# Fetch the list of block files
synced_files: list[str] = ct.read_filenames(directory_b)


def process_blocks(blocks_file):
    """
    Processes block files and saves processed data to a new file in the blocks_pricing directory.

    Args:
        blocks_file (str): Path to the blocks file.
    """
    # Load block data as a LazyFrame for efficient processing
    blocks_lf = pl.scan_parquet(blocks_file)

    # Create the new directory if it doesn't exist
    if not os.path.exists(new_directory):
        os.makedirs(new_directory)

    # Skip file processing if it already exists in the new directory
    block_range_match = re.search(r"__(\d+_to_\d+)", blocks_file)
    if block_range_match:
        block_range = block_range_match.group(1)
        new_filename = f"{block_range}.parquet"

        full_path = os.path.join(new_directory, new_filename)
        if not os.path.exists(full_path):
            blocks_lf.collect(streaming=True).write_parquet(full_path)
            print(f"File written: {full_path}")


# Use ThreadPoolExecutor for concurrent processing of block files
with ThreadPoolExecutor() as executor:
    for blocks_file in synced_files:
        # Process each block file concurrently
        full_blocks_file = os.path.join(directory_b, blocks_file)
        executor.submit(process_blocks, full_blocks_file)
