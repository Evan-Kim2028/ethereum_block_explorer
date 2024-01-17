from ethereum_block_explorer.cryo_query import cryoQuery


cryo_query = cryoQuery()

cryo_query.query_blocks_txs(
    block_range=["16000000:18950000"]  # November 18, 2022 to January 6, 2024
)
