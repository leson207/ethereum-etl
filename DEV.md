# Target
note to remove at data type at the end of use.
other way to group node by folder

parse -> extract
rename function: parse_raw_block -> extract_raw_block or process_raw_block_base or ....
pool_init, pool_enrich_token, ....

more etl term: map, reduce, aggreate, ingest, .. 
empty to none?
normlaize address
docker compose
update python
add sql_schema to table file
# Dev:
- better shortest path query
- why the address got lower?
- memgraph - persis - increase paralel/concurrent, timeout? use async here
- rename function
- do not create memgraph client if not need?

- larger scale
- real time
- create/delete table, create/delete dabase
- parse transfer with different topic kind (use the legacy function)
- pool: hold error pool or find a way to get it token_address (some error pool: 0x8eea6cc08d824b20efb3bf7c248de694cb1f75f4, 0xbcca60bb61934080951369a648fb03df4f96263c, 0x00a0be1bbc0c99898df7e6524bf16e893c1e3bb9)