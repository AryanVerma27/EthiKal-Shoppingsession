# Shopping Sessions: native Rust pipeline

One static binary implements `rules.md` end to end:

1. **Parse** every CSV region in parallel (`pread` into reused buffers, AVX2 bitmask field splitter, exact quoting rules). Nonzero ASCII IDs up to nine bytes fit into an order-preserving `u64`. Longer event IDs live in worker-local buffers and compare by their full bytes during deduplication. Other IDs that need interning use sharded lookup tables.
2. **Links**: union-find over all link pairs; each person is named by its smallest ID. Runs on its own thread while events parse.
3. **Dedup** events per `event_id` hash bucket (min `ingest_ts` wins, before validation), resolve persons via a prefetched open-addressing table, re-bucket by person.
4. **Sessionize** per person bucket: sort by `(person, ts)`, 30-minute gaps, sliding 7-day click window for credit (latest, then smallest `click_id`) and distinct-campaign assists.
5. **Write** both tables as uncompressed Parquet (`day` is `DATE`).

## Test

```sh
python scripts/fixtures_docker.py          # 36 fixtures inside one container
sh scripts/check_all.sh                     # build + fixtures + dev-set exact compare
```
