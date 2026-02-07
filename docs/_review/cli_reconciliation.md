# CLI Reconciliation Note

This note reconciles `docs/cli.md` against `kucoin_lake/cli.py` and the underlying API modules. The user-captured `--help` output was referenced in the task prompt but is not stored in the repo; re-run the help commands listed in `docs/cli.md` to confirm the surface output matches.

Verified by `--help` (user capture)
- TODO: Compare the user-captured `kucoin-lake --help` and per-command `--help` outputs to sections 1-6 in `docs/cli.md`. The capture is not stored in the repo.

Verified by source
- Command names, required args, and default values for all subcommands: `kucoin_lake/cli.py:18`, `kucoin_lake/cli.py:22`, `kucoin_lake/cli.py:23`, `kucoin_lake/cli.py:24`, `kucoin_lake/cli.py:31`, `kucoin_lake/cli.py:33`, `kucoin_lake/cli.py:34`, `kucoin_lake/cli.py:52`, `kucoin_lake/cli.py:55`, `kucoin_lake/cli.py:60`, `kucoin_lake/cli.py:66`, `kucoin_lake/cli.py:68`, `kucoin_lake/cli.py:69`, `kucoin_lake/cli.py:82`, `kucoin_lake/cli.py:88`, `kucoin_lake/cli.py:94`, `kucoin_lake/cli.py:96`, `kucoin_lake/cli.py:97`, `kucoin_lake/cli.py:98`, `kucoin_lake/cli.py:102`, `kucoin_lake/cli.py:104`, `kucoin_lake/cli.py:114`, `kucoin_lake/cli.py:122`, `kucoin_lake/cli.py:124`, `kucoin_lake/cli.py:125`, `kucoin_lake/cli.py:127`, `kucoin_lake/cli.py:138`, `kucoin_lake/cli.py:149`, `kucoin_lake/cli.py:160`, `kucoin_lake/cli.py:170`, `kucoin_lake/cli.py:177`, `kucoin_lake/cli.py:178`, `kucoin_lake/cli.py:179`, `kucoin_lake/cli.py:180`, `kucoin_lake/cli.py:184`, `kucoin_lake/cli.py:185`, `kucoin_lake/cli.py:186`, `kucoin_lake/cli.py:187`, `kucoin_lake/cli.py:189`
- Default dataset list for metadata: `kucoin_lake/constants.py:51`
- Symbol filter merge and date parsing for build-metadata: `kucoin_lake/cli.py:212`, `kucoin_lake/cli.py:214`, `kucoin_lake/cli.py:215`, `kucoin_lake/cli.py:218`, `kucoin_lake/cli.py:219`
- Symbol filter merge and date parsing for backfill-derived: `kucoin_lake/cli.py:250`, `kucoin_lake/cli.py:252`, `kucoin_lake/cli.py:253`, `kucoin_lake/cli.py:256`, `kucoin_lake/cli.py:257`
- build-kline-integrity passes `start_date`/`end_date` through as strings: `kucoin_lake/cli.py:241`, `kucoin_lake/cli.py:242`, `kucoin_lake/api.py:56`, `kucoin_lake/api.py:57`
- resample dataset validation and overwrite semantics: `kucoin_lake/cli.py:104`, `kucoin_lake/cli.py:110`, `kucoin_lake/api.py:125`, `kucoin_lake/api.py:131`, `kucoin_lake/resample.py:347`, `kucoin_lake/resample.py:401`
- ingest requires bounded dates: `kucoin_lake/ingest.py:1339`, `kucoin_lake/ingest.py:1340`
- ingest default roots: `kucoin_lake/ingest.py:33`, `kucoin_lake/ingest.py:34`, `kucoin_lake/api.py:175`
- fetch datatype validation and aliasing: `kucoin_lake/fetch.py:66`, `kucoin_lake/fetch.py:68`, `kucoin_lake/fetch.py:70`, `kucoin_lake/constants.py:6`, `kucoin_lake/constants.py:22`
- fetch date window requirement (`days` vs `start_date`/`end_date`): `kucoin_lake/fetch.py:34`, `kucoin_lake/fetch.py:50`
- CLI output printing behavior: `kucoin_lake/cli.py:205`, `kucoin_lake/cli.py:208`, `kucoin_lake/cli.py:352`

TODOs
- Verify the `--help` output in a runtime environment and update `docs/cli.md` if any argparse rendering differs from the source defaults.
