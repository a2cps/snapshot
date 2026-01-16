# snapshot

-----

## Release Checklist

- [ ] Update list of participants in the [DataFreeze](src/snapshot/data/DataFreeze_3_022825.csvataFreeze_3_022825.csv) file (from <https://github.com/a2cps/DataFreeeze>), both the file and the name in the `datasets::get_recordids` function.
- [ ] Update path to demographics (e.g., point to a file on TACC) in `datasets::get_demographics`.
- [ ] Update the [Release Notes](src/snapshot/data/A2CPS_Release_2.1_Notes.docx) file---the file and the name in the `datasets::get_release_notes` function.
- [ ] Tag commit (v#.#.#).
- [ ] Do GitHub Release with tag, adding Release Notes.
- [ ] Update relevant parts of the snapshot_app in the main pipeline (<https://github.com/a2cps/mri_imaging_pipeline>).
- [ ] Submit snapshot_app job and check results.

## Additional Steps

- [ ] After releasing, the manual qc ratings should be copied into the final folder. Parquet versions of the ratings can be produced with [dump_dirt](tools/dump_dirt) and [gather_dirt.py](tools/gather_dirt.py).
