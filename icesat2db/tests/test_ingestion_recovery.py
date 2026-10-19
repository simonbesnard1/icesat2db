"""Recovery and schema regressions exercised against temporary TileDB arrays."""

import concurrent.futures
import copy
import csv
import gzip
import time
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import tiledb

from icesat2db.core.icesat2database import IceSat2Database
from icesat2db.core.icesat2granule import IceSat2Granule
from icesat2db.core.icesat2processor import IceSat2Processor
from icesat2db.utils.segment_id import pack_photon_id

CMR = {"ATL08": "test-atl08", "ATL03": "test-atl03"}


def make_config(tmp_path):
    return {
        "earth_data_info": {"CMR_PRODUCT_IDS": dict(CMR)},
        "tiledb": {
            "local_path": str(tmp_path / "db"),
            "config_overrides": {
                "sm.compute_concurrency_level": "2",
                "sm.io_concurrency_level": "2",
            },
            "spatial_range": {
                "lat_min": -90.0,
                "lat_max": 90.0,
                "lon_min": -180.0,
                "lon_max": 180.0,
            },
            "time_range": {"start_time": "2018-01-01", "end_time": "2030-01-01"},
            "dimensions": ["latitude", "longitude", "time"],
        },
        "level_atl08": {
            "variables": {
                "segment_id": {"dtype": "int64"},
                "h_canopy": {"dtype": "float32"},
            }
        },
        "level_atl03": {
            "confidence_threshold": 3,
            "variables": {
                "segment_id": {"dtype": "int64"},
                "h_ph": {"dtype": "float32"},
            },
        },
    }


def test_s3_ingest_writes_skip_multipart(tmp_path, monkeypatch):
    monkeypatch.setattr(
        IceSat2Database, "_get_tiledb_spatial_domain", lambda self: None
    )
    config = make_config(tmp_path)
    config["tiledb"].update(
        storage_type="s3", s3_bucket="bucket", url="https://s3.example"
    )
    creds = {"AccessKeyId": "a", "SecretAccessKey": "b"}
    db = IceSat2Database(config, product="atl08", credentials=creds)
    assert db.write_ctx.config()["vfs.s3.use_multipart_upload"] == "false"
    # Consolidation and other operations keep multipart uploads for large files.
    assert db.ctx.config()["vfs.s3.use_multipart_upload"] == "true"
    config["tiledb"]["s3_settings"] = {"ingest_multipart_upload": True}
    db = IceSat2Database(config, product="atl08", credentials=creds)
    assert db.write_ctx.config()["vfs.s3.use_multipart_upload"] == "true"


@pytest.fixture
def config(tmp_path):
    return make_config(tmp_path)


@pytest.fixture
def database(config):
    db = IceSat2Database(config, product="atl08")
    db._create_arrays()
    return db


@pytest.fixture
def photon_db(config):
    db = IceSat2Database(config, product="atl03")
    db._create_arrays()
    return db


def segments():
    return pd.DataFrame(
        {
            "segment_id": np.array([1001, 1002], dtype="int64"),
            "latitude": [0.0, 0.0],
            "longitude": [0.0, 0.0],
            "time": pd.to_datetime(["2020-01-01", "2020-01-01"]).as_unit("us"),
            "h_canopy": [1.0, 2.0],
        }
    )


def photons():
    ids = pack_photon_id(10, 3, 0, np.array([5, 5, 5]), np.array([0, 1, 2]))
    return pd.DataFrame(
        {
            "photon_id": ids,
            "segment_id": np.array([5, 5, 5], dtype="int64"),
            # Photons of one pulse share time and nearly share position.
            "latitude": [0.0, 0.0, 0.0],
            "longitude": [0.0, 0.0, 0.0],
            "time": pd.to_datetime(["2020-01-01"] * 3).as_unit("us"),
            "h_ph": [1.0, 2.0, 3.0],
        }
    )


def read(db):
    with tiledb.open(db.array_uri, "r", ctx=db.ctx) as array:
        return array[:]


# ── record identity ──────────────────────────────────────────────────────────


def test_distinct_colocated_records_survive_replay(database):
    frame = segments()
    database.write_granule(frame)
    database.write_granule(frame)
    data = read(database)
    assert sorted(data["segment_id"]) == [1001, 1002]
    assert (data["timestamp_ns"] == pd.Timestamp("2020-01-01").value).all()
    assert (data["time"] == 18262).all()


def test_colocated_photons_are_kept_and_deduplicated_by_photon_id(photon_db):
    frame = photons()
    photon_db.write_granule(pd.concat([frame, frame.iloc[:1]]))
    photon_db.write_granule(frame)
    data = read(photon_db)
    assert sorted(data["photon_id"]) == sorted(frame.photon_id)


def test_photon_id_is_added_to_atl03_schema(photon_db):
    assert "photon_id" in photon_db.variables_config
    assert photon_db.record_id == "photon_id"
    assert "photon_id" in photon_db._get_schema_cache()["attrs"]


def test_record_id_is_required(database):
    with pytest.raises(ValueError, match="segment_id"):
        database.write_granule(segments().drop(columns="segment_id"))
    frame = segments()
    frame["segment_id"] = frame["segment_id"].astype(float)
    with pytest.raises(ValueError, match="integer dtype"):
        database.write_granule(frame)


def test_atl08_config_without_segment_id_is_rejected(config):
    del config["level_atl08"]["variables"]["segment_id"]
    with pytest.raises(ValueError, match="segment_id"):
        IceSat2Database(config, product="atl08")


def test_missing_attribute_is_an_error_not_a_silent_skip(database):
    with pytest.raises(ValueError, match="attribute missing.*h_canopy"):
        database.write_granule(segments().drop(columns="h_canopy"))


def test_time_outside_domain_is_rejected(database):
    frame = segments()
    frame["time"] = pd.to_datetime(["2031-01-01", "2031-01-01"])
    with pytest.raises(ValueError, match="time domain"):
        database.write_granule(frame)


def test_pack_photon_id_is_unique_and_range_checked():
    segs = np.repeat(np.arange(1, 4), 3)
    offsets = np.tile(np.arange(3), 3)
    ids = pack_photon_id(1387, 255, 5, segs, offsets)
    assert ids.dtype == np.int64 and (ids > 0).all()
    assert len(np.unique(ids)) == len(ids)
    # Beams, cycles and tracks never collide.
    others = [pack_photon_id(1387, 255, 4, segs, offsets)]
    others.append(pack_photon_id(1387, 254, 5, segs, offsets))
    others.append(pack_photon_id(1386, 255, 5, segs, offsets))
    assert len(np.unique(np.concatenate([ids, *others]))) == 4 * len(ids)
    with pytest.raises(ValueError, match="offset"):
        pack_photon_id(1, 1, 0, np.array([1]), np.array([1 << 19]))
    with pytest.raises(ValueError, match="rgt"):
        pack_photon_id(1 << 11, 1, 0, np.array([1]), np.array([0]))


# ── retries and interrupted flushes ──────────────────────────────────────────


def test_ambiguous_commit_retry_does_not_duplicate(database, monkeypatch):
    original = tiledb.open
    attempts = []

    class CommitThenFail:
        def __enter__(self):
            self.array = original(database.array_uri, "w", ctx=database.ctx)
            return self.array

        def __exit__(self, *args):
            self.array.close()
            raise ConnectionError("acknowledgment lost after commit")

    def open_array(uri, mode="r", **kwargs):
        if mode == "w" and not attempts:
            attempts.append(True)
            return CommitThenFail()
        return original(uri, mode, **kwargs)

    # Warm schema cache before injecting the write failure.
    database._get_schema_cache()
    monkeypatch.setattr(tiledb, "open", open_array)
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    database.write_granule(segments(), check_existing=False)
    assert len(read(database)["segment_id"]) == 2


def processor(writers, tmp_path, engine, data_info=None):
    proc = IceSat2Processor.__new__(IceSat2Processor)
    proc.data_info = data_info or {"tiledb": {"max_in_flight": 2}}
    proc.progress_dir = str(tmp_path / "progress")
    proc.download_path = str(tmp_path / "download")
    proc.flush_every, proc.report_every = 50, 1
    proc.database_writers = writers
    proc.parallel_engine = engine
    proc._write_products = {}
    return proc


def entry(product):
    return (f"https://host/{product}.h5", product, "2020-01-01T00:00:00", 1.0)


def test_partial_spatial_flush_is_recoverable(database, tmp_path, monkeypatch):
    frame = segments()
    frame.loc[1, "latitude"] = 45.0  # different 30° write block
    monkeypatch.setattr(
        IceSat2Processor,
        "process_single_granule",
        staticmethod(lambda gid, *args: (gid, {"atl08": frame}, {"n_records": 2})),
    )
    original = database.write_granule
    count = 0
    checks = []

    def fail_second(tile, check_existing=True):
        nonlocal count
        count += 1
        checks.append(check_existing)
        if count == 2:
            raise RuntimeError("second tile failed")
        original(tile, check_existing=check_existing)

    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        proc = processor({"atl08": database}, tmp_path, executor)
        monkeypatch.setattr(database, "write_granule", fail_second)
        with pytest.raises(RuntimeError, match="second tile failed"):
            proc._process_granules({"G1": [entry("atl08")]})
        assert database.check_granules_status(["G1"]) == {"G1": False}
        assert len(read(database)["segment_id"]) == 1
        rows = list(csv.DictReader((tmp_path / "progress/all/ledger.csv").open()))
        assert [row["status"] for row in rows] == ["fail"]
        # A fresh granule skips the existing-record read.
        assert checks == [False, False]
        assert database.interrupted_flush_granules() == {"G1"}
        proc = processor({"atl08": database}, tmp_path, executor)
        proc._process_granules({"G1": [entry("atl08")]})
        # The replay of the interrupted write checks, so the first tile is not duplicated.
        assert checks[2:] == [True, True]
    assert len(read(database)["segment_id"]) == 2
    assert database.check_granules_status(["G1"]) == {"G1": True}
    assert database.interrupted_flush_granules() == set()


def test_empty_success_and_parse_failure_have_distinct_status(
    database, tmp_path, monkeypatch
):
    def parse(gid, *args):
        if gid == "bad":
            raise ValueError("missing SDS")
        return gid, {}, {"n_records": 0}

    monkeypatch.setattr(IceSat2Processor, "process_single_granule", staticmethod(parse))
    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        proc = processor({"atl08": database}, tmp_path, executor)
        with pytest.raises(RuntimeError, match="1 granule"):
            proc._process_granules({"empty": [entry("atl08")], "bad": [entry("atl08")]})
    assert database.check_granules_status(["empty", "bad"]) == {
        "empty": True,
        "bad": False,
    }


def test_buffer_byte_limit_triggers_flush(database, tmp_path, monkeypatch):
    flushes = []
    original = database.mark_granules_as_processed_batch

    def mark(keys):
        flushes.append(list(keys))
        original(keys)

    monkeypatch.setattr(database, "mark_granules_as_processed_batch", mark)
    monkeypatch.setattr(
        IceSat2Processor,
        "process_single_granule",
        staticmethod(lambda gid, *args: (gid, {"atl08": segments()}, {})),
    )
    data_info = {"tiledb": {"max_in_flight": 1, "max_buffer_bytes": 1}}
    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        proc = processor({"atl08": database}, tmp_path, executor, data_info)
        proc._process_granules({"G1": [entry("atl08")], "G2": [entry("atl08")]})
    assert flushes == [["G1"], ["G2"]]


def test_status_read_failure_is_not_treated_as_unprocessed(database):
    with patch(
        "icesat2db.core.icesat2database.tiledb.open",
        side_effect=tiledb.TileDBError("read failed"),
    ):
        with pytest.raises(tiledb.TileDBError, match="read failed"):
            database.check_granules_status(["G1"])


# ── ATL03/ATL08 linked resume ────────────────────────────────────────────────


def test_linked_resume_downloads_companion_but_writes_only_atl03(
    config, database, photon_db, tmp_path, monkeypatch
):
    """ATL08 written, ATL03 write failed: the retry must still download ATL08 to
    link the photons, but must not write ATL08 a second time."""
    config["level_atl03"]["link_atl08"] = True
    writers = {"atl08": database, "atl03": photon_db}
    database.mark_granules_as_processed_batch(["G1"])
    proc = processor(writers, tmp_path, None, config)
    pending = proc._filter_unprocessed_granules(
        {"G1": [entry("atl08"), entry("atl03")]}
    )
    assert {e[1] for e in pending["G1"]} == {"atl08", "atl03"}
    assert proc._write_products == {"G1": {"atl03"}}

    calls = []

    def parse(gid, info, data_info, download_path, write_products):
        calls.append(write_products)
        # A worker returning both frames must not cause an ATL08 rewrite.
        return gid, {"atl03": photons(), "atl08": segments()}, {}

    monkeypatch.setattr(IceSat2Processor, "process_single_granule", staticmethod(parse))
    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        proc.parallel_engine = executor
        proc._process_granules(pending)
    assert calls == [["atl03"]]
    assert len(read(database)["segment_id"]) == 0
    assert len(read(photon_db)["photon_id"]) == 3
    assert photon_db.check_granules_status(["G1"]) == {"G1": True}


def test_linked_atl03_without_companion_is_skipped(config, tmp_path):
    config["level_atl03"]["link_atl08"] = True
    writers = {"atl08": MagicMock(), "atl03": MagicMock()}
    for writer in writers.values():
        writer.check_granules_status.return_value = {}
    proc = processor(writers, tmp_path, None, config)
    pending = proc._filter_unprocessed_granules(
        {"G1": [entry("atl03")], "G2": [entry("atl08"), entry("atl03")]}
    )
    assert "G1" not in pending
    assert proc._write_products["G2"] == {"atl08", "atl03"}


def test_process_single_granule_returns_only_write_products(tmp_path, monkeypatch):
    path = tmp_path / "x.h5"
    path.write_text("x")
    monkeypatch.setattr(
        "icesat2db.core.icesat2processor.H5FileDownloader.download",
        lambda self, gid, url, product: (gid, (product.value, str(path))),
    )
    monkeypatch.setattr(
        IceSat2Granule,
        "process_granule",
        lambda self, row: ("G1", {"atl03": photons(), "atl08": segments()}),
    )
    gid, frames, metrics = IceSat2Processor.process_single_granule(
        "G1", [entry("atl08"), entry("atl03")], {}, str(tmp_path), ["atl03"]
    )
    assert set(frames) == {"atl03"}
    assert metrics["products"] == ["atl03"]
    assert metrics["n_records"] == 3
    assert metrics["bytes_downloaded"] == 2


# ── provenance ───────────────────────────────────────────────────────────────


def test_resume_rejects_policy_and_schema_changes(config, photon_db):
    IceSat2Database(config, product="atl03")._create_arrays()
    for change in ("collection", "dtype", "domain", "confidence", "link"):
        changed = copy.deepcopy(config)
        if change == "collection":
            changed["earth_data_info"]["CMR_PRODUCT_IDS"]["ATL03"] = "other-version"
        elif change == "dtype":
            changed["level_atl03"]["variables"]["h_ph"]["dtype"] = "float64"
        elif change == "domain":
            changed["tiledb"]["spatial_range"]["lat_min"] = -80.0
        elif change == "confidence":
            changed["level_atl03"]["confidence_threshold"] = 4
        else:
            changed["level_atl03"]["link_atl08"] = True
        with pytest.raises(ValueError):
            IceSat2Database(changed, product="atl03")._create_arrays()


def test_legacy_array_without_manifest_is_rejected(config, database):
    with tiledb.open(database.array_uri, "w", ctx=database.ctx) as array:
        del array.meta["icesat2db.ingestion_manifest"]
    with pytest.raises(ValueError, match="provenance"):
        IceSat2Database(config, product="atl08")._create_arrays()


def test_source_change_rejected_before_checkpoint(database):
    database.register_granule_sources(
        {"G1": [("https://host/v7.h5?token=secret", "atl08", None)]}
    )
    database.register_granule_sources(
        {"G1": [("https://host/v7.h5?token=new", "atl08", None)]}
    )
    with tiledb.VFS(ctx=database.ctx).open(database._sources_uri, "rb") as f:
        assert b"secret" not in gzip.decompress(f.read())
    with pytest.raises(ValueError, match="Source files changed"):
        database.register_granule_sources(
            {"G1": [("https://host/v8.h5", "atl08", None)]}
        )


def test_sources_are_pinned_per_product(database, photon_db):
    granules = {"G1": [entry("atl08"), entry("atl03")]}
    database.register_granule_sources(granules)
    photon_db.register_granule_sources(granules)
    # Another product's source never conflicts with this writer's registry.
    database.register_granule_sources({"G1": [entry("atl08")]})
    with pytest.raises(ValueError, match="atl03"):
        photon_db.register_granule_sources(
            {"G1": [("https://host/new.h5", "atl03", None)]}
        )


def test_source_registry_stays_out_of_array_metadata(database):
    granules = {f"G{i}": [(f"https://host/{i}.h5", "atl08", None)] for i in range(3000)}
    database.register_granule_sources(granules)
    with tiledb.open(database.array_uri, ctx=database.ctx) as array:
        assert not any(k.endswith("_sources") for k in array.meta.keys())
    database.register_granule_sources(granules)
    with pytest.raises(ValueError, match="Source files changed"):
        database.register_granule_sources(
            {"G7": [("https://host/x.h5", "atl08", None)]}
        )


# ── chunking and parsing ─────────────────────────────────────────────────────


def test_spatial_chunking_ignores_schema_tile_size(database):
    database.config["tiledb"]["latitude_tile"] = 0.25
    database.config["tiledb"]["longitude_tile"] = 0.25
    lats = np.linspace(-50, 50, 1000)
    frame = pd.DataFrame({"latitude": lats, "longitude": lats * 1.5})
    blocks = list(database.spatial_chunking(frame))
    # 30° blocks: a 100°-long track splits into a handful of writes, not ~100.
    assert 3 <= len(blocks) <= 10
    assert sum(len(part) for _, part in blocks) == len(frame)


def test_spatial_chunking_uses_positions(database):
    frame = segments().set_axis([10, 20])
    result = pd.concat([part for _, part in database.spatial_chunking(frame)])
    pd.testing.assert_frame_equal(result, frame)


def test_parse_failure_preserves_downloads(tmp_path, monkeypatch):
    path = tmp_path / "G1"
    path.mkdir()
    (path / "ATL08.h5").write_text("placeholder")
    granule = IceSat2Granule(str(tmp_path), {"level_atl08": {"variables": {}}})
    monkeypatch.setattr(
        "icesat2db.core.icesat2granule.granule_parser.parse_h5_file",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(ValueError, match="Failed to parse"):
        granule.process_granule([("G1", ("atl08", str(path / "ATL08.h5")))])
    assert path.exists()


def test_linked_atl03_without_companion_file_raises(tmp_path, monkeypatch):
    """The original bug: this used to return an empty success and checkpoint."""
    granule = IceSat2Granule(
        str(tmp_path), {"level_atl03": {"link_atl08": True, "variables": {}}}
    )
    monkeypatch.setattr(
        "icesat2db.granule.granule_parser.Path.exists", lambda self: True
    )
    with pytest.raises(ValueError, match="matching ATL08"):
        granule.process_granule(
            [("G1", ("atl03", "/x/ATL03_20200101000000_01230405_007_01.h5"))]
        )


# ── dask path ────────────────────────────────────────────────────────────────


def test_dask_path_releases_futures_and_checkpoints_empty_results(
    database, tmp_path, monkeypatch
):
    import icesat2db.core.icesat2processor as module

    class DaskFuture(concurrent.futures.Future):
        def release(self):
            self.released = True

    class Client:
        def __init__(self):
            self.futures = []
            self.max_unreleased = 0

        def submit(self, fn, *args, pure):
            assert pure is False
            future = DaskFuture()
            future.released = False
            future.set_result(fn(*args))
            self.futures.append(future)
            self.max_unreleased = max(
                self.max_unreleased, sum(not f.released for f in self.futures)
            )
            return future

    monkeypatch.setattr(module, "Client", Client)
    monkeypatch.setattr(module, "dask_wait", concurrent.futures.wait)
    monkeypatch.setattr(
        IceSat2Processor,
        "process_single_granule",
        staticmethod(lambda gid, *args: (gid, {}, {"n_records": 0})),
    )
    client = Client()
    proc = processor({"atl08": database}, tmp_path, client)
    ids = {f"G{i}": [entry("atl08")] for i in range(5)}
    proc._process_granules(ids)
    assert client.max_unreleased <= 2
    assert all(f.released for f in client.futures)
    assert all(database.check_granules_status(list(ids)).values())
