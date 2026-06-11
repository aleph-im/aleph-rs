"""Golden-vector tests for the aleph_cid Python module.

Golden CIDs are copied from the Rust test suite (crates/aleph-cid/src/verify.rs
and crates/aleph-cid/tests/folder_hash.rs), which pins them against real kubo.
Folder fixtures mirror the Rust fixture builders byte for byte.
"""

import pytest

import aleph_cid

# === verify.rs goldens ===

EMPTY_FILE_V0 = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH"
DEADBEEF_128MIB_V0 = "QmcYKke22MG2rnu4nPVj8Z3hMPi2wtVMKzqLcJwYRThYif"
DEADBEEF_128MIB_V1 = "bafybeiawhayvhrtunmsazigmne75kqyyb2z7oqlvky3abpk4tbkqyzv6iu"
HELLO_NL_V1_RAW = "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"

# === tests/folder_hash.rs goldens (kubo v0.30.0) ===

GOLDEN_SINGLE_FILE_SMALL_V1 = "bafybeigdcg7pksx2zk5336vrfsktjodlr4rbfz37qr3koc5xboxe5ekv24"
GOLDEN_FLAT_DIR_SMALL_V1 = "bafybeic44rqkymydh3gvookwnqasv5ydbk5owkl7l2pkvgmh4stny4cdly"
GOLDEN_FLAT_DIR_SMALL_V0 = "QmaVbVDQrEVXH6EAQQExN82Xt44VmrQWkkey4S8eYcTNRs"
GOLDEN_NESTED_DIR_V1 = "bafybeidcclyz24mrl4furbaf4ecb3ks52dbfer7r6dxqavd4wrqg7bp7lu"
GOLDEN_HAMT_SHORT_NAMES_V1 = "bafybeidk3a4hr6msgcv24pkutwrethttydqzx56m724lsg75fzgeu3yzn4"


def fixture_flat_dir_small(root):
    for c in "abcdefghij":
        (root / f"{c}.txt").write_bytes(c.encode())


def fixture_nested_dir(root):
    (root / "top.txt").write_bytes(b"top\n")
    sub = root / "sub"
    sub.mkdir()
    (sub / "inner.txt").write_bytes(b"inner\n")
    deeper = sub / "deeper"
    deeper.mkdir()
    (deeper / "leaf.txt").write_bytes(b"leaf\n")


class TestComputeCid:
    def test_empty(self):
        assert aleph_cid.compute_cid(b"") == EMPTY_FILE_V0

    def test_multi_level_dag(self):
        assert aleph_cid.compute_cid(b"deadbeef" * (16 * 1024 * 1024)) == DEADBEEF_128MIB_V0


class TestCidHasher:
    def test_for_ipfs_matches_compute_cid(self):
        data = b"hello dag-pb world"
        h = aleph_cid.CidHasher.for_ipfs()
        h.update(data)
        assert h.finalize() == aleph_cid.compute_cid(data)

    def test_for_ipfs_chunked_updates(self):
        data = b"deadbeef" * (16 * 1024 * 1024)
        h = aleph_cid.CidHasher.for_ipfs()
        for i in range(0, len(data), 1 << 20):
            h.update(data[i : i + (1 << 20)])
        assert h.finalize() == DEADBEEF_128MIB_V0

    def test_v1_raw_leaves_short_input(self):
        h = aleph_cid.CidHasher.for_ipfs_v1_raw_leaves()
        h.update(b"hello\n")
        assert h.finalize() == HELLO_NL_V1_RAW

    def test_v1_raw_leaves_multi_level_dag(self):
        h = aleph_cid.CidHasher.for_ipfs_v1_raw_leaves()
        h.update(b"deadbeef" * (16 * 1024 * 1024))
        assert h.finalize() == DEADBEEF_128MIB_V1

    # Explicit ids keep the bytes params out of the generated test names:
    # pytest would otherwise render the 128 MiB payload into a 128 MB test id,
    # which hangs the GitHub Actions log pipeline.
    @pytest.mark.parametrize(
        "cid,data",
        [
            pytest.param(EMPTY_FILE_V0, b"", id="empty-v0"),
            pytest.param(HELLO_NL_V1_RAW, b"hello\n", id="hello-v1-raw"),
            pytest.param(
                DEADBEEF_128MIB_V1,
                b"deadbeef" * (16 * 1024 * 1024),
                id="deadbeef-128mib-v1",
            ),
        ],
    )
    def test_for_expected_roundtrip(self, cid, data):
        h = aleph_cid.CidHasher.for_expected(cid)
        h.update(data)
        assert h.finalize() == cid

    def test_for_expected_rejects_garbage(self):
        with pytest.raises(ValueError):
            aleph_cid.CidHasher.for_expected("not a cid")

    def test_finalize_twice_raises(self):
        h = aleph_cid.CidHasher.for_ipfs()
        h.finalize()
        with pytest.raises(ValueError):
            h.finalize()
        with pytest.raises(ValueError):
            h.update(b"late")


class TestHashFolder:
    def test_single_file_v1(self, tmp_path):
        (tmp_path / "hello.txt").write_bytes(b"hello\n")
        assert aleph_cid.hash_folder(tmp_path) == GOLDEN_SINGLE_FILE_SMALL_V1

    def test_flat_dir_v1(self, tmp_path):
        fixture_flat_dir_small(tmp_path)
        assert aleph_cid.hash_folder(tmp_path, cid_version=1) == GOLDEN_FLAT_DIR_SMALL_V1

    def test_flat_dir_v0(self, tmp_path):
        fixture_flat_dir_small(tmp_path)
        assert aleph_cid.hash_folder(tmp_path, cid_version=0) == GOLDEN_FLAT_DIR_SMALL_V0

    def test_nested_dir_v1(self, tmp_path):
        fixture_nested_dir(tmp_path)
        assert aleph_cid.hash_folder(tmp_path) == GOLDEN_NESTED_DIR_V1

    def test_hamt_sharded_dir_v1(self, tmp_path):
        for i in range(6000):
            (tmp_path / f"{i:08d}").write_bytes(b"x")
        assert aleph_cid.hash_folder(tmp_path) == GOLDEN_HAMT_SHORT_NAMES_V1

    def test_accepts_str_path(self, tmp_path):
        fixture_nested_dir(tmp_path)
        assert aleph_cid.hash_folder(str(tmp_path)) == GOLDEN_NESTED_DIR_V1

    def test_empty_folder_raises(self, tmp_path):
        with pytest.raises(ValueError):
            aleph_cid.hash_folder(tmp_path)

    def test_bad_cid_version_raises(self, tmp_path):
        (tmp_path / "a").write_bytes(b"a")
        with pytest.raises(ValueError):
            aleph_cid.hash_folder(tmp_path, cid_version=2)


class TestWriteFolderCar:
    def test_root_matches_hash_folder_and_header(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        fixture_nested_dir(src)
        car = tmp_path / "out.car"
        root = aleph_cid.write_folder_car(src, car)
        assert root == GOLDEN_NESTED_DIR_V1
        assert root == aleph_cid.hash_folder(src)
        assert aleph_cid.read_carv1_root(car) == root

    def test_v0_root(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        fixture_flat_dir_small(src)
        car = tmp_path / "out.car"
        root = aleph_cid.write_folder_car(src, car, cid_version=0)
        assert root == GOLDEN_FLAT_DIR_SMALL_V0
        assert aleph_cid.read_carv1_root(car) == root

    def test_read_carv1_root_rejects_garbage(self, tmp_path):
        bogus = tmp_path / "bogus.car"
        bogus.write_bytes(b"\x00\x01\x02\x03")
        with pytest.raises(ValueError):
            aleph_cid.read_carv1_root(bogus)


def test_version_attribute():
    assert isinstance(aleph_cid.__version__, str)
    assert aleph_cid.__version__
