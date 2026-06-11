import os

__version__: str

def compute_cid(data: bytes) -> str:
    """Compute the kubo-default CIDv0 of `data` in one shot."""

def hash_folder(
    path: str | os.PathLike[str],
    *,
    cid_version: int = 1,
    follow_symlinks: bool = True,
) -> str:
    """Compute the UnixFS root CID of a folder, matching kubo."""

def write_folder_car(
    path: str | os.PathLike[str],
    car_path: str | os.PathLike[str],
    *,
    cid_version: int = 1,
    follow_symlinks: bool = True,
) -> str:
    """Pack a folder into a CARv1 file and return the root CID."""

def read_carv1_root(path: str | os.PathLike[str]) -> str:
    """Read the single root CID from a CARv1 file header."""

class CidHasher:
    @staticmethod
    def for_ipfs() -> CidHasher:
        """Hasher producing the kubo-default CIDv0 (dag-pb wrapped leaves)."""

    @staticmethod
    def for_ipfs_v1_raw_leaves() -> CidHasher:
        """Hasher producing the kubo --cid-version=1 default CIDv1 (raw leaves)."""

    @staticmethod
    def for_expected(cid: str) -> CidHasher:
        """Hasher matching the flavor of an existing CID string."""

    def update(self, data: bytes) -> None:
        """Absorb a chunk of data. Releases the GIL while hashing."""

    def finalize(self) -> str:
        """Consume the hasher and return the CID string."""
