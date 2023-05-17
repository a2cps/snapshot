import shutil
import subprocess
import tempfile
from pathlib import Path

import prefect


@prefect.task
def _deface(volume: Path, out: Path, odir: Path | None = None) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdp = Path(tmpdir)
        tmp_out = tmpdp / out.name
        cmd = ["mideface", "--i", str(volume), "--o", str(tmp_out)]
        if odir:
            tmpo_dir = tmpdp / "odir" / odir.name
            cmd += ["--odir", str(tmpo_dir)]
        subprocess.run(
            cmd,  # noqa: S603
            capture_output=True,
        )
        if tmp_out.exists():
            if out.exists():
                out.unlink()
            shutil.move(tmp_out, out)
            if odir:
                shutil.move(tmpo_dir, odir)
        else:
            raise AssertionError


@prefect.flow
def deface(volume: list[Path], out: list[Path], odir: list[Path]) -> None:
    for i, o, q in zip(volume, out, odir):
        _deface.submit(i, o, q)
