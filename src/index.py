
import os
import tempfile
from pathlib import Path

from . import utils
from . import config


def launch_indexing_job(*, list_file: Path, geometry_file: Path, output_stream_path: Path, config: config.SwissFELConfig) -> int:

    with tempfile.TemporaryDirectory() as tempdir:
        script_path = os.path.join(tempdir, "indexing_sbatch.sh")

        idx = config.indexing
        script_content = f"""#!/bin/sh

module purge
module load crystfel/{config.crystfel_version}

indexamajig -i {list_file} \\
  --output={output_stream_path} \\
  --geometry={geometry_file} \\
  --pdb={config.cell_file_path} \\
  -j $(nproc) \\
  --peaks={idx.peak_finding_method} \\
  --threshold={idx.peak_threshold} \\
  --min-snr={idx.min_snr} \\
  --min-pix-count={idx.min_pixel_count} \\
  --min-res={idx.min_resolution} \\
  --max-res={idx.max_resolution} \\
  --indexing={idx.indexing_method} \\
  --int-radius={idx.integration_radius} \\
  --integration={idx.integration_method} \\
  --local-bg-radius={idx.local_bg_radius} \\
  --multi --retry --check-peaks
"""

        with open(script_path, "w") as f:
            f.write(script_content)

        job_id = utils.submit_job(script_path)

    return job_id
