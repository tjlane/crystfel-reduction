#!/usr/bin/env python

import gemmi
import argparse
from pathlib import Path

from cpdred.swissfel.proc import constants

parser = argparse.ArgumentParser()
parser.add_argument("mtz_file", help="modify the cell of this mtz file")
args = parser.parse_args()


mtz = gemmi.read_mtz_file(args.mtz_file)
mtz.cell = gemmi.UnitCell(*constants.CELL)

output_mtz_file_name = f"{Path(args.mtz_file).stem}-common_cell.mtz"

print(f" > Changing cell of : {args.mtz_file}")
print(f" > New Cell: {mtz.cell}")
print(f"   ... writing: {output_mtz_file_name}")

mtz.write_to_file(output_mtz_file_name)
