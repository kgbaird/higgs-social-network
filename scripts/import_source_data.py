"""
### CODE OWNERS: Kyle Baird

### OBJECTIVE:
  Import raw source data into a convenient format for working with in spark

### DEVELOPER NOTES:
  *none*
"""
import logging
import typing
import os
import gzip
import shutil
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

import prm.spark.io_txt
from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)
_PATH_DIR_SOURCE = Path(os.environ["USERPROFILE"]) / "scrap"
_PATH_SCHEMAS = _PATH_DIR_SOURCE / "schemas"

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS go above here
# =============================================================================


def _get_source_filepaths(
        dir_source: typing.Optional[Path]=_PATH_DIR_SOURCE,
    ) -> "typing.Iterator[Path]":
    """Lazily search through the source file paths"""
    for path in dir_source.glob("*.gz"):
        yield path


def _unzip_source_file(source: Path) -> Path:
    """Unzip a source file and return it's path"""
    target = source.parent / source.stem.replace("-", "_")  # DataFrameWriter no like `-` in paths
    with gzip.open(str(source)) as fh_in, target.open("wb") as fh_out:
        shutil.copyfileobj(fh_in, fh_out)
    return target


def _import_source_file(sparkapp: SparkApp, source: Path, schema: StructType) -> DataFrame:
    """Import the source flat file using the provided schema"""
    dataframe = sparkapp.session.read.csv(
        str(source),
        schema=schema,
        sep=" ",
        mode="FAILFAST",
        )
    return dataframe


def main() -> int:
    """A function to enclose business logic"""
    LOGGER.info("Importing source files")
    sparkapp = SparkApp("higgs-social-network", allow_local_io=True)  # Intend to run standalone
    unzipped_source_files = [
        _unzip_source_file(source_filepath)
        for source_filepath in _get_source_filepaths()
        ]
    source_dataframes = {
        unzipped_source_file.with_suffix(".parquet"): _import_source_file(
            sparkapp,
            unzipped_source_file,
            prm.spark.io_txt.build_structtype_from_csv(
                unzipped_source_file.with_suffix(".csv"),
                )
            )
        for unzipped_source_file in unzipped_source_files
        }
    for output_filepath, dataframe in source_dataframes.items():
        sparkapp.save_df(dataframe, output_filepath)

    return 0


if __name__ == "__main__":
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    with SparkApp("higgs-social-network", allow_local_io=True):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)
