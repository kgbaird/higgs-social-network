"""
### CODE OWNERS: Kyle Baird

### OBJECTIVE:
  Define the pipeline to enable batch processing

### DEVELOPER NOTES:
  When module is run as a script, batch processing should occur
"""
import logging
import os
from pathlib import Path

import luigi

from indypy.nonstandard.ext_luigi import PythonTask, IndyPyLocalTarget, mutate_config
from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)
_PATH_DIR_SCRIPTS = Path(os.environ["USERPROFILE"]) / "repos" / "higgs-social-network" / "scripts"
_PATH_DIR_OUTPUTS = Path(os.environ["USERPROFILE"]) / "scrap"
_PATH_DIR_LOGS = _PATH_DIR_OUTPUTS / "logs"

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS go above here
# =============================================================================


class HiggsTwitterParamMixin():
    """Class to define parameters shared across all pipeline tasks"""
    pipeline_signature = luigi.Parameter(significant=True)


class ImportSourceData(PythonTask, HiggsTwitterParamMixin):
    """Imports source data for processing in Spark"""
    def run(self):
        """Task execution"""
        return super().run(
            _PATH_DIR_SCRIPTS / "import_source_data.py",
            path_log=_PATH_DIR_LOGS,
            )

    def output(self):
        """Task outputs"""
        return [
            IndyPyLocalTarget(_PATH_DIR_OUTPUTS / "higgs_activity_time.txt"),
            IndyPyLocalTarget(_PATH_DIR_OUTPUTS / "higgs_social_network.edgelist"),
            IndyPyLocalTarget(_PATH_DIR_OUTPUTS / "higgs_activity_time.parquet" / "_SUCCESS"),
            IndyPyLocalTarget(_PATH_DIR_OUTPUTS / "higgs_social_network.parquet" / "_SUCCESS"),
            ]


class ExploratorySummaries(PythonTask, HiggsTwitterParamMixin):
    """Summaries to further understanding of source data"""
    def requires(self):
        return [ImportSourceData(pipeline_signature=self.pipeline_signature)]

    def run(self):
        """Task execution"""
        return super().run(
            _PATH_DIR_SCRIPTS / "explore_data" / "summaries.py",
            path_log=_PATH_DIR_LOGS,
            )

    def output(self):
        """Task outputs"""
        return [
            IndyPyLocalTarget(_PATH_DIR_OUTPUTS / "user_interactions_ranked.parquet" / "_SUCCESS"),
            IndyPyLocalTarget(
                _PATH_DIR_OUTPUTS / "retweet_to_follower_ratios_ranked.parquet" / "_SUCCESS",
                ),
            ]


class ExploratoryVisuals(PythonTask, HiggsTwitterParamMixin):
    """Visualizations to support our exploratory data analysis"""
    def requires(self):
        return [ExploratorySummaries(pipeline_signature=self.pipeline_signature)]

    def run(self):
        """Task execution"""
        return super().run(
            _PATH_DIR_SCRIPTS / "explore_data" / "visuals.py",
            path_log=_PATH_DIR_LOGS,
            )

    def output(self):
        """Task outputs"""
        return [
            IndyPyLocalTarget(_PATH_DIR_OUTPUTS / "retweet_to_follower_ratios.png"),
            IndyPyLocalTarget(_PATH_DIR_OUTPUTS / "user_interactions_types.html"),
            ]


def main() -> int:
    """A function to enclose the execution of business logic."""
    LOGGER.info("Beginning pipeline execution")
    _PATH_DIR_LOGS.mkdir(exist_ok=True)

    LOGGER.info("Mutating luigi config to suppress duplicate logging")
    mutate_config({"core": {"no_configure_logging": "true"}})

    LOGGER.info("Building pipeline")
    failed = luigi.build(
        [ExploratoryVisuals(pipeline_signature="higgs-social-network")],
        local_scheduler=True,
        )
    LOGGER.info("Pipeline finished with failed='%s'", failed)

    return int(not failed)


if __name__ == "__main__":
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    with SparkApp("higgs-social-network", allow_local_io=True):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)
