"""
### CODE OWNERS: Kyle Baird

### OBJECTIVE:
  Graph analysis of the data to explore infulential users

### DEVELOPER NOTES:
  Probably going to go light on features
  TODO: Figure out the timestamp format so we can use it
"""
import logging
import os
from pathlib import Path

import pyspark.sql.functions as spark_funcs

from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)
_PATH_DIR_PARQUETS = Path(os.environ["USERPROFILE"]) / "scrap"

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS go above here
# =============================================================================


def main() -> int:
    """A function to enclose business logic"""
    LOGGER.info("Performing graph analysis on data")
    sparkapp = SparkApp("higgs-social-network", allow_local_io=True)  # Intend to run standalone

    df_n_followers = sparkapp.load_df(
        _PATH_DIR_PARQUETS / "retweet_to_follower_ratios_ranked.parquet",
        ).select(
            "id_user",
            "n_followers",
            )
    df_edges_raw = sparkapp.load_df(
        _PATH_DIR_PARQUETS / "higgs_activity_time.parquet"
        )
    df_verticies_raw = df_edges_raw.select(
        spark_funcs.col("id_usera").alias("id_user"),
        ).distinct().union(
            df_edges_raw.select(
                spark_funcs.col("id_userb").alias("id_user"),
                ).distinct()
            ).distinct()
    df_verticies = df_verticies_raw.join(
        df_n_followers,
        "id_user",
        how="left_outer",
        ).select(
            spark_funcs.col("id_user").alias("id"),  # Graphframes requires `id` name
            spark_funcs.col("n_followers"),
            ).filter(
                spark_funcs.col("n_followers") > 210,  # Must have follower base to be influential
                )
    # df_verticies.view()
    # TODO: Time weighted decay of interaction timestamps
    # TODO: Score the interation types and throw out some of the noise
    # PageRank does not care about duplicate/parallel edges
    df_edges = df_edges_raw.select(
        spark_funcs.col("id_usera").alias("src"),
        spark_funcs.col("id_userb").alias("dst"),
        spark_funcs.col("interaction").alias("interaction"),
        )

    return 0


if __name__ == "__main__":
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    with SparkApp("higgs-social-network", allow_local_io=True):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)
