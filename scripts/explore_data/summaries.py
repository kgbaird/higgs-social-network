"""
### CODE OWNERS: Kyle Baird

### OBJECTIVE:
  Explore source data to further our understanding of it

### DEVELOPER NOTES:
  *none*
"""
import logging
import typing
import os
from pathlib import Path

import pyspark.sql.functions as spark_funcs
from pyspark.sql.window import Window

from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)
_PATH_DIR_PARQUETS = Path(os.environ["USERPROFILE"]) / "scrap"

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS go above here
# =============================================================================


def _get_parquet_paths(
        dir_to_search: typing.Optional[Path]=_PATH_DIR_PARQUETS,
    ) -> "typing.Iterator[Path]":
    """Scan the source directory for all parquet 'files'"""
    LOGGER.info("Scanning '%s' for parquet files", dir_to_search)
    for path in dir_to_search.glob("*.parquet"):
        if not path.is_dir():
            continue
        yield path


def main() -> int:
    """A function to enclose business logic"""
    LOGGER.info("Exploring data files")
    sparkapp = SparkApp("higgs-social-network", allow_local_io=True)

    sources = {
        parquet_path.stem: sparkapp.load_df(parquet_path)
        for parquet_path in _get_parquet_paths()
        }
    interaction_type_counts = sources["higgs_activity_time"].groupBy(
        "id_usera",
        "interaction"
        ).count()
    # summed_interaction_types.view()
    rank_window = Window().partitionBy(
        "interaction",
        ).orderBy(
            spark_funcs.desc("count"),
            "id_usera", # Tie goes to the first user id (to encourage reproducibility)
            )
    ranked_interaction_types = interaction_type_counts.withColumn(
        "rank",
        spark_funcs.rank().over(rank_window),
        )
    # ranked_interaction_types.orderBy("rank").view()
    sparkapp.save_df(
        ranked_interaction_types,
        _PATH_DIR_PARQUETS / "user_interactions_ranked.parquet",
        )

    follower_counts = sources["higgs_social_network"].groupBy(
        "id_user",
        ).count()
    # follower_counts.view()
    retweet_counts = interaction_type_counts.filter(
        spark_funcs.col("interaction") == "RT"
        ).drop(
            "interaction"
            )
    # retweet_counts.view()
    retweet_to_follower_ratios = follower_counts.withColumnRenamed(
        "count",
        "n_followers",
        ).join(
            retweet_counts.withColumnRenamed(
                "id_usera",
                "id_user",
                ).withColumnRenamed(
                    "count",
                    "n_retweets",
                    ),
            "id_user",
            how="left_outer",
            ).withColumn(
                "retweet_to_follower_ratio",
                spark_funcs.coalesce(spark_funcs.col("n_retweets"), spark_funcs.lit(0)) /\
                    spark_funcs.col("n_followers"),
                )
    # retweet_to_follower_ratios.view()
    rt_to_follower_ratio_window = Window().orderBy(
        spark_funcs.desc("retweet_to_follower_ratio"),
        spark_funcs.desc("n_followers"),  # Prefer users with more followers
        "id_user",  # To encourage reproducibility
        )
    ranked_rt_to_follower_ratios = retweet_to_follower_ratios.withColumn(
        "rank",
        spark_funcs.rank().over(rt_to_follower_ratio_window),
        )
    # ranked_rt_to_follower_ratios.orderBy("rank").view()
    sparkapp.save_df(
        ranked_rt_to_follower_ratios,
        _PATH_DIR_PARQUETS / "retweet_to_follower_ratios_ranked.parquet",
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
