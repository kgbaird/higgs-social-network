"""
### CODE OWNERS: Kyle Baird

### OBJECTIVE:
  Visualization of our data explorations to show them in a more presentable format

### DEVELOPER NOTES:
  Largely quick and dirty at this point due to time constraints
  Visuals is a loose term at this point. Could likely be improve with use of bokeh
"""
import logging
import os
from pathlib import Path
from collections import OrderedDict

import numpy as np
import matplotlib.pyplot as plt
import pyspark.sql.functions as spark_funcs

from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)
_PATH_DIR_PARQUETS = Path(os.environ["USERPROFILE"]) / "scrap"
_N_INTERACTIONS_VIEW = 1
_N_RATIO_VIEW = 5

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS go above here
# =============================================================================


def main() -> int:
    """A function to enclose business logic"""
    LOGGER.info("Creating visuals for our data exploration")
    sparkapp = SparkApp("higgs-social-network", allow_local_io=True)  # Intend to run standalone

    LOGGER.info("Loading summary tables")
    df_user_interaction_ranks = sparkapp.load_df(
        _PATH_DIR_PARQUETS / "user_interactions_ranked.parquet",
        )
    df_rt_to_follower_ranks = sparkapp.load_df(
        _PATH_DIR_PARQUETS / "retweet_to_follower_ratios_ranked.parquet"
        )

    LOGGER.info("Collecting data into local python objects")
    user_interaction_ranks = df_user_interaction_ranks.filter(
        spark_funcs.col("rank") <= _N_INTERACTIONS_VIEW
        ).drop(
            "rank",
            ).collect()
    rt_to_follower_rank_rows = df_rt_to_follower_ranks.filter(
        spark_funcs.col("rank") <= _N_RATIO_VIEW
        ).orderBy(
            spark_funcs.col("rank")
            ).collect()
    rt_to_follower_ranks = OrderedDict([
        (rt_to_follower_rank_row["id_user"], rt_to_follower_rank_row["retweet_to_follower_ratio"])
        for rt_to_follower_rank_row in rt_to_follower_rank_rows
        ])

    LOGGER.info("Building interaction types ~visual")
    # There isn't a natural visual that I can see, so we'll just dump to an HTML table
    html_str = "<html>\n"
    html_str += "<title>Users with most interactions by type</title>\n"
    html_str += '<table border="4">\n'
    for field in user_interaction_ranks[0].__fields__:
        html_str += "<th>{}</th>\n".format(field)
    for user_interaction_rank in user_interaction_ranks:
        data_row_str = "".join("<td>{}</td>".format(field) for field in user_interaction_rank)
        html_str += "<tr>{}</tr>\n".format(data_row_str)
    html_str += "</table>\n"
    html_str += "</html>\n"
    with (_PATH_DIR_PARQUETS / "user_interactions_types.html").open("w") as fh_out:
        fh_out.write(html_str)

    LOGGER.info("Building retweet to follower ratio plot")
    users = tuple(rt_to_follower_ranks.keys())
    idx = np.arange(_N_RATIO_VIEW)
    ratios = tuple(rt_to_follower_ranks.values())
    plt.bar(idx, ratios, align='center', alpha=0.5)
    plt.xticks(idx, users)
    plt.xlabel("User ID")
    plt.ylabel("Ratio")
    plt.title("Top retweet to follower ratios")
    plt.savefig(str(_PATH_DIR_PARQUETS / "retweet_to_follower_ratios.png"))

    return 0


if __name__ == "__main__":
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    with SparkApp("higgs-social-network", allow_local_io=True):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)
