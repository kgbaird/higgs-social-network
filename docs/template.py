"""
### CODE OWNERS: *At least two names*

### OBJECTIVE:
  *What and WHY does this code exist*

### DEVELOPER NOTES:
  *What do future developers need to know*
"""
import logging

from prm.spark.app import SparkApp

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS go above here
# =============================================================================


def main() -> int:
    """A function to enclose business logic"""
    LOGGER.info("About to do something awesome")
    sparkapp = SparkApp("higgs-social-network", allow_local_io=True)  # Intend to run standalone

    return 0


if __name__ == "__main__":
    # pylint: disable=wrong-import-position, wrong-import-order, ungrouped-imports
    import sys
    import prm.utils.logging_ext

    prm.utils.logging_ext.setup_logging_stdout_handler()

    with SparkApp("higgs-social-network", allow_local_io=True):
        RETURN_CODE = main()

    sys.exit(RETURN_CODE)
