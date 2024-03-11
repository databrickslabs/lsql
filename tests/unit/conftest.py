import logging

from databricks.labs.blueprint.logger import install_logger

install_logger()
logging.getLogger("databricks").setLevel("DEBUG")
