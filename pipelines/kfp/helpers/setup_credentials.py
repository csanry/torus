import os

from pipelines.kfp.dependencies import CREDENTIALS_FILE


def setup_credentials() -> None:

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_FILE

