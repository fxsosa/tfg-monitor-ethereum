import pandas as pd
import logging
from df_utilities import get_df_by_url, df_to_line_protocol, set_df_tags

logger = logging.getLogger(__name__)

def get_df_config_fork_schedule(base_url: str, network: str, token: str) -> pd.pandas:
    endpoint = "/eth/v1/config/fork_schedule"
    args = {"token": token}
    measurement = 'beacon_config_info'

    df = get_df_by_url(base_url, endpoint, args)
    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    return df


def get_df_config_fork_head(base_url: str, network: str, token: str) -> pd.pandas:
    endpoint = "/eth/v1/beacon/states/head/fork"
    args = {"token": token}
    measurement = 'beacon_config_info'

    df = get_df_by_url(base_url, endpoint, args)
    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    return df


def get_df_config_spec(base_url: str, network: str, token: str) -> pd.pandas:
    endpoint = "/eth/v1/config/spec"
    args = {"token": token}
    measurement = 'beacon_config_info'

    df = get_df_by_url(base_url, endpoint, args)
    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    # eliminar campos con hexadecimales
    df = df.drop(columns=[
        col for col in df.columns
        if df[col].astype(str).str.startswith("0x").all()
    ])

    # eliminar columnas que tengan dict/list/tuple/set en algún valor (como BLOB_SCHEDULE)
    df = df.drop(columns=[
        col for col in df.columns
        if df[col].apply(lambda v: isinstance(v, (dict, list, tuple, set))).any()
    ], errors="ignore")

    return df


def get_config_info(beacon_api_url: str, network: str = "mainnet", token: str = '') -> str:
    df_config_fork_schedule = get_df_config_fork_schedule(beacon_api_url, network, token)

    lp_config_fork_schedule = df_to_line_protocol(
        df_config_fork_schedule,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint", "previous_version", "current_version"],
        field_cols=["epoch"],
        timestamp_col="timestamp_df"
    )

    df_config_fork = get_df_config_fork_head(beacon_api_url, network, token)

    lp_config_fork = df_to_line_protocol(
        df_config_fork,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint", "previous_version", "current_version"],
        field_cols=["epoch"],
        timestamp_col="timestamp_df"
    )

    df_config_spec = get_df_config_spec(beacon_api_url, network, token)

    lp_config_spec = df_to_line_protocol(
        df_config_spec,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint"],
        field_cols=None,
        timestamp_col="timestamp_df"
    )

    return lp_config_fork + '\n' + lp_config_fork_schedule + '\n' + lp_config_spec

    
def setup_logger():
    """Configura el logger básico para consola."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main():
    setup_logger()
    print(get_config_info("https://www.lightclientdata.org", "mainnet"))


if __name__ == "__main__":
    main()