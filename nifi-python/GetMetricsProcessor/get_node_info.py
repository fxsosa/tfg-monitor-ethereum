import pandas as pd
import logging
from df_utilities import get_df_by_url, df_to_line_protocol, get_df_status_code, extract_name_version, set_df_tags

logger = logging.getLogger(__name__)

def get_df_node_health(base_url: str, network: str, token: str) -> pd.pandas:
    endpoint = "/eth/v1/node/health"
    args = {"token": token}
    measurement = 'beacon_node_info'

    return get_df_status_code(base_url, endpoint, args, measurement, network)


def get_df_node_syncing(base_url: str, network: str, token: str) -> pd.pandas:
    endpoint = "/eth/v1/node/syncing"
    args = {"token": token}
    measurement = 'beacon_node_info'

    df = get_df_by_url(base_url, endpoint, args)
    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    return df


def get_df_peer_count(base_url: str, network: str, token: str) -> pd.pandas:
    endpoint = "/eth/v1/node/peer_count"
    args = {"token": token}
    measurement = 'beacon_node_info'

    df = get_df_by_url(base_url, endpoint, args)
    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    return df

def get_df_peers(base_url: str, network: str, token: str) -> pd.pandas:
    endpoint = "/eth/v1/node/peers"
    args = {"token": token}
    measurement = 'beacon_node_info'

    df = get_df_by_url(base_url, endpoint, args)

    df[['agent_name', 'agent_version']] = df['agent'].apply(extract_name_version).apply(pd.Series)
    df[['proto_name', 'proto_version']] = df['proto'].apply(extract_name_version).apply(pd.Series)
    df_grouped = df.groupby(['state','direction','agent_name', 'agent_version','proto_name', 'proto_version']).size().reset_index(name='count')

    df = set_df_tags(df_grouped, base_url, endpoint, measurement, network, args)


    return df_grouped

def get_node_info(beacon_api_url: str, network: str = "mainnet", token: str = '') -> str:
    df_health = get_df_node_health(beacon_api_url, network, token)

    lp_health = df_to_line_protocol(
        df_health,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint"],
        field_cols=["status_code"],
        timestamp_col="timestamp_df"
    )

    df_node_syncing = get_df_node_syncing(beacon_api_url, network, token)

    lp_node_syncing = df_to_line_protocol(
        df_node_syncing,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint"],
        field_cols=None,
        timestamp_col="timestamp_df"
    )

    df_node_peers_count = get_df_peer_count(beacon_api_url, network, token)

    lp_node_peers_count = df_to_line_protocol(
        df_node_peers_count,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint"],
        field_cols=None,
        timestamp_col="timestamp_df"
    )

    # Query pesada
    #df_node_peers = get_df_peers(beacon_api_url, network)

    #lp_node_peers = df_to_line_protocol(
    #    df_node_peers,
    #    measurement_col="measurement",
    #    tag_cols=["source", "network", "endpoint", 'state','direction','agent_name', 'agent_version','proto_name', 'proto_version'],
    #    field_cols=None,
    #    timestamp_col="timestamp"
    #)
    return lp_health + '\n' + lp_node_syncing + '\n' + lp_node_peers_count# + '\n' + lp_node_peers

    
def setup_logger():
    """Configura el logger básico para consola."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main():
    setup_logger()
    print(get_node_info("https://www.lightclientdata.org", "mainnet"))


if __name__ == "__main__":
    main()
