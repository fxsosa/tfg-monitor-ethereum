import pandas as pd
import logging
from datetime import datetime
import pytz
from df_utilities import get_df_by_url, df_to_line_protocol, set_df_tags

logger = logging.getLogger(__name__)

# Validación general para listas: get_df_block_info
def safe_len(x):
    return len(x) if isinstance(x, list) else 0

def safe_sum_amount(x, key_path):
    if not isinstance(x, list):
        return 0
    total = 0
    for item in x:
        try:
            # soporte para claves anidadas como "data.amount"
            for key in key_path.split("."):
                item = item[key]
            total += int(item)
        except (KeyError, TypeError, ValueError):
            continue
    return total

def get_df_block_info(base_url: str, network: str, token: str, block_id: str) -> pd.pandas:
    endpoint = "/eth/v2/beacon/blocks/{block_id}"
    args = {"block_id": block_id, "token": token}
    measurement = 'beacon_block_info'

    df = get_df_by_url(base_url, endpoint, args)

    # Calculos de cantidades
    df["count_proposer_slashings"] = df["message.body.proposer_slashings"].apply(safe_len)
    df["count_attester_slashings"] = df["message.body.attester_slashings"].apply(safe_len)
    df["count_attestations"] = df["message.body.attestations"].apply(safe_len)
    df["count_deposits"] = df["message.body.deposits"].apply(safe_len)
    df["count_voluntary_exits"] = df["message.body.voluntary_exits"].apply(safe_len)
    df["count_transactions"] = df["message.body.execution_payload.transactions"].apply(safe_len)
    df["count_withdrawals"] = df["message.body.execution_payload.withdrawals"].apply(safe_len)
    df["count_bls_to_execution_changes"] = df["message.body.bls_to_execution_changes"].apply(safe_len)
    df["count_blob_kzg_commitments"] = df["message.body.blob_kzg_commitments"].apply(safe_len)
    df["count_exec_req_deposits"] = df["message.body.execution_requests.deposits"].apply(safe_len)
    df["count_exec_req_withdrawals"] = df["message.body.execution_requests.withdrawals"].apply(safe_len)
    df["count_exec_req_consolidations"] = df["message.body.execution_requests.consolidations"].apply(safe_len)

    # Sumas de amount
    df["sum_deposit_amount"] = df["message.body.deposits"].apply(lambda x: safe_sum_amount(x, "data.amount"))
    df["sum_withdrawals_amount"] = df["message.body.execution_payload.withdrawals"].apply(lambda x: safe_sum_amount(x, "amount"))
    df["sum_exec_req_deposit_amount"] = df["message.body.execution_requests.deposits"].apply(lambda x: safe_sum_amount(x, "amount"))
    df["sum_exec_req_withdrawal_amount"] = df["message.body.execution_requests.withdrawals"].apply(lambda x: safe_sum_amount(x, "amount"))

    # Se eliminan las columnas que se usaron para los calculos
    df.drop(columns=[
        "message.body.proposer_slashings",
        "message.body.attester_slashings",
        "message.body.attestations",
        "message.body.deposits",
        "message.body.voluntary_exits",
        "message.body.execution_payload.transactions",
        "message.body.execution_payload.withdrawals",
        "message.body.bls_to_execution_changes",
        "message.body.blob_kzg_commitments",
        "message.body.execution_requests.deposits",
        "message.body.execution_requests.withdrawals",
        "message.body.execution_requests.consolidations"
    ], inplace=True)

    # para el timestamp
    df["timestamp"] = df["message.body.execution_payload.timestamp"].astype("int") * 1_000_000_000

    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    # eliminar campos con hexadecimales
    df = df.drop(columns=[
        col for col in df.columns
        if df[col].astype(str).str.startswith("0x").all()
    ])

    return df


def get_df_checkpoints_info(base_url: str, network: str, token: str, state_id: str, name_block_id: str) -> pd.pandas:
    endpoint = "/eth/v1/beacon/states/{state_id}/finality_checkpoints"
    args = {"state_id": state_id, "token": token}
    measurement = 'beacon_block_info'

    df = get_df_by_url(base_url, endpoint, args)

    args = {"state_id": name_block_id}  # Se reajusta con el nombre del identificador
    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    # eliminar campos con hexadecimales
    df = df.drop(columns=[
        col for col in df.columns
        if df[col].astype(str).str.startswith("0x").all()
    ])

    return df


def get_df_blob_info(base_url: str, network: str, token: str, block_id: str, name_block_id: str) -> pd.pandas:
    endpoint = "/eth/v1/beacon/blob_sidecars/{block_id}"
    args = {"block_id": block_id, "token": token}  
    measurement = 'beacon_block_info'

    df = get_df_by_url(base_url, endpoint, args)
    df = pd.DataFrame({"count": [len(df)]}) # Solo se contabiliza los blobs

    args = {"block_id": name_block_id}  # Se reajusta con el nombre del identificador
    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    return df


def get_df_reward_info(base_url: str, network: str, token: str, block_id: str, name_block_id: str) -> pd.pandas:
    endpoint = "/eth/v1/beacon/rewards/blocks/{block_id}"
    args = {"block_id": block_id, "token": token}  
    measurement = 'beacon_block_info'

    df = get_df_by_url(base_url, endpoint, args)

    args = {"block_id": name_block_id}  # Se reajusta con el nombre del identificador
    df = set_df_tags(df, base_url, endpoint, measurement, network, args)

    return df


def get_block_info(beacon_api_url: str, network: str = "mainnet", token: str = "", block_id: str = "head") -> str:
    df_block = get_df_block_info(beacon_api_url, network, token, block_id)

    lp_block = df_to_line_protocol(
        df_block,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint"],
        field_cols=None,
        timestamp_col="timestamp"
    )

    slot = df_block["message.slot"][0]
    timestamp_block = df_block["timestamp"][0]

    df_checkpoints_info = get_df_checkpoints_info(beacon_api_url, network, token, slot, block_id)
    df_checkpoints_info["timestamp"] = timestamp_block

    lp_checkpoints_info = df_to_line_protocol(
        df_checkpoints_info,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint"],
        field_cols=None,
        timestamp_col="timestamp"
    )

    df_blob_info = get_df_blob_info(beacon_api_url, network, token, slot, block_id)
    df_blob_info["timestamp"] = timestamp_block

    lp_blob_info = df_to_line_protocol(
        df_blob_info,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint"],
        field_cols=None,
        timestamp_col="timestamp"
    )

    df_reward_info = get_df_reward_info(beacon_api_url, network, token, slot, block_id)
    df_reward_info["timestamp"] = timestamp_block

    lp_reward_info = df_to_line_protocol(
        df_reward_info,
        measurement_col="measurement",
        tag_cols=["source", "network", "endpoint"],
        field_cols=None,
        timestamp_col="timestamp"
    )

    return lp_block + '\n' + lp_checkpoints_info + '\n' + lp_blob_info + '\n' + lp_reward_info

    
def setup_logger():
    """Configura el logger básico para consola."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main():
    setup_logger()
    print(get_block_info("https://www.lightclientdata.org", "mainnet", "finalized"))


if __name__ == "__main__":
    main()