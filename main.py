import datetime
import json
import time
import itertools
import re
import pandas as pd
import functions_framework
from google.cloud import storage
from utils import get_data_binance


storage_client = storage.Client()
BUCKET_NAME = "crypto-data-candle"
JOB_NAME = "weekly_extract"

symbols_list = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
intervals_list = ["1h", "1d"]
symbols_intervals_list = list(itertools.product(symbols_list, intervals_list))


def log_event(event, severity="INFO", **fields):
    print(json.dumps({"severity": severity, "event": event, "job": JOB_NAME, **fields}), flush=True)


def recuperer_dernier_timestamp_global():
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="crypto_")

    timestamps_trouves = []
    pattern = r"_to_(\d+)\.parquet$"

    for blob in blobs:
        match = re.search(pattern, blob.name)
        if match:
            timestamps_trouves.append(int(match.group(1)))

    if timestamps_trouves:
        return max(timestamps_trouves)
    return 0


@functions_framework.http
def executer_tache_hebdomadaire(request):
    start = time.time()
    try:
        date_maintenant = datetime.datetime.now()
        end_time = int(date_maintenant.timestamp() * 1000)
        start_time = recuperer_dernier_timestamp_global()

        log_event("weekly_extract_started", start_time=start_time, end_time=end_time)

        tous_les_dataframes = []
        pairs_ok, pairs_failed, pairs_empty = [], [], []

        for symbole, intervalle in symbols_intervals_list:
            pair = f"{symbole}_{intervalle}"
            try:
                donnees_crypto = get_data_binance(
                    symbole, intervalle, start_time=start_time, end_time=end_time
                )
            except Exception as e:
                log_event("binance_fetch_failed", severity="ERROR",
                          symbol=symbole, interval=intervalle, error=str(e))
                pairs_failed.append(pair)
                continue

            if not donnees_crypto:
                log_event("binance_data_empty", severity="WARNING",
                          symbol=symbole, interval=intervalle)
                pairs_empty.append(pair)
                continue

            df_temp = pd.DataFrame(donnees_crypto)
            df_temp["symbole"] = symbole
            df_temp["intervalle"] = intervalle
            tous_les_dataframes.append(df_temp)
            pairs_ok.append(pair)
            time.sleep(30)

        if not tous_les_dataframes:
            log_event("weekly_extract_no_data", severity="ERROR",
                      pairs_failed=pairs_failed, pairs_empty=pairs_empty,
                      duration_s=round(time.time() - start, 2))
            return "Aucune donnée récupérée", 204

        df_final = pd.concat(tous_les_dataframes, ignore_index=True)
        df_final = df_final.drop_duplicates()  # correction : le résultat était ignoré

        nom_fichier = f"crypto_{start_time}_to_{end_time}.parquet"
        df_final.to_parquet(f"gs://{BUCKET_NAME}/{nom_fichier}", index=False, engine="pyarrow")

        log_event("weekly_extract_success",
                  file=nom_fichier, rows=len(df_final),
                  pairs_ok=len(pairs_ok), pairs_failed=len(pairs_failed),
                  pairs_empty=len(pairs_empty),
                  duration_s=round(time.time() - start, 2))
        return "Extraction et sauvegarde réussies", 200

    except Exception as e:
        log_event("weekly_extract_failed", severity="ERROR",
                  error=str(e), duration_s=round(time.time() - start, 2))
        raise
