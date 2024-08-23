from datetime import datetime, timedelta
import pandas as pd
import requests
from time import sleep
import json
import os
import base64
from google.cloud import bigquery
from scripts.bigquery import query_bigquery, send_bigquery
from scripts.criptografia import decrypt_password, encrypt_password
import streamlit as st


class api_bling:
    def __init__(self):
        self.cache = {}

    def get(self, url, loja: str):
        try:
            titulo_loja = "".join(loja.split()).upper()
            acess_token = self._access_token(titulo_loja)

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {acess_token}",
            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                self.invalidate_cache(loja)
                return self.get(url, loja)
            elif response.status_code == 429:
                sleep(2)
                return self.get(url, loja)
            else:
                response.raise_for_status()
        except Exception as e:
            print("Ocorreu um erro:", e)
            return "error"

    def post(self, url, body, loja: str):
        try:
            titulo_loja = "".join(loja.split()).upper()
            acess_token = self._access_token(titulo_loja)

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {acess_token}",
            }
            payload = json.dumps(body)
            response = requests.post(url, headers=headers, data=payload)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                self.invalidate_cache(loja)
                return self.get(url, loja)
            elif response.status_code == 429:
                sleep(2)
                return self.get(url, loja)
            else:
                response.raise_for_status()
        except Exception as e:
            print("Ocorreu um erro:", e)
            return "error"

    def invalidate_cache(self, loja=None):
        if loja:
            if loja in self.cache:
                del self.cache[loja]
        else:
            self.cache.clear()

    def _oauth_refresh(self, df_credenciais: pd.DataFrame, loja: str) -> str:
        chave_criptografia = str(st.secrets["chave_criptografia"]).encode()
        bling_client_id = st.secrets[f"BLING_CLIENT_ID_{loja}"]
        bling_client_secret = st.secrets[f"BLING_CLIENT_SECRET_{loja}"]

        refresh_token = decrypt_password(
            str(
                df_credenciais["valor"]
                .loc[
                    (df_credenciais["titulo"] == "refresh_token")
                    & (df_credenciais["loja"] == f"BLING_{loja}")
                ]
                .values[0]
            ),
            chave_criptografia,
        )

        credentialbs4 = f"{bling_client_id}:{bling_client_secret}"
        credentialbs4 = credentialbs4.encode("ascii")
        credentialbs4 = base64.b64encode(credentialbs4)
        credentialbs4 = bytes.decode(credentialbs4)

        header = {"Accept": "1.0", "Authorization": f"Basic {credentialbs4}"}
        dice = {"grant_type": "refresh_token", "refresh_token": refresh_token}

        api = requests.post(
            "https://www.bling.com.br/Api/v3/oauth/token", headers=header, json=dice
        )

        situationStatusCode = api.status_code
        api = api.json()

        if situationStatusCode == 400:
            print(f"Request failed. code: {situationStatusCode}")
            print(api)

        df_credenciais.loc[
            (df_credenciais["loja"] == f"BLING_{loja}")
            & (df_credenciais["titulo"] == "access_token"),
            "valor",
        ] = encrypt_password(api["access_token"], chave_criptografia)

        df_credenciais.loc[
            (df_credenciais["loja"] == f"BLING_{loja}")
            & (df_credenciais["titulo"] == "access_token"),
            "validade",
        ] = str(datetime.now())

        df_credenciais.loc[
            (df_credenciais["loja"] == f"BLING_{loja}")
            & (df_credenciais["titulo"] == "refresh_token"),
            "valor",
        ] = encrypt_password(api["refresh_token"], chave_criptografia)

        df_credenciais.loc[
            (df_credenciais["loja"] == f"BLING_{loja}")
            & (df_credenciais["titulo"] == "refresh_token"),
            "validade",
        ] = str(datetime.now())

        schema = [
            bigquery.SchemaField("loja", "STRING"),
            bigquery.SchemaField("titulo", "STRING"),
            bigquery.SchemaField("validade", "STRING"),
            bigquery.SchemaField("valor", "STRING"),
        ]
        table_id = f"integracao-414415.data_ptl.credenciais"

        send_bigquery(df_credenciais, table_id, schema)

        return api["access_token"]

    def _validade_access_token(self, df_credenciais: pd.DataFrame, loja: str) -> str:

        data_atualizacao = datetime.strptime(
            df_credenciais["validade"]
            .loc[
                (df_credenciais["titulo"] == "access_token")
                & (df_credenciais["loja"] == f"BLING_{loja}")
            ]
            .values[0],
            "%Y-%m-%d %H:%M:%S.%f",
        )

        data_limite = data_atualizacao + timedelta(hours=6)

        if datetime.now() > data_limite:
            return self._oauth_refresh(df_credenciais, loja)

        return decrypt_password(
            df_credenciais["valor"]
            .loc[
                (df_credenciais["titulo"] == "access_token")
                & (df_credenciais["loja"] == f"BLING_{loja}")
            ]
            .values[0],
            str(st.secrets["chave_criptografia"]).encode(),
        )

    def _access_token(self, loja: str) -> str:
        if loja in self.cache:
            return self.cache[loja]

        results_query_credenciais = query_bigquery(
            "SELECT * FROM `integracao-414415.data_ptl.credenciais`"
        )

        df_credenciais = pd.DataFrame(
            data=[row.values() for row in results_query_credenciais],
            columns=[field.name for field in results_query_credenciais.schema],
        )

        self.cache[loja] = self._validade_access_token(df_credenciais, loja)

        return self.cache[loja]
