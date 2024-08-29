from asyncio import sleep
import base64
from datetime import datetime, timedelta
import hmac
import json
import re
import unicodedata
import streamlit as st
from stqdm import stqdm
import pandas as pd
import logging
from google.oauth2 import service_account
from google.cloud import bigquery
from cryptography.fernet import Fernet
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


def send_bigquery(df: pd.DataFrame, table_id: str, schema: list):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {} in {}".format(
            table.num_rows, len(table.schema), table_id, datetime.now()
        )
    )

    print("Send to biquery !")


def query_bigquery(sql_query: str):
    query_job = client.query(sql_query)

    return query_job.result()


def encrypt_password(password, key):
    fernet = Fernet(key)
    encrypted_password = fernet.encrypt(password.encode())
    return encrypted_password


def decrypt_password(encrypted_password, key):
    fernet = Fernet(key)
    decrypted_password = fernet.decrypt(encrypted_password).decode()
    return decrypted_password


# classe para requisições no bling
class api_bling:
    def __init__(self):
        self.cache = {}
        self._401_count = 0

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
                if self._401_count >= 2:
                    response.raise_for_status()

                self._401_count += 1
                self.cache.clear()
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
                if self._401_count >= 2:
                    response.raise_for_status()

                self._401_count += 1
                self.cache.clear()
                return self.post(url, loja)
            elif response.status_code == 429:
                sleep(2)
                return self.post(url, loja)
            else:
                response.raise_for_status()
        except Exception as e:
            print("Ocorreu um erro:", e)
            return "error"

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

        if datetime.now() > data_limite or self._401_count >= 2:
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


def atualizar_lista_estoque_full(sku_to_check, inventory, qtd, id):
    found = False
    for item in inventory:
        if item["sku"] == sku_to_check:
            item["qtd"] = item["qtd"] + qtd
            found = True
            break

    if not found:
        new_item = {"sku": sku_to_check, "qtd": qtd, "id": id}
        inventory.append(new_item)

    return inventory


def gerar_df_lista_full(planilha: pd.DataFrame, df_produtos: pd.DataFrame):
    lista_estoque_full = []
    try:
        df_planilha_full = planilha
        df_planilha_full = df_planilha_full.drop(index=df_planilha_full.index[:14])
        df_planilha_full = pd.DataFrame(df_planilha_full.iloc[:, 18])
        df_planilha_full = df_planilha_full.rename(
            columns={df_planilha_full.columns[0]: "QTD"}
        )
        df_planilha_full = df_planilha_full.groupby(df_planilha_full.index)["QTD"].sum()
        data_dict = df_planilha_full.to_dict()

        for sku, qtd in data_dict.items():
            if sku in df_produtos["sku"].values:
                estrutura = (
                    df_produtos["estrutura"].loc[df_produtos["sku"] == sku].values[0]
                )
                estrutura = re.sub(
                    r"(\w+):", r'"\1":', estrutura
                )  # Adiciona aspas duplas às chaves
                estrutura = re.sub(
                    r"\'", r'"', estrutura
                )  # Substitui aspas simples por aspas duplas nos valores
                estrutura = json.loads(estrutura)  # Converte o JSON para um dicionário

                if len(estrutura["componentes"]) > 0:
                    for componente in estrutura["componentes"]:
                        id = str(componente["produto"]["id"])
                        sku = (
                            df_produtos["sku"].loc[df_produtos["id"] == id].values[0]
                            if len(
                                df_produtos["sku"].loc[df_produtos["id"] == id].values
                            )
                            > 0
                            else None
                        )

                        qtd_composicao = int(componente["quantidade"]) * qtd

                        lista_estoque_full = atualizar_lista_estoque_full(
                            sku, lista_estoque_full, qtd_composicao, id
                        )
                else:
                    lista_estoque_full = atualizar_lista_estoque_full(
                        sku,
                        lista_estoque_full,
                        qtd,
                        df_produtos["id"].loc[df_produtos["sku"] == sku].values[0],
                    )

        return lista_estoque_full

    except FileNotFoundError as e:
        print(
            f"Erro: Arquivo {planilha} nâo encontrado. Verifique se o caminho e o nome do arquivo estão corretos.\nDetalhes do erro: {e}"
        )
        return [[]]


# Id e nome dos depósitos
deposito_full = {
    "proteloja": {"deposito": int(9738790725), "nome": "Proteloja"},
    "vendolandia": {"deposito": int(14197230585), "nome": "Vendolandia"},
    "vendolandia2": {"deposito": int(14886665514), "nome": "Vendolandia2"},
}


# Função para atualizar full
def processar_arquivo(uploaded_file, nome_loja):
    with st.spinner(f"Processando arquivo do full {nome_loja}..."):
        logger.debug("inicio processo")

        query_job = query_bigquery(
            """
            SELECT * FROM `integracao-414415.data_ptl.produtos_bling_proteloja`
            """
        )

        logger.debug("query job done")

        df_produtos = pd.DataFrame(
            data=[row.values() for row in query_job],
            columns=[field.name for field in query_job.schema],
        )

        api = api_bling()

        try:
            df_estoque = pd.read_excel(uploaded_file, index_col=3)
            df_lista_full_sku_matriz = gerar_df_lista_full(df_estoque, df_produtos)
            logger.debug("gerar_df_lista_full done")

            if len(df_lista_full_sku_matriz[0]) > 0:
                status_placeholder = st.empty()
                for row in stqdm(df_lista_full_sku_matriz):
                    if row["sku"]:
                        body = {
                            "deposito": {"id": deposito_full[nome_loja]["deposito"]},
                            "operacao": "B",
                            "produto": {"id": int(row["id"])},
                            "quantidade": float(row["qtd"]),
                            "preco": float(0),
                            "custo": float(0),
                            "observacoes": "AUTOMÁTICO - ESTOQUE FULL ML ATUALIZADO",
                        }

                        api.post(
                            "https://www.bling.com.br/Api/v3/estoques",
                            body,
                            "proteloja",
                        )

                        status_placeholder.text(
                            f"Sku {row['sku']} atualizado com {int(row['qtd'])} de estoque !"
                        )

                status_placeholder.text("")
        except AttributeError as e:
            logger.error(e)


# Título 1 do aplicativo
st.header("Atualize Estoque Full do Bling 🚛", divider="orange")


# Função para normalizar strings com caracteres Unicode
def normalize_string(s):
    return unicodedata.normalize("NFKC", s)


# Senha de Login
def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        # Normaliza as strings antes de compará-las
        entered_password = normalize_string(st.session_state["password"])
        correct_password = normalize_string(st.secrets["password"])

        if hmac.compare_digest(entered_password, correct_password):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the password.
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show input for password.
    st.text_input(
        "Password", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state:
        st.error("😕 Senha incorreta")
    return False


if not check_password():
    st.stop()  # Do not continue if check_password is not True.


# Título 2 do aplicativo
st.subheader("Faça o upload dos relatórios de estoque do mercado livre")

# Botões para upload de arquivos
planilha_estoque_proteloja = st.file_uploader(
    "Suba a planilha de estoque para Proteloja", type=["xlsx", "xls"]
)
planilha_estoque_vendolandia = st.file_uploader(
    "Suba a planilha de estoque para Vendolandia", type=["xlsx", "xls"]
)
planilha_estoque_vendolandia2 = st.file_uploader(
    "Suba a planilha de estoque para Vendolandia2", type=["xlsx", "xls"]
)

# verifica se o botão está habilitado com o estado da sessão
if "run_button" in st.session_state and st.session_state.run_button == True:
    st.session_state.running = True
else:
    st.session_state.running = False

# após botão ser clicado, a função é executada e o botão desabilitado
if st.button(
    "Processar Planilhas", disabled=st.session_state.running, key="run_button"
):

    if planilha_estoque_proteloja is not None:
        processar_arquivo(planilha_estoque_proteloja, "proteloja")

    if planilha_estoque_vendolandia is not None:
        processar_arquivo(planilha_estoque_vendolandia, "vendolandia")

    if planilha_estoque_vendolandia2 is not None:
        processar_arquivo(planilha_estoque_vendolandia2, "vendolandia2")

    st.rerun()
