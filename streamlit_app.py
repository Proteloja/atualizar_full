import hmac
import streamlit as st
from stqdm import stqdm
import pandas as pd

from scripts.atualizar_full import gerar_df_lista_full
from scripts.bigquery import query_bigquery
from scripts.requisicoes_bling import api_bling

st.header("Atualize Estoque Full do Bling 🚛", divider="orange")


def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if hmac.compare_digest(st.session_state["password"], st.secrets["password"]):
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
        st.error("😕 Password incorrect")
    return False


if not check_password():
    st.stop()  # Do not continue if check_password is not True.


# Verifica se um arquivo foi enviado
def processar_arquivo(uploaded_file, nome_loja):
    with st.spinner(f"Processando arquivo do full {nome_loja}..."):
        deposito_full = {
            "proteloja": {"deposito": int(9738790725), "nome": "Proteloja"},
            "vendolandia": {"deposito": int(14197230585), "nome": "Vendolandia"},
            "vendolandia2": {"deposito": int(14886665514), "nome": "Vendolandia2"},
        }
        print("inicio processo")

        query_job = query_bigquery(
            """
            SELECT * FROM `integracao-414415.data_ptl.produtos_bling_proteloja`
            """
        )

        print("query job done")

        df_produtos = pd.DataFrame(
            data=[row.values() for row in query_job],
            columns=[field.name for field in query_job.schema],
        )

        api = api_bling()

        try:
            df_estoque = pd.read_excel(uploaded_file, index_col=3)
            df_lista_full_sku_matriz = gerar_df_lista_full(df_estoque, df_produtos)
            print("gerar_df_lista_full done")

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
            print(e)


# Título do aplicativo
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

if "run_button" in st.session_state and st.session_state.run_button == True:
    st.session_state.running = True
else:
    st.session_state.running = False

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
