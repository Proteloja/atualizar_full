import json
import pandas as pd
import re



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


