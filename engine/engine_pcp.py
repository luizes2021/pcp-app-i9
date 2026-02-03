import pandas as pd

def identificar_gargalo(df):
    carga = df.groupby("recurso")["tempo_proc"].sum()
    gargalo = carga.idxmax()
    return gargalo, carga[gargalo]

def calcular_atraso_acumulado(df):
    df["atraso"] = (df["data_entrega"] - df["data_prometida"]).dt.days
    return df["atraso"].clip(lower=0).sum()

def ranquear_pedidos(df):
    df = df.sort_values(by=["atraso", "tempo_proc"], ascending=[False, True])
    df["prioridade"] = range(1, len(df) + 1)
    return df

def executar_ciclo_pcp(df):
    gargalo, carga = identificar_gargalo(df)
    atraso_total = calcular_atraso_acumulado(df)
    ranking = ranquear_pedidos(df)

    return {
        "gargalo": gargalo,
        "carga_gargalo": float(carga),
        "atraso_acumulado": int(atraso_total),
        "ranking": ranking.to_dict(orient="records")
    }
