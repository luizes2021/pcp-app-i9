import pandas as pd


def executar_ciclo_pcp(df: pd.DataFrame, parametros: dict) -> dict:
    """
    Engine de PCP baseado em TOC para Job Shop.
    Quantidade é tratada como unidades inteiras.

    Colunas obrigatórias da planilha:
    - pedido
    - recurso
    - tempo_processamento   (tempo unitário)
    - quantidade            (inteiro)
    - data_entrega
    """

    # -----------------------------
    # 1️⃣ Validação das colunas
    # -----------------------------
    colunas_obrigatorias = {
        "pedido",
        "recurso",
        "tempo_processamento",
        "quantidade",
        "data_entrega"
    }

    if not colunas_obrigatorias.issubset(df.columns):
        raise ValueError(
            f"Planilha inválida. Esperado: {colunas_obrigatorias}, "
            f"Recebido: {set(df.columns)}"
        )

    df = df.copy()

    # Tipos corretos
    df["quantidade"] = df["quantidade"].astype(int)
    df["tempo_processamento"] = df["tempo_processamento"].astype(float)
    df["data_entrega"] = pd.to_datetime(df["data_entrega"])

    # -----------------------------
    # 2️⃣ Parâmetros do sistema
    # -----------------------------
    horas_disponiveis = parametros.get("horas_disponiveis_dia", 8.0)
    eficiencia = parametros.get("eficiencia_media", 1.0)
    setup = parametros.get("tempo_setup_medio", 0.0)

    # -----------------------------
    # 3️⃣ Tempo requerido (TOC)
    # -----------------------------
    # Tempo total = quantidade × tempo unitário
    df["tempo_requerido"] = df["quantidade"] * df["tempo_processamento"]

    # Ajustes operacionais
    df["tempo_ajustado"] = (df["tempo_requerido"] / eficiencia) + setup

    # -----------------------------
    # 4️⃣ Carga por recurso
    # -----------------------------
    carga_recurso = (
        df.groupby("recurso")["tempo_ajustado"]
        .sum()
        .reset_index()
        .rename(columns={"tempo_ajustado": "carga_total"})
    )

    carga_recurso["capacidade"] = horas_disponiveis
    carga_recurso["utilizacao"] = (
        carga_recurso["carga_total"] / carga_recurso["capacidade"]
    )

    # -----------------------------
    # 5️⃣ Identificação do gargalo
    # -----------------------------
    gargalo = (
        carga_recurso
        .sort_values("utilizacao", ascending=False)
        .iloc[0]["recurso"]
    )

    # -----------------------------
    # 6️⃣ Ranking TOC (EDD no gargalo)
    # -----------------------------
    ranking = (
        df[df["recurso"] == gargalo]
        .sort_values("data_entrega")
        .reset_index(drop=True)
    )

    ranking["ordem"] = ranking.index + 1
    ranking["tempo_acumulado"] = ranking["tempo_ajustado"].cumsum()

    # -----------------------------
    # 7️⃣ Cálculo de atraso
    # -----------------------------
    ranking["atraso"] = (
        ranking["tempo_acumulado"] - horas_disponiveis
    ).clip(lower=0)

    atraso_total = ranking["atraso"].sum()

    # -----------------------------
    # 8️⃣ Retorno PADRONIZADO
    # -----------------------------
    return {
        "gargalo": str(gargalo),
        "atraso_total": float(atraso_total),
        "ranking": ranking
    }
