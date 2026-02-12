from fastapi import FastAPI, UploadFile, File, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import pandas as pd

from engine.engine_pcp import executar_ciclo_pcp
from app.config import PARAMETROS_SISTEMA

# ⬇️ OBJETO ASGI (OBRIGATÓRIO)
app = FastAPI()

# ⚠️ CONFIRME SE O CAMINHO EXISTE EXATAMENTE ASSIM
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "Templates"))

@app.get("/")
def root():
    return {"status": "PCP i9 rodando"}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "gargalo": None,
            "atraso_total": None,
            "ranking": [],
            "erro": None
        }
    )


@app.post("/upload_pedidos", response_class=HTMLResponse)
async def upload_pedidos(
    request: Request,
    horas_disponiveis: float = Form(...),
    eficiencia: float = Form(...),
    setup: float = Form(...),
    file: UploadFile = File(...)
):
    try:
        # 1️⃣ Ler arquivo Excel
        df = pd.read_excel(file.file)

        print("=== DEBUG DATAFRAME ===")
        print(df.head())
        print(df.columns)

        # 2️⃣ Montar parâmetros
        parametros = {
            "horas_disponiveis_dia": horas_disponiveis,
            "eficiencia_media": eficiencia,
            "tempo_setup_medio": setup,
            "recursos": {
                "Corte": {"quantidade": 1},
                "Usinagem": {"quantidade": 1},
                "Montagem": {"quantidade": 1}
            }
        }

        # 3️⃣ Executar engine
        resultado = executar_ciclo_pcp(df, parametros)

        if resultado is None:
            raise ValueError("executar_ciclo_pcp retornou None")

        if not isinstance(resultado, dict):
            raise ValueError(f"Engine retornou tipo inválido: {type(resultado)}")

        # 4️⃣ Extrair resultados com segurança
        gargalo = resultado.get("gargalo")
        atraso_total = resultado.get("atraso_total")

        ranking_df = resultado.get("ranking")

        if isinstance(ranking_df, pd.DataFrame):
            ranking = ranking_df.to_dict(orient="records")
        else:
            ranking = []

        # 5️⃣ Renderizar dashboard
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "gargalo": gargalo,
                "atraso_total": atraso_total,
                "ranking": ranking,
                "erro": None
            }
        )

    except Exception as e:
        # ❗ NUNCA QUEBRAR O DASHBOARD
        print("ERRO NO ENDPOINT /upload_pedidos:", e)

        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "gargalo": "ERRO",
                "atraso_total": "ERRO",
                "ranking": [],
                "erro": str(e)
            }
        )
