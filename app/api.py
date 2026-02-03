from fastapi import FastAPI, UploadFile, File
import pandas as pd
from engine.engine_pcp import executar_ciclo_pcp

app = FastAPI()

@app.get("/")
def root():
    return {"ok": "funcionando"}

@app.post("/upload_pedidos")
async def upload_pedidos(file: UploadFile = File(...)):
    df = pd.read_excel(file.file)
    df["data_entrega"] = pd.to_datetime(df["data_entrega"])
    df["data_prometida"] = pd.to_datetime(df["data_prometida"])

    resultado = executar_ciclo_pcp(df)
    return resultado
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request}
    )

@app.post("/dashboard", response_class=HTMLResponse)
async def dashboard_post(request: Request, file: UploadFile = File(...)):
    df = pd.read_excel(file.file)

    df["data_entrega"] = pd.to_datetime(df["data_entrega"])
    df["data_prometida"] = pd.to_datetime(df["data_prometida"])

    resultado = executar_ciclo_pcp(df)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "resultado": resultado
        }
    )






