import datetime
from fastapi import APIRouter, Request, Depends
from typing import Optional
from fastapi.concurrency import run_in_threadpool
from supabase import Client
from core.database import get_dados_apurados
from core.analysis import gerar_dashboard_e_mapas
from core.security import get_current_user # Segurança

router = APIRouter(prefix="/xadrez", tags=["Xadrez"])

def get_supabase(request: Request) -> Client:
    return request.state.supabase

def processar_xadrez_sincrono(df, view_mode):
    resumo_viagens, dashboard_equipas = [], None
    if view_mode == 'equipas_fixas':
        resultado_xadrez = gerar_dashboard_e_mapas(df)
        dashboard_equipas = resultado_xadrez["dashboard_data"]
    else: 
        # Lógica simplificada para detalhado
        resumo_df = df.sort_values(by='MOTORISTA')
        resumo_df.fillna('', inplace=True)
        resumo_viagens = resumo_df.to_dict('records')
    return resumo_viagens, dashboard_equipas

@router.get("/")
async def ler_relatorio_xadrez(
    request: Request, 
    view_mode: str = "equipas_fixas",
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    search_query: Optional[str] = None,
    current_user: dict = Depends(get_current_user), # <--- Proteção
    supabase: Client = Depends(get_supabase)
):

    hoje = datetime.date.today()
    data_inicio = data_inicio or hoje.replace(day=1).isoformat()
    data_fim = data_fim or hoje.isoformat()
    search_str = search_query or ""
    
    df, error = await run_in_threadpool(get_dados_apurados, supabase, data_inicio, data_fim, search_str)
    
    resumo, dashboard = [], []
    if not error and df is not None:
        if 'MAPA' in df.columns: df = df.drop_duplicates(subset=['MAPA'])
        else: df = df.drop_duplicates()
        
        resumo, dashboard = await run_in_threadpool(processar_xadrez_sincrono, df, view_mode)

    return {
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "view_mode": view_mode,
        "dashboard": dashboard,
        "resumo": resumo,
        "error": error
    }