import datetime
import pandas as pd
from fastapi import APIRouter, Request, Depends
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool
from supabase import Client

# Importa as funções de base de dados
from core.database import get_dados_apurados, get_cadastro_sincrono, get_indicadores_sincrono, get_caixas_sincrono
from core.analysis import gerar_dashboard_e_mapas
from .metas import _get_metas_sincrono
from core.security import get_current_user # Segurança
from core.database import get_supabase

router = APIRouter(prefix="/incentivo", tags=["Incentivo"])

# --- FUNÇÃO DE PROCESSAMENTO (Mantida igual à sua lógica original) ---
def processar_incentivos_sincrono(
    df_viagens: Optional[pd.DataFrame], 
    df_cadastro: Optional[pd.DataFrame], 
    df_indicadores: Optional[pd.DataFrame], 
    df_caixas: Optional[pd.DataFrame], 
    metas: Dict[str, Any]
):
    incentivo_motoristas = []
    incentivo_ajudantes = []
    
    metas_motorista = metas.get("motorista", {})
    metas_ajudante = metas.get("ajudante", {})
    
    premio_motorista_map = {}
    default_premio_info = {
        "dev_pdv_val": "N/A", "dev_pdv_passou": False,
        "rating_val": "N/A", "rating_passou": False,
        "refugo_val": "N/A", "refugo_passou": False,
        "premio_caixas": 0.0 
    }
    
    # Mapas de CPF e Indicadores
    cpf_motorista_map = {}
    data_inicio_motorista_map = {}
    cpf_ajudante_map = {}
    data_inicio_ajudante_map = {}
    indicadores_map = {}
    hoje = datetime.date.today()

    if df_cadastro is not None and not df_cadastro.empty:
        df_motoristas_cadastro = df_cadastro[pd.notna(df_cadastro['Codigo_M'])].drop_duplicates(subset=['Codigo_M'])
        df_motoristas_cadastro['Codigo_M_int'] = pd.to_numeric(df_motoristas_cadastro['Codigo_M'], errors='coerce').fillna(0).astype(int)
        cpf_motorista_map = df_motoristas_cadastro.set_index('Codigo_M_int')['CPF_M'].to_dict()
        df_motoristas_cadastro['Data_M_dt'] = pd.to_datetime(df_motoristas_cadastro['Data_M'], errors='coerce').dt.date
        data_inicio_motorista_map = df_motoristas_cadastro.set_index('Codigo_M_int')['Data_M_dt'].to_dict()

        df_ajudantes_cadastro = df_cadastro[pd.notna(df_cadastro['Codigo_J'])].drop_duplicates(subset=['Codigo_J'])
        df_ajudantes_cadastro['Codigo_J_int'] = pd.to_numeric(df_ajudantes_cadastro['Codigo_J'], errors='coerce').fillna(0).astype(int)
        cpf_ajudante_map = df_ajudantes_cadastro.set_index('Codigo_J_int')['CPF_J'].to_dict()
        df_ajudantes_cadastro['Data_J_dt'] = pd.to_datetime(df_ajudantes_cadastro['Data_J'], errors='coerce').dt.date
        data_inicio_ajudante_map = df_ajudantes_cadastro.set_index('Codigo_J_int')['Data_J_dt'].to_dict()

    if df_indicadores is not None and not df_indicadores.empty:
        df_indicadores['dev_pdv'] = pd.to_numeric(df_indicadores['dev_pdv'], errors='coerce')
        df_indicadores['Rating_tx'] = pd.to_numeric(df_indicadores['Rating_tx'], errors='coerce')
        df_indicadores['refugo'] = pd.to_numeric(df_indicadores['refugo'], errors='coerce')
        indicadores_map = df_indicadores.set_index('Codigo_M').to_dict('index')

    # Lógica de Motoristas
    if df_viagens is not None and not df_viagens.empty:
        motoristas_no_periodo = df_viagens[['COD', 'MOTORISTA']].drop_duplicates(subset=['COD'])
        
        incentivo_motoristas, premio_motorista_map = processar_motoristas(motoristas_no_periodo, cpf_motorista_map, indicadores_map, metas_motorista)

        # Lógica de Ajudantes (Herança)
        res_xadrez = gerar_dashboard_e_mapas(df_viagens)
        mapas = res_xadrez["mapas"]
        df_melted = res_xadrez["df_melted"]
        motorista_fixo_map = mapas.get("motorista_fixo_map", {})
        ajudantes_unicos = df_melted.drop_duplicates(subset=['AJUDANTE_COD'])
        
        for _, ajudante in ajudantes_unicos.iterrows():
            cod = ajudante['AJUDANTE_COD']
            cod_mot = motorista_fixo_map.get(cod)
            herdado = premio_motorista_map.get(cod_mot, default_premio_info) if cod_mot else default_premio_info
            
            p_dev = metas_ajudante.get("dev_pdv_premio", 0) if herdado["dev_pdv_passou"] else 0.0
            p_rat = metas_ajudante.get("rating_premio", 0) if herdado["rating_passou"] else 0.0
            p_ref = metas_ajudante.get("refugo_premio", 0) if herdado["refugo_passou"] else 0.0
            
            incentivo_ajudantes.append({
                "cpf": cpf_ajudante_map.get(cod, ""), 
                "cod": cod,
                "nome": ajudante['AJUDANTE_NOME'],
                "dev_pdv_val": herdado["dev_pdv_val"],
                "dev_pdv_premio_val": p_dev,
                "rating_val": herdado["rating_val"],
                "rating_premio_val": p_rat,
                "refugo_val": herdado["refugo_val"],
                "refugo_premio_val": p_ref,
                "total_premio": p_dev + p_rat + p_ref
            })
            
    return sorted(incentivo_motoristas, key=lambda x: x['nome']), sorted(incentivo_ajudantes, key=lambda x: x['nome'])

def processar_motoristas(df_viagens, cpf_motorista_map, indicadores_map, metas_motorista):
    incentivo_motoristas = []
    premio_motorista_map = {}
    for _, motorista in df_viagens.iterrows():
        linha = {}
        cod = int(motorista['COD'])
        linha["cpf"] = cpf_motorista_map.get(cod, "") 
        linha["cod"] = cod
        linha["nome"] = str(motorista.get('MOTORISTA', 'N/A')).strip()

        indicadores = indicadores_map.get(cod, {})
        dev = indicadores.get('dev_pdv'); dev = dev * 100 if pd.notna(dev) else None
        rating = indicadores.get('Rating_tx'); rating = rating * 100 if pd.notna(rating) else None
        refugo = indicadores.get('refugo'); refugo = refugo * 100 if pd.notna(refugo) else None
        
        dev_passou = (dev is not None and dev <= metas_motorista.get("dev_pdv_meta_perc", 0))
        rating_passou = (rating is not None and rating >= metas_motorista.get("rating_meta_perc", 0))
        refugo_passou = (refugo is not None and refugo <= metas_motorista.get("refugo_meta_perc", 0))

        linha["dev_pdv_val"] = f"{dev:.2f}%" if dev is not None else "N/A"
        linha["dev_pdv_premio_val"] = metas_motorista.get("dev_pdv_premio", 0) if dev_passou else 0.0
        linha["rating_val"] = f"{rating:.2f}%" if rating is not None else "N/A"
        linha["rating_premio_val"] = metas_motorista.get("rating_premio", 0) if rating_passou else 0.0
        linha["refugo_val"] = f"{refugo:.2f}%" if refugo is not None else "N/A"
        linha["refugo_premio_val"] = metas_motorista.get("refugo_premio", 0) if refugo_passou else 0.0

        linha["total_premio"] = linha["dev_pdv_premio_val"] + linha["rating_premio_val"] + linha["refugo_premio_val"]
        incentivo_motoristas.append(linha)
        
        if linha["cod"]:
            premio_motorista_map[linha["cod"]] = {
                "dev_pdv_val": linha["dev_pdv_val"], "dev_pdv_passou": dev_passou,
                "rating_val": linha["rating_val"], "rating_passou": rating_passou,
                "refugo_val": linha["refugo_val"], "refugo_passou": refugo_passou,
            }
    return incentivo_motoristas, premio_motorista_map


@router.get("/")
async def ler_relatorio_incentivo(
    request: Request, 
    data_inicio: str,
    data_fim: str,
    current_user: dict = Depends(get_current_user), # Proteção
    supabase: Client = Depends(get_supabase)
):
    # Lógica de datas para Indicadores (Período de corte dia 26)
    try:
        d_obj = datetime.date.fromisoformat(data_inicio)
        if d_obj.day < 26:
            d_fim_p = d_obj.replace(day=25)
            d_ini_p = (d_obj.replace(day=1) - datetime.timedelta(days=1)).replace(day=26)
        else:
            d_ini_p = d_obj.replace(day=26)
            d_fim_p = (d_ini_p + datetime.timedelta(days=32)).replace(day=25)
        d_ini_str, d_fim_str = d_ini_p.isoformat(), d_fim_p.isoformat()
    except:
        d_ini_str, d_fim_str = data_inicio, data_fim

    # Buscas
    metas = await run_in_threadpool(_get_metas_sincrono, supabase)
    df_viagens, err1 = await run_in_threadpool(get_dados_apurados, supabase, data_inicio, data_fim, "")
    df_cadastro, err2 = await run_in_threadpool(get_cadastro_sincrono, supabase)
    df_indicadores, err3 = await run_in_threadpool(get_indicadores_sincrono, supabase, d_ini_str, d_fim_str)
    df_caixas, err4 = await run_in_threadpool(get_caixas_sincrono, supabase, data_inicio, data_fim)
    
    error = err1 or err2 or err3 or err4
    
    motoristas, ajudantes = [], []
    if not error and df_viagens is not None:
        # Remove duplicados de mapa para KPIs (igual ao Xadrez)
        if 'MAPA' in df_viagens.columns: 
            df_viagens_dedup = df_viagens.drop_duplicates(subset=['MAPA'])
        else:
            df_viagens_dedup = df_viagens.drop_duplicates()

        motoristas, ajudantes = await run_in_threadpool(
            processar_incentivos_sincrono, df_viagens_dedup, df_cadastro, df_indicadores, df_caixas, metas
        )

    # FILTRO DE SEGURANÇA
    if current_user["role"] != "admin":
        cpf_user = current_user["username"].replace(".", "").replace("-", "")
        motoristas = [m for m in motoristas if str(m.get("cpf", "")).replace(".", "").replace("-", "") == cpf_user]
        ajudantes = [a for a in ajudantes if str(a.get("cpf", "")).replace(".", "").replace("-", "") == cpf_user]

    return {
        "motoristas": motoristas,
        "ajudantes": ajudantes,
        "error": error
    }