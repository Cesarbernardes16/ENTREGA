import datetime
import pandas as pd
from fastapi import APIRouter, Request, Depends, HTTPException, Query
from typing import Optional, Dict, Any, List
from fastapi.concurrency import run_in_threadpool
from core.database import get_dados_apurados, get_cadastro_sincrono, get_caixas_sincrono, get_supabase
from .metas import _get_metas_sincrono
from core.security import get_current_user
from supabase import Client

router = APIRouter(prefix="/caixas", tags=["Caixas"])

def _get_valor_por_caixa(dias_antiguidade: int, metas_colaborador: Dict[str, Any]) -> float:
    """Calcula o valor unitário por caixa baseado na antiguidade."""
    try:
        # Nível 4: Mais de 5 anos (1825 dias)
        if dias_antiguidade > metas_colaborador.get("meta_cx_dias_n3", 1825):
            return metas_colaborador.get("meta_cx_valor_n4", 0.0)
        # Nível 3: 2 a 5 anos (730 dias)
        if dias_antiguidade > metas_colaborador.get("meta_cx_dias_n2", 730):
            return metas_colaborador.get("meta_cx_valor_n3", 0.0)
        # Nível 2: 1 a 2 anos (365 dias)
        if dias_antiguidade > metas_colaborador.get("meta_cx_dias_n1", 365):
            return metas_colaborador.get("meta_cx_valor_n2", 0.0)
        # Nível 1: Valor base
        return metas_colaborador.get("meta_cx_valor_n1", 0.0)
    except:
        return 0.0

def processar_caixas_sincrono(df_viagens: pd.DataFrame, df_cadastro: pd.DataFrame, df_caixas: pd.DataFrame, metas: Dict[str, Any]):
    """Processa o bônus de caixas separando motoristas e ajudantes para evitar colisão de IDs."""
    metas_motorista = metas.get("motorista", {})
    metas_ajudante = metas.get("ajudante", {})
    hoje = datetime.date.today()
    
    # --- 1. MAPEAMENTOS DE MOTORISTAS ---
    motorista_antiguidade_map = {}
    motorista_info_map = {} 
    if df_cadastro is not None:
        df_motoristas = df_cadastro[pd.notna(df_cadastro['Codigo_M'])].drop_duplicates(subset=['Codigo_M']).copy()
        df_motoristas['Codigo_M_int'] = pd.to_numeric(df_motoristas['Codigo_M'], errors='coerce').fillna(0).astype(int)
        df_motoristas['Data_M_dt'] = pd.to_datetime(df_motoristas['Data_M'], errors='coerce').dt.date
        for _, row in df_motoristas.iterrows():
            cod = row['Codigo_M_int']
            if cod == 0: continue
            dias = (hoje - row['Data_M_dt']).days if pd.notna(row['Data_M_dt']) else 0
            motorista_antiguidade_map[cod] = dias
            motorista_info_map[cod] = {
                "nome": str(row.get('Nome_M', '')).strip(), 
                "cpf": str(row.get('CPF_M', '')).strip()
            }

    # --- 2. MAPEAMENTOS DE AJUDANTES ---
    ajudante_antiguidade_map = {}
    ajudante_info_map = {}
    if df_cadastro is not None:
        df_ajudantes = df_cadastro[pd.notna(df_cadastro['Codigo_J'])].drop_duplicates(subset=['Codigo_J']).copy()
        df_ajudantes['Codigo_J_int'] = pd.to_numeric(df_ajudantes['Codigo_J'], errors='coerce').fillna(0).astype(int)
        df_ajudantes['Data_J_dt'] = pd.to_datetime(df_ajudantes['Data_J'], errors='coerce').dt.date
        for _, row in df_ajudantes.iterrows():
            cod = row['Codigo_J_int']
            if cod == 0: continue
            dias = (hoje - row['Data_J_dt']).days if pd.notna(row['Data_J_dt']) else 0
            ajudante_antiguidade_map[cod] = dias
            ajudante_info_map[cod] = {
                "nome": str(row.get('Nome_J', '')).strip(), 
                "cpf": str(row.get('CPF_J', '')).strip()
            }

    # --- 3. MAPEAMENTO DE VOLUMES (CAIXAS POR MAPA) ---
    mapa_caixas_total = {}
    if df_caixas is not None and not df_caixas.empty:
        df_caixas_limpo = df_caixas.drop_duplicates(subset=['mapa'])
        mapa_caixas_total = df_caixas_limpo.set_index('mapa')['caixas'].to_dict()

    # --- 4. ACUMULAÇÃO SEGREGADA ---
    motorista_caixas_acumuladas = {}
    ajudante_caixas_acumuladas = {}
    
    colunas_ajudantes = [col for col in df_viagens.columns if col.startswith('CODJ_')]

    if df_viagens is not None:
        for _, viagem in df_viagens.iterrows():
            mapa_id = str(viagem.get('MAPA', ''))
            caixas_do_mapa = float(mapa_caixas_total.get(mapa_id, 0))
            if caixas_do_mapa == 0: continue
            
            # Acumula para Motorista Principal
            cod_motorista = int(viagem.get('COD', 0))
            if cod_motorista in motorista_info_map:
                motorista_caixas_acumuladas[cod_motorista] = motorista_caixas_acumuladas.get(cod_motorista, 0) + caixas_do_mapa
            
            # Acumula para Motorista 2 (se houver)
            cod_motorista_2 = pd.to_numeric(viagem.get('COD_2'), errors='coerce')
            if pd.notna(cod_motorista_2):
                cod_m2 = int(cod_motorista_2)
                if cod_m2 in motorista_info_map:
                    motorista_caixas_acumuladas[cod_m2] = motorista_caixas_acumuladas.get(cod_m2, 0) + caixas_do_mapa

            # Acumula para Ajudantes
            for col in colunas_ajudantes:
                cod_aj = pd.to_numeric(viagem.get(col), errors='coerce')
                if cod_aj and pd.notna(cod_aj):
                    aj_int = int(cod_aj)
                    if aj_int in ajudante_info_map:
                        ajudante_caixas_acumuladas[aj_int] = ajudante_caixas_acumuladas.get(aj_int, 0) + caixas_do_mapa

    # --- 5. RESULTADO FINAL PARA MOTORISTAS ---
    resultado_motoristas = []
    for cod, total in motorista_caixas_acumuladas.items():
        info = motorista_info_map.get(cod, {"cpf": "N/A", "nome": f"COD {cod}"})
        dias = motorista_antiguidade_map.get(cod, 0)
        valor = _get_valor_por_caixa(dias, metas_motorista)
        resultado_motoristas.append({
            "cpf": info["cpf"], "cod": cod, "nome": info["nome"],
            "total_caixas": total, "valor_por_caixa": valor, "total_premio": total * valor,
            "antiguidade_dias": dias
        })

    # --- 6. RESULTADO FINAL PARA AJUDANTES ---
    resultado_ajudantes = []
    for cod, total in ajudante_caixas_acumuladas.items():
        info = ajudante_info_map.get(cod, {"cpf": "N/A", "nome": f"COD {cod}"})
        dias = ajudante_antiguidade_map.get(cod, 0)
        valor = _get_valor_por_caixa(dias, metas_ajudante)
        resultado_ajudantes.append({
            "cpf": info["cpf"], "cod": cod, "nome": info["nome"],
            "total_caixas": total, "valor_por_caixa": valor, "total_premio": total * valor,
            "antiguidade_dias": dias
        })

    return sorted(resultado_motoristas, key=lambda x: x['nome']), sorted(resultado_ajudantes, key=lambda x: x['nome'])

@router.get("/")
async def ler_relatorio_caixas(
    request: Request, 
    data_inicio: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="Data no formato YYYY-MM-DD"),
    data_fim: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="Data no formato YYYY-MM-DD"),
    current_user: dict = Depends(get_current_user),
    supabase: Client = Depends(get_supabase)
):
    """Rota da API que retorna o relatório de caixas segregado."""
    metas = await run_in_threadpool(_get_metas_sincrono, supabase)
    df_viagens, err1 = await run_in_threadpool(get_dados_apurados, supabase, data_inicio, data_fim, "")
    df_cadastro, err2 = await run_in_threadpool(get_cadastro_sincrono, supabase)
    df_caixas, err3 = await run_in_threadpool(get_caixas_sincrono, supabase, data_inicio, data_fim)
    
    error = err1 or err2 or err3
    
    motoristas, ajudantes = [], []
    if not error:
        motoristas, ajudantes = await run_in_threadpool(
            processar_caixas_sincrono, df_viagens, df_cadastro, df_caixas, metas
        )

    # Filtro de Segurança por CPF (se não for admin)
    if current_user["role"] != "admin":
        user_cpf = current_user["username"].replace(".", "").replace("-", "")
        motoristas = [m for m in motoristas if str(m['cpf']).replace(".", "").replace("-", "") == user_cpf]
        ajudantes = [a for a in ajudantes if str(a['cpf']).replace(".", "").replace("-", "") == user_cpf]

    return {
        "motoristas": motoristas,
        "ajudantes": ajudantes,
        "error": error
    }