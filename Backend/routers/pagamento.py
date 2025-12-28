import datetime
import pandas as pd
import io
import traceback
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool
from supabase import Client
from core.security import get_current_user

from core.database import get_dados_apurados, get_cadastro_sincrono, get_caixas_sincrono, get_indicadores_sincrono
from .incentivo import processar_incentivos_sincrono
from .caixas import processar_caixas_sincrono
from .metas import _get_metas_sincrono

router = APIRouter(tags=["Pagamento"])

def get_supabase(request: Request) -> Client:
    return request.state.supabase

async def _get_dados_completos(data_inicio: str, data_fim: str, supabase: Client) -> Dict[str, Any]:
    try:
        user_date_obj = datetime.date.fromisoformat(data_inicio)
        dia_corte = 26
        if user_date_obj.day < dia_corte:
            d_fim_p = user_date_obj.replace(day=25)
            d_ini_p = (user_date_obj.replace(day=1) - datetime.timedelta(days=1)).replace(day=dia_corte)
        else:
            d_ini_p = user_date_obj.replace(day=dia_corte)
            d_fim_p = (d_ini_p + datetime.timedelta(days=32)).replace(day=25)
        d_ini_str, d_fim_str = d_ini_p.isoformat(), d_fim_p.isoformat()
    except ValueError:
        d_ini_str, d_fim_str = data_inicio, data_fim

    metas = await run_in_threadpool(_get_metas_sincrono, supabase)
    df_viagens, err1 = await run_in_threadpool(get_dados_apurados, supabase, data_inicio, data_fim, "")
    df_cadastro, err2 = await run_in_threadpool(get_cadastro_sincrono, supabase)
    df_indicadores, err3 = await run_in_threadpool(get_indicadores_sincrono, supabase, d_ini_str, d_fim_str)
    df_caixas, err4 = await run_in_threadpool(get_caixas_sincrono, supabase, data_inicio, data_fim)
    
    # --- DEDUPLICAÇÃO DE MAPAS ---
    # Cria uma versão sem duplicatas de MAPA para garantir cálculos corretos de pagamento
    df_viagens_dedup = None
    if df_viagens is not None:
        if 'MAPA' in df_viagens.columns: 
            df_viagens_dedup = df_viagens.drop_duplicates(subset=['MAPA'])
        else: 
            df_viagens_dedup = df_viagens.drop_duplicates()

    return {
        "metas": metas,
        "df_viagens_bruto": df_viagens,
        "df_viagens_dedup": df_viagens_dedup, # Usaremos este para KPIs e Caixas
        "df_cadastro": df_cadastro,
        "df_indicadores": df_indicadores,
        "df_caixas": df_caixas,
        "error_message": err1 or err2 or err3 or err4
    }

def _merge_resultados(m_kpi, a_kpi, m_cx, a_cx):
    # --- CORREÇÃO: Incluir nome e cpf também nas colunas de Caixas ---
    cols_kpi = ['cod', 'nome', 'cpf', 'total_premio']
    cols_cx = ['cod', 'nome', 'cpf', 'total_premio']

    if m_kpi:
        df_m_kpi = pd.DataFrame(m_kpi).reindex(columns=cols_kpi).rename(columns={"total_premio": "premio_kpi"})
    else:
        df_m_kpi = pd.DataFrame(columns=cols_kpi)

    if a_kpi:
        df_a_kpi = pd.DataFrame(a_kpi).reindex(columns=cols_kpi).rename(columns={"total_premio": "premio_kpi"})
    else:
        df_a_kpi = pd.DataFrame(columns=cols_kpi)
    
    if m_cx:
        df_m_cx = pd.DataFrame(m_cx).reindex(columns=cols_cx).rename(columns={"total_premio": "premio_caixas"})
    else:
        df_m_cx = pd.DataFrame(columns=cols_cx)

    if a_cx:
        df_a_cx = pd.DataFrame(a_cx).reindex(columns=cols_cx).rename(columns={"total_premio": "premio_caixas"})
    else:
        df_a_cx = pd.DataFrame(columns=cols_cx)

    # Garantir tipos numéricos e preencher NAs antes do merge
    if 'premio_kpi' in df_m_kpi.columns: df_m_kpi['premio_kpi'] = df_m_kpi['premio_kpi'].fillna(0)
    if 'premio_caixas' in df_m_cx.columns: df_m_cx['premio_caixas'] = df_m_cx['premio_caixas'].fillna(0)

    if not df_m_kpi.empty: df_m_kpi['cod'] = pd.to_numeric(df_m_kpi['cod'], errors='coerce').fillna(0).astype(int)
    if not df_m_cx.empty: df_m_cx['cod'] = pd.to_numeric(df_m_cx['cod'], errors='coerce').fillna(0).astype(int)
    if not df_a_kpi.empty: df_a_kpi['cod'] = pd.to_numeric(df_a_kpi['cod'], errors='coerce').fillna(0).astype(int)
    if not df_a_cx.empty: df_a_cx['cod'] = pd.to_numeric(df_a_cx['cod'], errors='coerce').fillna(0).astype(int)

    # Merge Outer para pegar quem tem só KPI ou só Caixas
    # Suffixes garantem que nome_kpi e nome_cx não colidam
    df_m = pd.merge(df_m_kpi, df_m_cx, on='cod', how='outer', suffixes=('_kpi', '_cx'))
    df_a = pd.merge(df_a_kpi, df_a_cx, on='cod', how='outer', suffixes=('_kpi', '_cx'))
    
    for df in [df_m, df_a]:
        if 'premio_kpi' not in df.columns: df['premio_kpi'] = 0
        if 'premio_caixas' not in df.columns: df['premio_caixas'] = 0
        
        df['premio_kpi'] = df['premio_kpi'].fillna(0)
        df['premio_caixas'] = df['premio_caixas'].fillna(0)
        df['total_a_pagar'] = df['premio_kpi'] + df['premio_caixas']
        
        # Consolida colunas de informação (Nome e CPF)
        # Prioriza KPI, mas se não tiver (ex: só ganhou caixa), pega de Caixas
        df['nome'] = df.apply(lambda row: row.get('nome_kpi') if pd.notna(row.get('nome_kpi')) else row.get('nome_cx', ''), axis=1)
        df['cpf'] = df.apply(lambda row: row.get('cpf_kpi') if pd.notna(row.get('cpf_kpi')) else row.get('cpf_cx', ''), axis=1)
        
        # Remove colunas intermediárias para limpar o JSON final
        cols_to_drop = [col for col in df.columns if '_kpi' in col or '_cx' in col]
        df.drop(columns=cols_to_drop, errors='ignore', inplace=True)

        df.fillna('', inplace=True)

    return df_m, df_a

@router.get("/pagamento")
async def ler_relatorio_pagamento(
    request: Request, 
    data_inicio: str,
    data_fim: str,
    current_user: dict = Depends(get_current_user),
    supabase: Client = Depends(get_supabase)
):
    try:
        dados = await _get_dados_completos(data_inicio, data_fim, supabase)
        
        if dados["error_message"]:
             return {"motoristas": [], "ajudantes": [], "error": dados["error_message"]}

        # Processa KPIs (já usava dedup)
        m_kpi, a_kpi = await run_in_threadpool(
            processar_incentivos_sincrono,
            dados["df_viagens_dedup"], dados["df_cadastro"], 
            dados["df_indicadores"], None, dados["metas"]
        )
        
        # --- CORREÇÃO IMPORTANTE AQUI ---
        # Agora usamos 'df_viagens_dedup' em vez de 'df_viagens_bruto' também para Caixas.
        # Isso evita que o mesmo mapa seja somado 2x se houver duplicidade no banco.
        m_cx, a_cx = await run_in_threadpool(
            processar_caixas_sincrono,
            dados["df_viagens_dedup"], dados["df_cadastro"], 
            dados["df_caixas"], dados["metas"]
        )
        
        df_m, df_a = await run_in_threadpool(_merge_resultados, m_kpi, a_kpi, m_cx, a_cx)
        
        # Filtro de Segurança
        if current_user["role"] != "admin":
            cpf_user = current_user["username"].replace(".", "").replace("-", "")
            if not df_m.empty:
                df_m = df_m[df_m['cpf'].astype(str).str.replace(r'[.-]', '', regex=True) == cpf_user]
            if not df_a.empty:
                df_a = df_a[df_a['cpf'].astype(str).str.replace(r'[.-]', '', regex=True) == cpf_user]

        return {
            "motoristas": df_m.to_dict('records'),
            "ajudantes": df_a.to_dict('records'),
            "error": None
        }
    except Exception as e:
        print(f"\n--- ERRO CRÍTICO EM /PAGAMENTO ---\n")
        print(traceback.format_exc())
        print(f"\n--- FIM DO ERRO ---\n")
        return {"motoristas": [], "ajudantes": [], "error": f"Erro interno no servidor. Consulte os logs do Backend para o traceback."}

@router.get("/pagamento/exportar")
async def exportar_relatorio_pagamento(
    data_inicio: str,
    data_fim: str,
    token: str, 
    supabase: Client = Depends(get_supabase)
):
    # Nota: A rota de exportação deve implementar a mesma lógica de deduplicação acima
    # para garantir que o Excel bata com a tela.
    # O código abaixo é um esqueleto, certifique-se de copiar a lógica do 'ler_relatorio_pagamento'.
    try:
        dados = await _get_dados_completos(data_inicio, data_fim, supabase)
        
        m_kpi, a_kpi = await run_in_threadpool(
            processar_incentivos_sincrono,
            dados["df_viagens_dedup"], dados["df_cadastro"], 
            dados["df_indicadores"], None, dados["metas"]
        )
        
        # CORREÇÃO AQUI TAMBÉM
        m_cx, a_cx = await run_in_threadpool(
            processar_caixas_sincrono,
            dados["df_viagens_dedup"], dados["df_cadastro"], 
            dados["df_caixas"], dados["metas"]
        )
        
        df_m, df_a = await run_in_threadpool(_merge_resultados, m_kpi, a_kpi, m_cx, a_cx)
        
        # Gerar Excel (simplificado para o exemplo, use sua lógica de exportação existente)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_m.to_excel(writer, sheet_name='Motoristas', index=False)
            df_a.to_excel(writer, sheet_name='Ajudantes', index=False)
        output.seek(0)
        
        headers = {
            'Content-Disposition': f'attachment; filename="Pagamento_{data_inicio}_{data_fim}.xlsx"'
        }
        return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except Exception as e:
        print(f"Erro na exportação: {e}")
        raise HTTPException(status_code=500, detail="Erro ao gerar Excel")