import datetime
import pandas as pd
import io
import traceback
from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool
from supabase import Client
from jose import jwt, JWTError # Necessário para validar o token na exportação

# Importações internas
from core.security import get_current_user, SECRET_KEY, ALGORITHM
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
    
    # --- DEDUPLICAÇÃO DE MAPAS (CRUCIAL) ---
    df_viagens_dedup = None
    if df_viagens is not None:
        if 'MAPA' in df_viagens.columns: 
            df_viagens_dedup = df_viagens.drop_duplicates(subset=['MAPA'])
        else: 
            df_viagens_dedup = df_viagens.drop_duplicates()

    return {
        "metas": metas,
        "df_viagens_bruto": df_viagens,
        "df_viagens_dedup": df_viagens_dedup,
        "df_cadastro": df_cadastro,
        "df_indicadores": df_indicadores,
        "df_caixas": df_caixas,
        "error_message": err1 or err2 or err3 or err4
    }

def _merge_resultados(m_kpi, a_kpi, m_cx, a_cx):
    cols_kpi = ['cod', 'nome', 'cpf', 'total_premio']
    cols_cx = ['cod', 'nome', 'cpf', 'total_premio']

    # Converte para DataFrame e renomeia coluna de valor para evitar colisão
    df_m_kpi = pd.DataFrame(m_kpi).reindex(columns=cols_kpi).rename(columns={"total_premio": "premio_kpi"}) if m_kpi else pd.DataFrame(columns=cols_kpi)
    df_a_kpi = pd.DataFrame(a_kpi).reindex(columns=cols_kpi).rename(columns={"total_premio": "premio_kpi"}) if a_kpi else pd.DataFrame(columns=cols_kpi)
    df_m_cx = pd.DataFrame(m_cx).reindex(columns=cols_cx).rename(columns={"total_premio": "premio_caixas"}) if m_cx else pd.DataFrame(columns=cols_cx)
    df_a_cx = pd.DataFrame(a_cx).reindex(columns=cols_cx).rename(columns={"total_premio": "premio_caixas"}) if a_cx else pd.DataFrame(columns=cols_cx)

    # Garante 0.0 onde for nulo
    if 'premio_kpi' in df_m_kpi.columns: df_m_kpi['premio_kpi'] = df_m_kpi['premio_kpi'].fillna(0)
    if 'premio_caixas' in df_m_cx.columns: df_m_cx['premio_caixas'] = df_m_cx['premio_caixas'].fillna(0)

    # Garante tipo numérico para o COD (chave de merge)
    for df in [df_m_kpi, df_m_cx, df_a_kpi, df_a_cx]:
        if not df.empty and 'cod' in df.columns:
            df['cod'] = pd.to_numeric(df['cod'], errors='coerce').fillna(0).astype(int)

    # Merge Outer (mantém quem tem só KPI ou só Caixa)
    df_m = pd.merge(df_m_kpi, df_m_cx, on='cod', how='outer', suffixes=('_kpi', '_cx'))
    df_a = pd.merge(df_a_kpi, df_a_cx, on='cod', how='outer', suffixes=('_kpi', '_cx'))
    
    for df in [df_m, df_a]:
        if 'premio_kpi' not in df.columns: df['premio_kpi'] = 0.0
        if 'premio_caixas' not in df.columns: df['premio_caixas'] = 0.0
        
        df['premio_kpi'] = df['premio_kpi'].fillna(0)
        df['premio_caixas'] = df['premio_caixas'].fillna(0)
        df['total_a_pagar'] = df['premio_kpi'] + df['premio_caixas']
        
        # Consolida Nome e CPF (pega do lado que tiver informação)
        df['nome'] = df.apply(lambda row: row.get('nome_kpi') if pd.notna(row.get('nome_kpi')) else row.get('nome_cx', ''), axis=1)
        df['cpf'] = df.apply(lambda row: row.get('cpf_kpi') if pd.notna(row.get('cpf_kpi')) else row.get('cpf_cx', ''), axis=1)
        
        # --- CORREÇÃO DO BUG DE COLUNAS ---
        # Remove apenas as colunas sufixadas criadas pelo merge, preservando 'premio_kpi'
        cols_to_drop = ['nome_kpi', 'nome_cx', 'cpf_kpi', 'cpf_cx']
        df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

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

        m_kpi, a_kpi = await run_in_threadpool(
            processar_incentivos_sincrono,
            dados["df_viagens_dedup"], dados["df_cadastro"], 
            dados["df_indicadores"], None, dados["metas"]
        )
        
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
        return {"motoristas": [], "ajudantes": [], "error": "Erro interno no servidor."}

@router.get("/pagamento/exportar")
async def exportar_relatorio_pagamento(
    data_inicio: str,
    data_fim: str,
    token: str, 
    supabase: Client = Depends(get_supabase)
):
    # 1. Validar Token manualmente (pois vem via URL)
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if username is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido")
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expirado ou inválido")

    try:
        # 2. Obter Dados (mesma lógica da visualização)
        dados = await _get_dados_completos(data_inicio, data_fim, supabase)
        
        m_kpi, a_kpi = await run_in_threadpool(
            processar_incentivos_sincrono,
            dados["df_viagens_dedup"], dados["df_cadastro"], 
            dados["df_indicadores"], None, dados["metas"]
        )
        
        m_cx, a_cx = await run_in_threadpool(
            processar_caixas_sincrono,
            dados["df_viagens_dedup"], dados["df_cadastro"], 
            dados["df_caixas"], dados["metas"]
        )
        
        df_m, df_a = await run_in_threadpool(_merge_resultados, m_kpi, a_kpi, m_cx, a_cx)
        
        # 3. Aplicar Filtro de Segurança
        if role != "admin":
            cpf_user = username.replace(".", "").replace("-", "")
            if not df_m.empty:
                df_m = df_m[df_m['cpf'].astype(str).str.replace(r'[.-]', '', regex=True) == cpf_user]
            if not df_a.empty:
                df_a = df_a[df_a['cpf'].astype(str).str.replace(r'[.-]', '', regex=True) == cpf_user]

        # 4. Formatação para Excel
        mapa_colunas = {
            "cod": "CÓDIGO",
            "nome": "NOME",
            "cpf": "CPF",
            "premio_kpi": "PRÊMIO KPI (R$)",
            "premio_caixas": "PRÊMIO CAIXAS (R$)",
            "total_a_pagar": "TOTAL A PAGAR (R$)"
        }
        
        colunas_finais = ["cod", "nome", "cpf", "premio_caixas", "premio_kpi", "total_a_pagar"]
        
        # Seleciona e renomeia colunas, garantindo que o DF não esteja vazio para evitar erro
        if not df_m.empty:
            df_m_final = df_m[colunas_finais].rename(columns=mapa_colunas)
        else:
            df_m_final = pd.DataFrame(columns=mapa_colunas.values())

        if not df_a.empty:
            df_a_final = df_a[colunas_finais].rename(columns=mapa_colunas)
        else:
            df_a_final = pd.DataFrame(columns=mapa_colunas.values())

        # 5. Gerar Arquivo Excel na memória
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_m_final.to_excel(writer, sheet_name='Motoristas', index=False)
            df_a_final.to_excel(writer, sheet_name='Ajudantes', index=False)
            
            # Ajuste cosmético de largura das colunas
            for sheet in writer.sheets.values():
                for column in sheet.columns:
                    try:
                        max_length = max(len(str(cell.value or "")) for cell in column)
                        adjusted_width = (max_length + 2)
                        sheet.column_dimensions[column[0].column_letter].width = adjusted_width
                    except:
                        pass

        output.seek(0)
        
        headers = {
            'Content-Disposition': f'attachment; filename="Pagamento_{data_inicio}_{data_fim}.xlsx"'
        }
        return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except Exception as e:
        print(f"Erro na exportação Excel: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Erro interno ao gerar o arquivo Excel.")