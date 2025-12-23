import os
import pandas as pd
from supabase import Client
from typing import Optional, Tuple
from .analysis import limpar_texto # Importa da mesma pasta 'core'
import functools
from fastapi import Request

NOME_DA_TABELA = "Distribuição"
NOME_COLUNA_DATA = "DATA"

# Cache para dados frequentemente acessados
cache = {}

def get_supabase(request: Request) -> Client:
    """Centralized function to retrieve the Supabase client from the request."""
    return request.state.supabase

# --- FUNÇÃO 1 (Existente) ---
@functools.lru_cache(maxsize=128)
def get_dados_apurados(
    supabase: Client, 
    data_inicio_str: str, 
    data_fim_str: str, 
    search_str: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca dados do Supabase (Distribuição), limpa e filtra.
    Retorna o DataFrame ou (None, error_message).
    """
    df = pd.DataFrame()
    error_message = None
    
    try:
        dados_completos = []
        page_size = 1000
        page = 0
        while True:
            # Assumindo que a tabela 'Distribuição' está no schema 'public'
            query = (
                supabase.table(NOME_DA_TABELA)
                .select("*")
                .gte(NOME_COLUNA_DATA, data_inicio_str)
                .lte(NOME_COLUNA_DATA, data_fim_str)
                .range(page * page_size, (page + 1) * page_size - 1)
            )
            response = query.execute() 
            
            if not response.data: break
            dados_completos.extend(response.data)
            page += 1
            if len(response.data) < page_size: break
        
        if not dados_completos:
            return None, "Nenhum dado encontrado para o período selecionado."
        
        df = pd.DataFrame(dados_completos)

    except Exception as e:
        print(f"Erro ao buscar dados do Supabase (Distribuição): {e}")
        if "permission denied" in str(e):
             return None, "Erro de permissão. Execute 'GRANT ALL ON TABLE public.\"Distribuição\" TO service_role;' no Supabase."
        return None, "Erro ao conectar à tabela 'Distribuição'."

    # Limpeza de Texto
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].apply(limpar_texto)
    
    if 'COD' in df.columns:
        df['COD'] = pd.to_numeric(df['COD'], errors='coerce')
        df.dropna(subset=['COD'], inplace=True)
        df['COD'] = df['COD'].astype(int)
    else:
         return None, "A coluna 'COD' principal não foi encontrada."

    # Filtro de Pesquisa
    if search_str:
        search_clean = limpar_texto(search_str)
        colunas_busca = ['MOTORISTA', 'MOTORISTA_2', 'AJUDANTE_1', 'AJUDANTE_2', 'AJUDANTE_3']
        colunas_existentes_busca = [col for col in colunas_busca if col in df.columns]
        mask = pd.Series(False, index=df.index)
        for col in colunas_existentes_busca:
            mask = mask | df[col].str.contains(search_clean, na=False)
        df = df[mask]
        if df.empty:
            return None, f"Nenhum dado encontrado para o termo de busca: '{search_str}'"

    return df, None

# --- FUNÇÃO 2 (Existente) ---
@functools.lru_cache(maxsize=128)
def get_cadastro_sincrono(supabase: Client) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca todos os dados da tabela de cadastro (public.Cadastro).
    """
    try:
        response = supabase.table("Cadastro").select("*").execute()
        
        if not response.data:
            return None, "Tabela 'Cadastro' está vazia ou não foi encontrada no schema 'public'."
        
        df_cadastro = pd.DataFrame(response.data)
        
        df_cadastro.columns = df_cadastro.columns.str.strip()

        if 'CPF_M' in df_cadastro.columns:
            df_cadastro['CPF_M'] = df_cadastro['CPF_M'].astype(str).str.replace(r'[.-]', '', regex=True).fillna('')
        if 'CPF_J' in df_cadastro.columns:
            df_cadastro['CPF_J'] = df_cadastro['CPF_J'].astype(str).str.replace(r'[.-]', '', regex=True).fillna('')

        return df_cadastro, None

    except Exception as e:
        print(f"Erro ao buscar dados do Cadastro: {e}")
        if "permission denied" in str(e):
            return None, "Erro de permissão. Execute 'GRANT ALL ON TABLE public.\"Cadastro\" TO service_role;' no Supabase."
        if "relation" in str(e) and "does not exist" in str(e):
             return None, "Erro: A tabela 'Cadastro' não existe no schema 'public'."
        return None, "Erro ao conectar à tabela de Cadastro."

# --- FUNÇÃO 3 (Nova) ---
@functools.lru_cache(maxsize=128)
def get_indicadores_sincrono(
    supabase: Client, 
    data_inicio_str: str, 
    data_fim_str: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca os resultados consolidados da tabela 'Resultados_Indicadores'.
    """
    try:
        # Busca os indicadores onde o período de pagamento corresponde
        # exatamente ao período calculado.
        response = (
            supabase.table("Resultados_Indicadores")
            .select("*")
            .eq("data_inicio_periodo", data_inicio_str)
            .eq("data_fim_periodo", data_fim_str)
            .execute()
        )
        
        if not response.data:
            # Não é um erro, apenas não há dados de indicador para este períodoo
            return pd.DataFrame(), None 
        
        df_indicadores = pd.DataFrame(response.data)
        df_indicadores.columns = df_indicadores.columns.str.strip()

        return df_indicadores, None

    except Exception as e:
        print(f"Erro ao buscar dados de Indicadores: {e}")
        if "permission denied" in str(e):
            return None, "Erro de permissão. Execute 'GRANT ALL ON TABLE public.\"Resultados_Indicadores\" TO service_role;' no Supabase."
        if "relation" in str(e) and "does not exist" in str(e):
             return None, "Erro: A tabela 'Resultados_Indicadores' não existe no schema 'public'."
        return None, "Erro ao conectar à tabela de Indicadores."
    # --- NOVA FUNÇÃO 4 (Adicionar esta ao final do ficheiro) ---
@functools.lru_cache(maxsize=128)
def get_caixas_sincrono(
    supabase: Client, 
    data_inicio_str: str, 
    data_fim_str: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca dados de caixas entregues da tabela 'Caixas'.
    Assume que a tabela 'Caixas' tem as colunas 'data', 'mapa', 'caixas'.
    """
    try:
        response = (
            supabase.table("Caixas")
            .select("data, mapa, caixas") # Seleciona as colunas que você criou
            .gte("data", data_inicio_str)
            .lte("data", data_fim_str)
            .execute()
        )
        
        if not response.data:
            # Não é um erro, apenas não há dados de caixas no período
            return pd.DataFrame(columns=["data", "mapa", "caixas"]), None 
        
        df_caixas = pd.DataFrame(response.data)
        
        # Converte tipos para garantir o cálculo correto
        # Assegura que 'mapa' seja tratado como texto/objeto para agrupar
        df_caixas['mapa'] = df_caixas['mapa'].astype(str)
        df_caixas['caixas'] = pd.to_numeric(df_caixas['caixas'], errors='coerce')
        df_caixas.dropna(subset=['mapa', 'caixas'], inplace=True)
        
        df_caixas['caixas'] = df_caixas['caixas'].astype(float) 

        return df_caixas, None

    except Exception as e:
        print(f"Erro ao buscar dados de Caixas: {e}")
        if "relation" in str(e) and "does not exist" in str(e):
             return None, "Erro: A tabela 'Caixas' não existe no schema 'public'."
        return None, "Erro ao conectar à tabela de Caixas."