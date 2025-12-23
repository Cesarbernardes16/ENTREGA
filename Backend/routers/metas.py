from fastapi import APIRouter, Request, Depends, Body, HTTPException, status
from core.security import get_current_user
from fastapi.concurrency import run_in_threadpool
from supabase import Client
import traceback

router = APIRouter(prefix="/metas", tags=["Metas"])

# --- DEFINIÇÃO DOS VALORES PADRÃO ---
DEFAULTS = {
    "dev_pdv_meta_perc": 0.0, 
    "dev_pdv_premio": 0.0,
    "rating_meta_perc": 0.0, 
    "rating_premio": 0.0,
    "refugo_meta_perc": 0.0, 
    "refugo_premio": 0.0,
    "meta_cx_dias_n1": 365, 
    "meta_cx_valor_n1": 0.0,
    "meta_cx_dias_n2": 730, 
    "meta_cx_valor_n2": 0.0,
    "meta_cx_dias_n3": 1825, 
    "meta_cx_valor_n3": 0.0, 
    "meta_cx_valor_n4": 0.0
}

def _get_metas_sincrono(supabase: Client) -> dict:
    """
    Busca as metas no Supabase (Tabela 'Metas').
    Adapta a leitura para a estrutura baseada em linhas (tipo_colaborador).
    """
    # Estrutura inicial de resposta
    result = {
        "motorista": DEFAULTS.copy(),
        "ajudante": DEFAULTS.copy()
    }

    try:
        # Busca todas as linhas da tabela
        response = supabase.table("Metas").select("*").execute()
        
        if response.data:
            for row in response.data:
                tipo = row.get("tipo_colaborador")
                # Se o tipo for válido (motorista ou ajudante), preenche os dados
                if tipo in result:
                    for key in result[tipo].keys():
                        # Se o valor existir no banco, atualiza. Senão, mantém o padrão.
                        if key in row and row[key] is not None:
                            try:
                                result[tipo][key] = float(row[key])
                            except:
                                result[tipo][key] = row[key]
        
        return result

    except Exception as e:
        print(f"Erro ao buscar metas (usando padrão): {e}")
        return result

# --- ROTAS ---

@router.get("/")
async def get_metas_json(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    return await run_in_threadpool(_get_metas_sincrono, request.state.supabase)

@router.post("/")
async def update_metas_json(
    request: Request,
    metas_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    # Segurança: Apenas admin pode salvar
    if current_user["role"] != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Acesso negado. Apenas Gestores podem alterar as metas.")
    
    supabase = request.state.supabase
    
    try:
        # Processa cada categoria (motorista/ajudante) separadamente
        for categoria, dados in metas_data.items():
            if categoria not in ["motorista", "ajudante"]:
                continue
            
            # Prepara o objeto para salvar
            # Filtramos apenas as chaves que esperamos para evitar erro de coluna inexistente
            record_to_save = {"tipo_colaborador": categoria}
            
            for key in DEFAULTS.keys():
                if key in dados:
                    record_to_save[key] = dados[key]
            
            # Realiza o UPSERT (Insere se não existe, Atualiza se existe)
            # A chave primária 'tipo_colaborador' garante que não duplique
            supabase.table("Metas").upsert(record_to_save).execute()
            
    except Exception as e:
        print(f"\n--- ERRO CRÍTICO AO SALVAR METAS ---\n")
        print(traceback.format_exc())
        print(f"\n--- FIM DO ERRO ---\n")

        if "permission denied" in str(e):
             raise HTTPException(status_code=500, detail="Erro de permissão no Supabase.")
        
        raise HTTPException(status_code=500, detail=f"Erro ao salvar metas: {str(e)}")
    
    return {"message": "Metas atualizadas com sucesso"}