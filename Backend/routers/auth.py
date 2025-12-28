from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordRequestForm
from supabase import Client
from core.database import get_cadastro_sincrono, get_supabase
from core.security import create_access_token
from datetime import timedelta
import os

router = APIRouter()

# --- SECURITY FIX: Load from Environment Variables ---
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "123") 

# CORREÇÃO: Tipagem explicita para o FastAPI injetar o objeto Request corretamente
def get_supabase(request: Request) -> Client:
    return request.state.supabase

@router.post("/token")
def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    supabase: Client = Depends(get_supabase)
):
    user_role = "colaborador"
    username = form_data.username
    
    # 1. Verificar se é Admin
    if username == ADMIN_USER and form_data.password == ADMIN_PASS:
        user_role = "admin"
    
    # 2. Se não for Admin, verificar se é CPF (Colaborador)
    else:
        df_cadastro, _ = get_cadastro_sincrono(supabase)
        
        if df_cadastro is None or df_cadastro.empty:
             raise HTTPException(status_code=400, detail="Erro ao aceder ao cadastro.")

        cpf_limpo = username.replace(".", "").replace("-", "")
        
        # Verificação segura se as colunas existem
        existe_motorista = False
        existe_ajudante = False

        if 'CPF_M' in df_cadastro.columns:
            existe_motorista = df_cadastro['CPF_M'].astype(str).str.replace(r'[.-]', '', regex=True).eq(cpf_limpo).any()
        if 'CPF_J' in df_cadastro.columns:
            existe_ajudante = df_cadastro['CPF_J'].astype(str).str.replace(r'[.-]', '', regex=True).eq(cpf_limpo).any()

        if not (existe_motorista or existe_ajudante):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="CPF não encontrado ou senha incorreta.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Se for colaborador, a senha é o próprio CPF (login simplificado)
        if form_data.password != username:
             raise HTTPException(status_code=401, detail="Para colaboradores, a senha é o CPF.")

    # 3. Gerar Token
    access_token_expires = timedelta(minutes=60 * 12)
    access_token = create_access_token(
        data={"sub": username, "role": user_role}, 
        expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer", "role": user_role}