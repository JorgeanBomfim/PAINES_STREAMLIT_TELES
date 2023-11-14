import streamlit as st
import pandas as pd 
import numpy as np
import pymysql
import base64

# Define o estilo padrão do Streamlit como dark
st.set_page_config(page_title="SMS - AV CLARO NEG", layout="wide", page_icon=":bar_chart:")
css = """
body {
    background-color: #1a1a1a;
    color: dark;
}
"""
st.write(f'<style>{css}</style>', unsafe_allow_html=True)



## ocultando menu e marca d'agua
hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

# Criando uma função para gerar uma imagem de fundo.
def add_bg_from_url():
    st.markdown(
         f"""
         <style>
         .stApp {{
             background-image: url("https://files.tecnoblog.net/wp-content/uploads/2022/04/claro-capa-tecnoblog-virtual-1060x596.png");
             background-attachment: fixed;
             background-size: cover
         }}
         </style>
         """,
         unsafe_allow_html=True
     )
add_bg_from_url() 

# st.markdown(
#     "<h1 style='text-align: center; color: white;text-shadow: 3px 3px black; font-weight: bold;'>SMS DIÁRIO CLARO</h1>", 
#     unsafe_allow_html=True
# )

# Criar abas para "Acordos Claro" e "Acordos Gevenue"
page = st.sidebar.selectbox("Selecione a página:", ["Acordos Claro", "Acordos Gevenue",'Acordos TIMCOB-DIA E NOITE'])

if page == "Acordos Claro":

    col_1,col_2,col_3 = st.columns(3)
    with col_1:
        pass
    with col_2:
        # Conteúdo para a página "Acordos Claro"
        st.write("<h2 style='text-align: left; color: white;text-shadow: 2px 2px black;'>Acordos Claro</h2>", 
                unsafe_allow_html=True)

        # Selecione o intervalo de datas
        start_date = st.date_input('Selecione a data inicial (Claro)')
        end_date = st.date_input('Selecione a data final (Claro)')

        if st.button('Gerar arquivo CSV (Claro)'):
            # Define a consulta SQL para Claro
            table_name = f"tb_infoagent_{start_date.year}_{str(start_date.month).zfill(2)}"
            query = f"""

                SELECT t.dataInicio,
                t.callid,
                t.dddDiscado,
                t.foneDiscado,
                t.cpf,
                t.linhaDigitavel,
                t.nome,
                t.valorTicket,
                t.tipoNegociacao,
                t.valorParcela,
                t.qtParcela,
                t.valorEntrada,
                t.dataVencimento_acordo,
                t.statusProduto AS codigo_da_tabulacao,
                case
                when t.statusProduto = 20 then 'ACORDO AVISTA'
                when t.statusProduto = 21 then 'ACORDO AVISTA - GRAVADO COM SUCESSO'
                when t.statusProduto = 23 then 'ACORDO PARCELADO'
                when t.statusProduto = 24 then 'ACORDO PARCELADO - GRAVADO COM SUCESSO'
                when t.statusProduto IN (44,43,45) then 'ACORDO - FALHA AO EMITIR BOLETO - INTERSIC'
                ELSE
                t.statusProduto
                END AS descricao,
                t.cod_grupo
                FROM tb_infoagent_2023_10 t
                WHERE  t.dataInicio>= '{start_date} 08:00:00'
                AND  t.dataInicio<= '{end_date} 23:00:00'
                AND t.statusProduto IN (20,21,23,24,44,43,45,47)
                AND tiponegociacao IN ('av', 'ap')
                AND cod_grupo IN ('6186')

            """

            # Estabelece a conexão com o banco de dados
            connection = pymysql.connect(host="10.0.6.2",
                user="planejamento",
                password="@Planejamento!123",
                database="atn"
            )

            # Use a função read_sql() do Pandas para executar a consulta e armazenar
            df_sms = pd.read_sql(query, connection)
            connection.close()

            # ...

            # Gerar o CSV e codificar em base64
            csv = df_sms.to_csv(index=False, sep=';').encode('utf-8')
            b64 = base64.b64encode(csv).decode('utf-8')
            href = f'<a href="data:text/csv;base64,{b64}" download="sms_agv_claro_{start_date}_to_{end_date}.csv">Download do arquivo CSV</a>'
            st.markdown(href, unsafe_allow_html=True)
    with col_3:
        pass
elif page == "Acordos Gevenue":

    col_1,col_2,col_3 = st.columns(3)
    with col_1:
        pass
    with col_2:
        
        # Conteúdo para a página "Acordos Gevenue"
        st.write("<h2 style='text-align: left; color: white;text-shadow: 2px 2px black;'>Acordos Gevenue</h2>", 
                unsafe_allow_html=True)

        # Selecione o intervalo de datas
        start_date = st.date_input('Selecione a data inicial (Gevenue)')
        end_date = st.date_input('Selecione a data final (Gevenue)')

        if st.button('Gerar arquivo CSV (Gevenue)'):
            # Define a consulta SQL para Gevenue
            table_name = f"tb_infoagent_{start_date.year}_{str(start_date.month).zfill(2)}"

            query = f"""
                SELECT t.dataInicio,
                t.callid,
                t.dddDiscado,
                t.foneDiscado,
                t.cpf,
                t.linhaDigitavel,
                t.nome,
                t.valorTicket,
                t.tipoNegociacao,
                t.valorParcela,
                t.qtParcela,
                t.valorEntrada,
                t.dataVencimento_acordo,
                t.statusProduto AS codigo_da_tabulacao,
                case
                when t.statusProduto = 20 then 'ACORDO AVISTA'
                when t.statusProduto = 21 then 'ACORDO AVISTA - GRAVADO COM SUCESSO'
                when t.statusProduto = 23 then 'ACORDO PARCELADO'
                when t.statusProduto = 24 then 'ACORDO PARCELADO - GRAVADO COM SUCESSO'
                when t.statusProduto IN (44,43,45) then 'ACORDO - FALHA AO EMITIR BOLETO'
                ELSE
                t.statusProduto
                END AS descricao,
                t.cod_grupo
                FROM tb_infoagent_2023_10 t
                WHERE  t.dataInicio>= '{start_date} 08:00:00'
                AND  t.dataInicio<= '{end_date} 23:00:00'
                AND t.statusProduto IN (20,21,23,24,44,43,45,47)
                AND tiponegociacao IN ('av', 'ap')
                AND cod_grupo IN ('6190')

            """

            # Estabelece a conexão com o banco de dados
            connection = pymysql.connect(host="10.0.6.2",
                user="planejamento",
                password="@Planejamento!123",
                database="atn"
            )

            # Use a função read_sql() do Pandas para executar a consulta e armazenar
            df_sms_gevenue = pd.read_sql(query, connection)
            connection.close()

            # ...

            # Gerar o CSV e codificar em base64
            csv = df_sms_gevenue.to_csv(index=False, sep=';').encode('utf-8')
            b64 = base64.b64encode(csv).decode('utf-8')
            href = f'<a href="data:text/csv;base64,{b64}" download="acordos_gevenue_{start_date}_to_{end_date}.csv">Download do arquivo CSV (Gevenue)</a>'
            st.markdown(href, unsafe_allow_html=True)
    with col_3:
        pass

elif page == "Acordos TIMCOB-DIA E NOITE":

        # Criando uma função para gerar uma imagem de fundo.
    def add_bg_from_url():
        st.markdown(
            f"""
            <style>
            .stApp {{
                background-image: url("https://cancelarplano.com.br/wp-content/uploads/2023/04/cancelar-linha-tim.jpg");
                background-attachment: fixed;
                background-size: cover
            }}
            </style>
            """,
            unsafe_allow_html=True
        )
    add_bg_from_url() 

    col_1,col_2,col_3 = st.columns(3)
    with col_1:
        pass
    with col_2:
        
        # Conteúdo para a página "Acordos Gevenue"
        st.write("<h2 style='text-align: left; color: white;text-shadow: 2px 2px black;'>Acordos TIM DIA E NOITE</h2>", 
                unsafe_allow_html=True)

        # Selecione o intervalo de datas
        start_date = st.date_input('Selecione a data inicial (TIM COB)')
        end_date = st.date_input('Selecione a data final (TIM COB)')

        if st.button('Gerar arquivo CSV (TIM COB)'):
            # Define a consulta SQL para Gevenue
            table_name = f"tb_infoagent_{start_date.year}_{str(start_date.month).zfill(2)}"

            query = f"""
                SELECT t.dataInicio,
                t.callid,
                t.dddDiscado,
                t.foneDiscado,
                t.cpf,
                t.linhaDigitavel,
                t.nome,
                t.valorTicket,
                t.tipoNegociacao,
                t.valorParcela,
                t.qtParcela,
                t.valorEntrada,
                t.dataVencimento_acordo,
                t.statusProduto AS codigo_da_tabulacao,
                case
                when t.statusProduto = 20 then 'ACORDO AVISTA'
                when t.statusProduto = 21 then 'ACORDO AVISTA - GRAVADO COM SUCESSO'
                when t.statusProduto = 23 then 'ACORDO PARCELADO'
                when t.statusProduto = 24 then 'ACORDO PARCELADO - GRAVADO COM SUCESSO'
                when t.statusProduto IN (44,43,45) then 'ACORDO - FALHA AO EMITIR BOLETO'
                ELSE
                t.statusProduto
                END AS descricao,
                t.cod_grupo
                FROM tb_infoagent_2023_11 t
                WHERE  t.dataInicio>= '{start_date} 08:00:00'
                AND  t.dataInicio<= '{end_date} 23:00:00'
                AND t.statusProduto IN (20,21,23,24,44,43,45,47)
                AND tiponegociacao IN ('av', 'ap')
                AND cod_grupo IN ('6333','6417')

            """

            # Estabelece a conexão com o banco de dados
            connection = pymysql.connect(host="10.0.6.2",
                user="planejamento",
                password="@Planejamento!123",
                database="atn"
            )

            # Use a função read_sql() do Pandas para executar a consulta e armazenar
            df_sms_tim_noite = pd.read_sql(query, connection)
            connection.close()

            # ...

            # Gerar o CSV e codificar em base64
            csv = df_sms_tim_noite.to_csv(index=False, sep=';').encode('utf-8')
            b64 = base64.b64encode(csv).decode('utf-8')
            href = f'<a href="data:text/csv;base64,{b64}" download="acordos_TIM_{start_date}_to_{end_date}.csv">Download do arquivo CSV (TIM NOITE)</a>'
            st.markdown(href, unsafe_allow_html=True)
    with col_3:
        pass