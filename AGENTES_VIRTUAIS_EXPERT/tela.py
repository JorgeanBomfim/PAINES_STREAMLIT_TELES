# trazendo a base que foi respondido de ura
import pymysql
import pandas as pd 
import numpy as np
import warnings
import datetime as dt
import streamlit as st
warnings.filterwarnings('ignore')
import time

while True: 
    connection = pymysql.connect(
        host='10.0.6.2',
        user='planejamento',
        password='@Planejamento!123',
        database='atn')

    ### Filtrando dia atual
    hoje = dt.datetime.now().strftime('%Y-%m-%d')



    ## Trazendo todos os acordos do dia.
    query = f"""
    SELECT t.dataInicio,
    t.callid,
    CONCAT(t.dddDiscado, t.foneDiscado) as Telefone,
    t.linhaDigitavel,
    t.valorTicket,
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
    FROM tb_infoagent_2023_11 t
    WHERE  t.dataInicio>= '{hoje} 08:00:00'
    AND  t.dataInicio<= '{hoje} 22:00:00'
    AND t.statusProduto IN (20,21,23,24,44,43,45,47)
    AND tiponegociacao IN ('av', 'ap')
    and COD_GRUPO IN (6186,6188,6190,6333,6417)
    """


    ## Trazendo todas as chamadas do dia
    query2 = f"""
    select distinct a.CallID as 'CODIGO UNICO CHAMADA',
    a.NA as 'DDR',
    a.id_arquivo as 'LOTE MAILING',
    a.instante as 'PERIODO',
    a.Operadora as 'LINK USADO',
    a.GrupoPrincipal,
    a.ResultadoClassificacao,
    b.codtabulacao,
    CONCAT(a.ddd, a.Fone) as Telefone,
    c.descricao,
    DATE_FORMAT(SEC_TO_TIME((a.tempoemfila_ms)/1000),'%H:%i:%s') AS 'TEMPO FILA',
    a.MotivoEncerramentoBilhete
    from totalinfo_2023_11 a
    LEFT JOIN atn.tabulacaooper b
    ON a.CallID = b.callid
    LEFT JOIN tabulacao c
    ON b.codtabulacao = c.codtabulacao
    WHERE a.GrupoPrincipal IN (6186,6188,6190,6333,6417)
    and a.instante >= '{hoje} 08:00:00'
    AND a.instante <= '{hoje} 22:00:00';
    """

    acordos_agvs_neg = pd.read_sql(query, connection)
    discagens_agvs_neg = pd.read_sql(query2, connection)
    discagens_agvs_neg = discagens_agvs_neg.drop_duplicates(['CODIGO UNICO CHAMADA'])

    connection.close()

    ### CRIANDO COLUNA HORA DO ACORDO, E DEIXANDO SOMENTE A DATA NA DATA INICIO
    acordos_agvs_neg['dataInicio'] = acordos_agvs_neg['dataInicio'].astype(str)
    acordos_agvs_neg['HORA_ACORDO'] = acordos_agvs_neg['dataInicio'].str[11:13]
    acordos_agvs_neg['dataInicio'] = acordos_agvs_neg['dataInicio'].str[:10]

    ##### IDENTIFICANDO AS FILAS PELOS NOMES
    acordos_agvs_neg[['cod_grupo']]= acordos_agvs_neg[['cod_grupo']].astype(str)  # converte a coluna para o tipo string
    acordos_agvs_neg.loc[acordos_agvs_neg['cod_grupo'] == '6186', 'FILA'] = 'CLARO'
    acordos_agvs_neg.loc[acordos_agvs_neg['cod_grupo'] == '6188', 'FILA'] = 'NET'
    acordos_agvs_neg.loc[acordos_agvs_neg['cod_grupo'] == '6190', 'FILA'] = 'GEVENUE'
    acordos_agvs_neg.loc[acordos_agvs_neg['cod_grupo'] == '6333', 'FILA'] = 'TIM_COB_DIA'
    acordos_agvs_neg.loc[acordos_agvs_neg['cod_grupo'] == '6417', 'FILA'] = 'TIM_COB_NOITE'

    acordos_agvs_neg = acordos_agvs_neg[['dataInicio','HORA_ACORDO','cod_grupo','FILA','descricao','callid']]

    # Defina uma função para substituir "None" pelos valores de "MotivoEncerramentoBilhete"
    def substituir_none(row):
        if row['descricao'] is None or row['descricao'] == 'None':
            return row['MotivoEncerramentoBilhete']
        else:
            return row['descricao']

    # Aplique a função à coluna "descricao"
    discagens_agvs_neg['descricao'] = discagens_agvs_neg.apply(substituir_none, axis=1)


    discagens_agvs_neg['codtabulacao'] = discagens_agvs_neg['codtabulacao'].fillna('9999')

    ### CRIANDO COLUNA HORA DO ACORDO, E DEIXANDO SOMENTE A DATA NA DATA INICIO
    discagens_agvs_neg['PERIODO'] = discagens_agvs_neg['PERIODO'].astype(str)
    discagens_agvs_neg['HORA_DISCAGEM'] = discagens_agvs_neg['PERIODO'].str[11:13]
    discagens_agvs_neg['PERIODO'] = discagens_agvs_neg['PERIODO'].str[:10]


    ##### IDENTIFICANDO AS FILAS PELOS NOMES
    discagens_agvs_neg[['GrupoPrincipal']]= discagens_agvs_neg[['GrupoPrincipal']].astype(str)  # converte a coluna para o tipo string
    discagens_agvs_neg.loc[discagens_agvs_neg['GrupoPrincipal'] == '6186', 'FILA'] = 'CLARO'
    discagens_agvs_neg.loc[discagens_agvs_neg['GrupoPrincipal'] == '6188', 'FILA'] = 'NET'
    discagens_agvs_neg.loc[discagens_agvs_neg['GrupoPrincipal'] == '6190', 'FILA'] = 'GEVENUE'
    discagens_agvs_neg.loc[discagens_agvs_neg['GrupoPrincipal'] == '6333', 'FILA'] = 'TIM_COB_DIA'
    discagens_agvs_neg.loc[discagens_agvs_neg['GrupoPrincipal'] == '6417', 'FILA'] = 'TIM_COB_NOITE'

    discagens_agvs_neg['codtabulacao'] = discagens_agvs_neg['codtabulacao'].replace(',',14)
    discagens_agvs_neg['codtabulacao'] = discagens_agvs_neg['codtabulacao'].astype('int16')

    ########### SEPARANDO AS BASES PARA TER O RANKING DE TABULAÇÃO
    rank_tab = discagens_agvs_neg[['PERIODO','HORA_DISCAGEM','codtabulacao','FILA','descricao']].sort_values(['HORA_DISCAGEM','FILA'])
    rank_tab = rank_tab.query("codtabulacao != 9999")

    discagens_agvs_neg_CLARO =  discagens_agvs_neg.query("GrupoPrincipal == '6186'")
    discagens_agvs_neg_NET =  discagens_agvs_neg.query("GrupoPrincipal == '6188'")
    discagens_agvs_neg_GEVENUE =  discagens_agvs_neg.query("GrupoPrincipal == '6190'")
    discagens_agvs_neg_TIMCOB =  discagens_agvs_neg.query("GrupoPrincipal == '6333' or GrupoPrincipal == '6417'")

    # IDs para as listas claro
    ids_cpc_claro = [3,5,13,15,16,18,21,26,16,20,21,24,27,28,36,37,42,45,48,49,51,86]
    ids_improdutivos_claro = [6,9,10,14,19,27,30,32,1,14,15,2,30,32,38,8,9]
    alert_cause_claro = [12,5,6]

    # Função para aplicar a lógica de preenchimento
    def preencher_descricao_claro(row):
        if row['codtabulacao'] in ids_cpc_claro:
            return 'CPC'
        elif row['codtabulacao'] in ids_improdutivos_claro:
            return 'IMPRODUTIVO'
        elif row['codtabulacao'] in alert_cause_claro:
            return 'F_ENTED'
        else:
            return None

    # IDs para as gevenue
    ids_cpc_gevenue = [1,2,3,4,5,16,18,21,24,25,26,31,16,19,20,21,23,27,28,45,51,86,88]
    ids_improdutivos_gevenue = [8,10,14,19,20,22,27,30,32,1,14,2,38,8,85,9]
    alert_cause_gevenue = [12,5,84]

    # Função para aplicar a lógica de preenchimento
    def preencher_descricao_gevenue(row):
        if row['codtabulacao'] in ids_cpc_claro:
            return 'CPC'
        elif row['codtabulacao'] in ids_improdutivos_claro:
            return 'IMPRODUTIVO'
        elif row['codtabulacao'] in alert_cause_claro:
            return 'F_ENTED'
        else:
            return None

    # IDs para as TIMCOB
    ids_cpc_tim = [1,3,18,16,19,20,21,27,28,37,46,51,86,87,88]
    ids_improdutivos_tim = [1,14,2,38,8,83,85]
    alert_cause_tim = [12,5,84]

    # Função para aplicar a lógica de preenchimento
    def preencher_descricao_tim(row):
        if row['codtabulacao'] in ids_cpc_claro:
            return 'CPC'
        elif row['codtabulacao'] in ids_improdutivos_claro:
            return 'IMPRODUTIVO'
        elif row['codtabulacao'] in alert_cause_claro:
            return 'F_ENTED'
        else:
            return None

    # Aplicar a função à coluna "codtabulacao" para criar a coluna "descricao"
    discagens_agvs_neg_CLARO['descricao'] = discagens_agvs_neg_CLARO.apply(preencher_descricao_claro, axis=1)
    discagens_agvs_neg_CLARO['descricao'] = discagens_agvs_neg_CLARO['descricao'].fillna("ST_TELEF")

    # Aplicar a função à coluna "codtabulacao" para criar a coluna "descricao"
    discagens_agvs_neg_GEVENUE['descricao'] = discagens_agvs_neg_GEVENUE.apply(preencher_descricao_gevenue, axis=1)
    discagens_agvs_neg_GEVENUE['descricao'] = discagens_agvs_neg_GEVENUE['descricao'].fillna("ST_TELEF")
    # Aplicar a função à coluna "codtabulacao" para criar a coluna "descricao"

    discagens_agvs_neg_TIMCOB['descricao'] = discagens_agvs_neg_TIMCOB.apply(preencher_descricao_tim, axis=1)
    discagens_agvs_neg_TIMCOB['descricao'] = discagens_agvs_neg_TIMCOB['descricao'].fillna("ST_TELEF")

    discagens_agvs_neg = pd.concat([discagens_agvs_neg_CLARO,discagens_agvs_neg_GEVENUE,discagens_agvs_neg_TIMCOB])

    discagens_acordos_agv = pd.merge(discagens_agvs_neg, acordos_agvs_neg, right_on=["callid","dataInicio"], left_on=["CODIGO UNICO CHAMADA",'PERIODO'], how="left")
    ### MANTENDO COLUNAS NECESSÁRIAS
    discagens_acordos_agv = discagens_acordos_agv[['PERIODO' ,'callid' ,	'DDR','LINK USADO','GrupoPrincipal','FILA_x','HORA_DISCAGEM','descricao_x','MotivoEncerramentoBilhete','descricao_y']]

    ## RENOMEANDO COLUNAS PARA FICA INTUITIVO
    discagens_acordos_agv = discagens_acordos_agv.rename({'FILA_x': 'FILA'}, axis=1)
    discagens_acordos_agv = discagens_acordos_agv.rename({'descricao_x': 'STATUS'}, axis=1)
    discagens_acordos_agv = discagens_acordos_agv.rename({'descricao_y': 'ACORDO'}, axis=1)



    ######################################################################################## CONSOLIDANDO OS DADOS DE DISCAGENS E ACORDOS HORA A HORA ###############################################################################
    ### CHAMADAS POR HORA
    total_chamadas_hora = discagens_acordos_agv.groupby(['PERIODO','HORA_DISCAGEM','FILA'])['MotivoEncerramentoBilhete'].count().reset_index().sort_values(['HORA_DISCAGEM'])
    total_chamadas_hora = total_chamadas_hora.rename({'MotivoEncerramentoBilhete': 'TOTAL_CHAMADAS'}, axis=1)

    #### ALÔ POR HORA
    alo_por_hora = discagens_acordos_agv.query("STATUS != 'ST_TELEF'")
    alo_por_hora = alo_por_hora.groupby(['PERIODO','HORA_DISCAGEM','FILA'])['STATUS'].count().reset_index()
    alo_por_hora = alo_por_hora.rename({'STATUS': 'ALÔ'}, axis=1)

    ### TOTAL CPC POR HORA
    total_cpc = discagens_acordos_agv.query("STATUS == 'CPC'")
    total_cpc = total_cpc.groupby(['PERIODO','HORA_DISCAGEM','FILA'])['STATUS'].count().reset_index()
    total_cpc = total_cpc.rename({'STATUS': 'CPC'}, axis=1)

    ### TOTAL STATUS TELEFONA POR HORA
    total_status_telefonia = discagens_acordos_agv.query("STATUS == 'ST_TELEF'")
    total_status_telefonia = total_status_telefonia.groupby(['PERIODO','HORA_DISCAGEM','FILA'])['STATUS'].count().reset_index()
    total_status_telefonia = total_status_telefonia.rename({'STATUS': 'ST_TELEF'}, axis=1)

    ### TOTAL FALTA DE ENTENDIMENTO POR HORA
    total_falta_entend = discagens_acordos_agv.query("STATUS == 'F_ENTED'")
    total_falta_entend = total_falta_entend.groupby(['PERIODO','HORA_DISCAGEM','FILA'])['STATUS'].count().reset_index()
    total_falta_entend = total_falta_entend.rename({'STATUS': 'F_ENTEND'}, axis=1)

    ### TOTAL IMPRODUTIVOS POR HORA
    total_improdutivo = discagens_acordos_agv.query("STATUS == 'IMPRODUTIVO'")
    total_improdutivo = total_improdutivo.groupby(['PERIODO','HORA_DISCAGEM','FILA'])['STATUS'].count().reset_index()
    total_improdutivo = total_improdutivo.rename({'STATUS': 'IMPRODUT'}, axis=1)


    ### TOTAL ACORDOS POR HORA
    total_acordos = discagens_acordos_agv.query("ACORDO.notnull()")
    total_acordos = total_acordos.groupby(['PERIODO','HORA_DISCAGEM','FILA'])['STATUS'].count().reset_index()
    total_acordos = total_acordos.rename({'STATUS': 'ACORDOS'}, axis=1)

    df_totais = pd.merge(total_chamadas_hora, alo_por_hora, right_on=["PERIODO","HORA_DISCAGEM","FILA"], left_on=["PERIODO","HORA_DISCAGEM","FILA"], how="left")
    df_totais = pd.merge(df_totais, total_cpc, right_on=["PERIODO","HORA_DISCAGEM","FILA"], left_on=["PERIODO","HORA_DISCAGEM","FILA"], how="left")
    df_totais = pd.merge(df_totais, total_status_telefonia, right_on=["PERIODO","HORA_DISCAGEM","FILA"], left_on=["PERIODO","HORA_DISCAGEM","FILA"], how="left")
    df_totais = pd.merge(df_totais, total_falta_entend, right_on=["PERIODO","HORA_DISCAGEM","FILA"], left_on=["PERIODO","HORA_DISCAGEM","FILA"], how="left")
    df_totais = pd.merge(df_totais, total_improdutivo, right_on=["PERIODO","HORA_DISCAGEM","FILA"], left_on=["PERIODO","HORA_DISCAGEM","FILA"], how="left")
    df_totais = pd.merge(df_totais, total_acordos, right_on=["PERIODO","HORA_DISCAGEM","FILA"], left_on=["PERIODO","HORA_DISCAGEM","FILA"], how="left")
    df_totais = df_totais.fillna(0)
    df_totais[['TOTAL_CHAMADAS',	'ALÔ',	'CPC',	'ST_TELEF',	'F_ENTEND',	'IMPRODUT',	'ACORDOS']] = df_totais[['TOTAL_CHAMADAS',	'ALÔ',	'CPC',	'ST_TELEF',	'F_ENTEND',	'IMPRODUT',	'ACORDOS']].astype('int')


    ### QUANTOS ALOS PARA 1 ACORDO
    df_totais['ALO/ACORDO'] = df_totais['ALÔ'] / df_totais['ACORDOS']
    df_totais['ALO/ACORDO'] = df_totais['ALO/ACORDO'].round(2)


    ### QUANTOS CPC PARA 1 ACORDO
    df_totais['CPC/ACORDO'] = df_totais['CPC'] / df_totais['ACORDOS']
    df_totais['CPC/ACORDO'] = df_totais['CPC/ACORDO'].round(2)

    ### PERC ALÔ = ALO / TOTAL CHAMADA
    df_totais['%ALÔ'] = df_totais['ALÔ'] / df_totais['TOTAL_CHAMADAS']
    df_totais['%ALÔ'] = df_totais['%ALÔ'].round(2)

    ### PERC CPC = CPC / ALÔ
    df_totais['%CPC'] = df_totais['CPC'] / df_totais['ALÔ']
    df_totais['%CPC'] = df_totais['%CPC'].round(2)

    ### PERC ST_TELEF = ST_TELEF / ALÔ
    df_totais['%ST_TELEF'] = df_totais['ST_TELEF'] / df_totais['TOTAL_CHAMADAS']
    df_totais['%ST_TELEF'] = df_totais['%ST_TELEF'].round(2)

    ### PERC FALTA DE ENTENDIMENTO = F_ENTEND / ALÔ
    df_totais['%F_ENTEND'] = df_totais['F_ENTEND'] / df_totais['ALÔ']
    df_totais['%F_ENTEND'] = df_totais['%F_ENTEND'].round(2)

    ### PERC IMPRODUTIVOS = IMPRODUT / ALÔ
    df_totais['%IMPRODUT'] = df_totais['IMPRODUT'] / df_totais['ALÔ']
    df_totais['%IMPRODUT'] = df_totais['%IMPRODUT'].round(2)

    ## QUANTIDADE DE CHAMADAS PARA UM ALÔ
    df_totais['CHAMADA/ALÔ'] = df_totais['TOTAL_CHAMADAS'] / df_totais['ALÔ']
    df_totais['CHAMADA/ALÔ'] = df_totais['CHAMADA/ALÔ'].round(2)


    ## QUANTIDADE DE CHAMADAS PARA UM CPC
    df_totais['CHAMADA/CPC'] = df_totais['TOTAL_CHAMADAS'] / df_totais['CPC']
    df_totais['CHAMADA/CPC'] = df_totais['CHAMADA/CPC'].round(2)

    #### ORGANIZANDO DF
    df_totais = df_totais[['PERIODO','HORA_DISCAGEM','FILA','TOTAL_CHAMADAS','ALÔ','%ALÔ','CPC','%CPC',	'ST_TELEF',	'%ST_TELEF','F_ENTEND',	'%F_ENTEND','IMPRODUT',	'%IMPRODUT','ACORDOS', 'CHAMADA/ALÔ', 'ALO/ACORDO',	'CHAMADA/CPC','CPC/ACORDO']]



    ############## RANKING DE TABULAÇÕES ###################


    #### TOTAL DE TABULAÇÕES POR HORA
    totais_tab = rank_tab.groupby(['PERIODO','HORA_DISCAGEM','FILA'])['codtabulacao'].count().reset_index()
    totais_tab = totais_tab.rename({'codtabulacao': 'TOTAL TABULADO'}, axis=1)

    ##### TOTAL DE CADA TABULAÇÃO POR HORA
    tabulacoes = rank_tab.groupby(['PERIODO','HORA_DISCAGEM','FILA','descricao'])['codtabulacao'].count().reset_index()
    tabulacoes = tabulacoes.rename({'codtabulacao': 'TABULACOES'}, axis=1)



    totais_tab = pd.merge(totais_tab, tabulacoes, right_on=["PERIODO","HORA_DISCAGEM","FILA"], left_on=["PERIODO","HORA_DISCAGEM","FILA"], how="left")


    totais_tab['%TABULACOES'] = totais_tab['TABULACOES'] / totais_tab['TOTAL TABULADO']
    totais_tab['%TABULACOES'] = totais_tab['%TABULACOES'].round(2)
    ## ORGANIZANDO O DA MAIOR TABULAÇÃO PARA A MENOR POR CADA HORA
    totais_tab = totais_tab.sort_values(['HORA_DISCAGEM','FILA','TABULACOES'],ascending=[True,True,False])


    totais_tab.to_csv(fr"R:\TI\TELEFONIA\AGVS_NEG_EXPERT\RANKING_TABULACAO\NOVEMBRO/{hoje}_RANK_TAB_AGVS_NEG.csv",index=False,sep=';')
    df_totais.to_csv(fr"R:\TI\TELEFONIA\AGVS_NEG_EXPERT\DISCAGENS_DIA\NOVEMBRO/{hoje}_AGVS_NEG.csv",index=False,sep=';',encoding='iso-8859-1')


    st.set_page_config(
        page_title="ROBO 2.0",
        layout="wide")


    # Criando uma função para gerar uma imagem de fundo.
    def add_bg_from_url():
        st.markdown(
            f"""
            <style>
            .stApp {{
                background-image: url("https://img.freepik.com/fotos-premium/fundo-de-moleculas-com-formas-poligonais_7247-764.jpg?size=626&ext=jpg&ga=GA1.1.867424154.1697932800&semt=ais");
                background-attachment: fixed;
                background-size: cover
            }}
            </style>
            """,
            unsafe_allow_html=True
        )
    add_bg_from_url() 

    ## ocultando menu e marca d'agua
    hide_streamlit_style = """
                <style>
                #MainMenu {visibility: hidden;}
                footer {visibility: hidden;}
                header {visibility: hidden;}
                </style>
                """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True) 


    st.markdown("""
            <style>
                .block-container {
                        padding-top: 0rem;
                        padding-bottom: 0rem;
                        padding-left: 0.rem;
                        padding-right: 0rem;
                        margin-right: 0rem;
                    }
                color: white;
            </style>
            """, unsafe_allow_html=True)

    # ### Filtrando dia atual
    # hoje = dt.datetime.now().strftime('%Y-%m-%d')
    # df_totais = pd.read_csv(fr"C:\Users\jorgean.bomfim\Desktop\SCRIPTS MAIS USADOS\AGENTES_VIRTUAIS_EXPERT\DISCAGENS_DIA\NOVEMBRO/2023-11-07_AGVS_NEG.csv",sep=';',encoding='iso-8859-1')

    # totais_tab = pd.read_csv(fr"C:\Users\jorgean.bomfim\Desktop\SCRIPTS MAIS USADOS\AGENTES_VIRTUAIS_EXPERT\RANKING_TABULACAO\NOVEMBRO/2023-11-07_RANK_TAB_AGVS_NEG.csv",sep=';',encoding='iso-8859-1')




    ############################################################ config de tela

    ################################# ACORDOS ######################################
    acordo_claro, acordo_gevenue, acordo_tim_dia, acordo_tim_noite = st.columns(4)

    with acordo_claro:
    ##################################### TOTAL ACORDO CLARO
        ac_claro = df_totais.query('FILA == "CLARO"').loc[:,['ACORDOS']].sum()[-1]
        st.markdown(
            f"""
            <div style="background-color: #DC143C; padding: 20px; border-radius: 10px;">
                <h3> ACORDO CLARO </h3>
                <h3>{ac_claro}</h3>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with acordo_gevenue:
        ##################################### TOTAL ACORDO GEVENUE
        ac_gevenue = df_totais.query('FILA == "GEVENUE"').loc[:,['ACORDOS']].sum()[-1]
        st.markdown(
            f"""
            <div style="background-color: #6495ED; padding: 20px; border-radius: 10px;">
                <h3> ACORDOS GEVENUE </h3>
                <h3>{ac_gevenue}</h3>
            </div>
            """,
            unsafe_allow_html=True,
        )  

    with acordo_tim_dia:
        ##################################### TOTAL ACORDO TIM DIA
        ac_tim_dia = df_totais.query('FILA == "TIM_COB_DIA"').loc[:,['ACORDOS']].sum()[-1]
        st.markdown(
            f"""
            <div style="background-color: #0000FF; padding: 20px; border-radius: 10px;">
                <h3> ACORDOS TIM DIA </h3>
                <h3>{ac_tim_dia}</h3>
            </div>
            """,
            unsafe_allow_html=True,
        )
        
    with acordo_tim_noite:
        ac_tim_noite = df_totais.query('FILA == "TIM_COB_NOITE"').loc[:,['ACORDOS']].sum()[-1]
        st.markdown(
            f"""
            <div style="background-color: #0000FF; padding: 20px; border-radius: 10px;">
                <h3> ACORDOS TIM NOITE </h3>
                <h3> {ac_tim_noite} </h3>
            </div>
            """,
            unsafe_allow_html=True,
        )

    ################################# ACORDOS ######################################

    "\n"
    "\n"

    ################################# TOTAIS DE CHAMADAS #################################

    chamadas_claro,chamadas_gevenue, chamadas_tim_dia, chamada_tim_noite = st.columns(4)

    ######################################## CHAMADAS CLARO ##################################

    total_chamadas_claro = df_totais.query('FILA == "CLARO"').loc[:,['TOTAL_CHAMADAS']].sum()[-1]
    total_cpc_claro = df_totais.query('FILA == "CLARO"').loc[:,['CPC']].sum()[-1]

    chamadas_claro.metric(label="CHAMADAS CLARO",value= total_chamadas_claro)
    chamadas_claro.metric(label="CPC",value= total_cpc_claro)



    ######################################## CHAMADAS GEVENUE ##################################

    total_chamadas_gevenue = df_totais.query('FILA == "GEVENUE"').loc[:,['TOTAL_CHAMADAS']].sum()[-1]
    total_cpc_gevenue = df_totais.query('FILA == "GEVENUE"').loc[:,['CPC']].sum()[-1]

    chamadas_gevenue.metric(label="CHAMADAS GEVENUE",value= total_chamadas_gevenue)
    chamadas_gevenue.metric(label="CPC",value= total_cpc_gevenue)


    ######################################## CHAMADAS TIM DIA ##################################

    total_chamadas_tim_dia = df_totais.query('FILA == "TIM_COB_DIA"').loc[:,['TOTAL_CHAMADAS']].sum()[-1]
    total_cpc_tim_dia = df_totais.query('FILA == "TIM_COB_DIA"').loc[:,['CPC']].sum()[-1]

    chamadas_tim_dia.metric(label="CHAMADAS TIM DIA",value= total_chamadas_tim_dia)
    chamadas_tim_dia.metric(label="CPC",value= total_cpc_tim_dia)


    ######################################## CHAMADAS TIM NOITE ##################################

    total_chamadas_tim_noite = df_totais.query('FILA == "TIM_COB_NOITE"').loc[:,['TOTAL_CHAMADAS']].sum()[-1]
    total_cpc_tim_noite = df_totais.query('FILA == "TIM_COB_NOITE"').loc[:,['CPC']].sum()[-1]

    chamada_tim_noite.metric(label="CHAMADAS TIM NOITE",value= total_chamadas_tim_noite)
    chamada_tim_noite.metric(label="CPC",value= total_cpc_tim_noite)


    ################################ TOTAIS DE CHAMADAS #################################


    "\n"
    ################################# RANKING TABULAÇÃO ULTIMA HORA ###########################

    tabulacao_hora_claro = totais_tab.query('FILA == "CLARO"').loc[:,['HORA_DISCAGEM','descricao','TABULACOES']].sort_values(['HORA_DISCAGEM','TABULACOES'],ascending=[False,False])[0:5]
    hora_discagem = tabulacao_hora_claro['HORA_DISCAGEM'].reset_index(drop=True).iloc[1]
    tabulacao_hora_claro = tabulacao_hora_claro.loc[:,['descricao','TABULACOES']]


    ranking_1, ranking_2 = st.columns(2)
    with ranking_1:
        st.title(f" Ranking Tablulações {hora_discagem}:00 h")

        col1,col2= st.columns(2)
        with col1:
            st.subheader('CLARO')
            st.write(tabulacao_hora_claro)
            
        with col2:
            tabulacao_hora_gevenue = totais_tab.query('FILA == "GEVENUE"').loc[:,['HORA_DISCAGEM','descricao','TABULACOES']].sort_values(['HORA_DISCAGEM','TABULACOES'],ascending=[False,False])[0:5]
            tabulacao_hora_gevenue = tabulacao_hora_gevenue.loc[:,['descricao','TABULACOES']]
            st.subheader('GEVENUE')
            st.write(tabulacao_hora_gevenue)
            



    with ranking_2:
        st.title(f"Ranking Tablulações {hora_discagem}:00 h")

        col1,col2= st.columns(2)
        with col1:
            tabulacao_hora_tim_dia = totais_tab.query('FILA == "TIM_COB_DIA"').loc[:,['HORA_DISCAGEM','descricao','TABULACOES']].sort_values(['HORA_DISCAGEM','TABULACOES'],ascending=[False,False])[0:5]
            tabulacao_hora_tim_dia = tabulacao_hora_tim_dia.loc[:,['descricao','TABULACOES']]

            st.subheader('TIM DIA')
            st.write(tabulacao_hora_tim_dia)
            
        with col2:
            tabulacao_hora_tim_noite = totais_tab.query('FILA == "TIM_COB_NOITE"').loc[:,['HORA_DISCAGEM','descricao','TABULACOES']].sort_values(['HORA_DISCAGEM','TABULACOES'],ascending=[False,False])[0:5]
            tabulacao_hora_tim_noite = tabulacao_hora_tim_noite.loc[:,['descricao','TABULACOES']]
            st.subheader('TIM NOITE')
            st.write(tabulacao_hora_tim_noite)
    "\n"
    "\n"




    tabulacoes_dia, dftotais =  st.columns([0.8,2.5])
    with tabulacoes_dia:
        st.title(f"Tablulações Dia")
        ### filtrando as 20 maiores incidencia de tabulação do dia inteiro
        totais_tab_dia = totais_tab.loc[:,['FILA','descricao','TABULACOES','TOTAL TABULADO']]
        totais_tab_dia = totais_tab_dia.groupby(['FILA','descricao'])['TABULACOES'].sum().reset_index().sort_values(['TABULACOES','TABULACOES'],ascending=[False,True])
        totais_tab_dia = totais_tab_dia.head(20)

        totais_tab_dia = totais_tab_dia.sort_values(['FILA','TABULACOES'],ascending=[True,False])
        st.write(totais_tab_dia)


    with dftotais:
        st.title(f"Discagens Dia")
        st.write(df_totais)




    # Espera por 5 minutos
    time.sleep(300)  # 600 segundos = 10 minutos

    # Reinicia o aplicativo para atualização
    st.experimental_rerun()
