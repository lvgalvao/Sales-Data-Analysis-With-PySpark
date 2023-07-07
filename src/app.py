import pandas as pd
import streamlit as st
from data_processing import data_processing
from pyspark.sql import SparkSession


# Function to calculate sales information
def calcular_informacoes_vendas(df):

    total_vendas = df['Vendas'].sum()
    media_vendas = df['Vendas'].mean()
    max_vendas = df['Vendas'].max()
    min_vendas = df['Vendas'].min()

    # Display calculated information
    col1, col2 = st.columns(2)
    col1.markdown(f'Total de Vendas: **`{total_vendas}`**')
    col2.markdown(f'Média de Vendas: **`{media_vendas}`**')

    col1.markdown(f'Maior Venda: **`{max_vendas}`**')
    col2.markdown(f'Menor Venda: **`{min_vendas}`**')

    # Plot of cumulative monthly sales
    st.subheader('Gráfico de Vendas Acumulado Mensal')
    vendas_acumuladas = df.resample('M')['Vendas'].sum().cumsum()
    st.line_chart(vendas_acumuladas)

    # Select specific products
    st.subheader('Selecionar Produtos')
    produtos = df['Produto'].unique()
    produtos_selecionados = st.multiselect('Selecione os produtos', produtos)
    if produtos_selecionados:
        df_filtrado = df[df['Produto'].isin(produtos_selecionados)]
        vendas_por_produto_vendedor = (
            df_filtrado.groupby(['Produto', 'Vendedor'])['Vendas']
            .sum()
            .unstack()
        )
        st.bar_chart(vendas_por_produto_vendedor)


# Main part of the app
st.title('Análise de Vendas')

# Load the data
st.header('Carregar Dados')
data_file = st.file_uploader('Faça o upload do arquivo CSV', type=['csv'])

if data_file is not None:
    # Process the CSV file with PySpark and get the path to the processed data
    processed_data_path = data_processing(data_file)

    # Read the processed data with Pandas
    df = pd.read_parquet(processed_data_path)

    # Display the loaded data
    st.subheader('Dados Carregados')
    st.write(df)

    # Calculate and display sales information
    st.header('InformInformações de Vendas')
    calcular_informacoes_vendas(df)
