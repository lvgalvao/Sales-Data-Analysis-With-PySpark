from data_clean import delete_csv_files
from data_generator import (generate_customers_data, generate_products_data,
                            generate_sales_data, generate_salesperson_data,
                            generate_store_data)


def main():
    # Defina o caminho do diretório onde será gerado os dados
    dir_path = 'data'

    # Deleta todos os arquivos CSV no diretório
    delete_csv_files(dir_path)

    # Gere os dados dos clientes
    generate_customers_data(dir_path, num_rows=50)

    # Gere os dados dos produtos
    generate_products_data(dir_path, num_rows=10)

    # Gere os dados das lojas
    generate_store_data(dir_path, num_rows=10)

    # Gere os dados dos vendedores
    generate_salesperson_data(dir_path, num_rows=10)

    # Gere os dados de vendas
    generate_sales_data(dir_path)


if __name__ == '__main__':
    main()
