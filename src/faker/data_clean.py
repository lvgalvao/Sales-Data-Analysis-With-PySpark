import glob
import os


def delete_csv_files(dir_path='.'):
    # Lista todos os arquivos CSV no diretório
    csv_files = glob.glob(os.path.join(dir_path, '*.csv'))

    # Exclui cada arquivo CSV
    for file in csv_files:
        os.remove(file)
