import pandas as pd
import os

def load_with_source(filename):
    """
    Carrega um arquivo CSV e adiciona uma coluna com o nome da farmácia
    baseado no nome do arquivo
    """
    df = pd.read_csv(filename)

    # Extrair nome da farmácia do arquivo
    farmacia_name = os.path.basename(filename).replace('_data.csv', '').replace('_', ' ').title()
    
    # Mapeamento específico para nomes mais corretos
    farmacia_mapping = {
        'Farmaponte': 'Farmaponte',
        'Sao Joao': 'São João',
        'Sao Paulo': 'São Paulo',
        'Veracruz': 'Veracruz'
    }
    
    # Aplicar mapeamento se existir
    farmacia_name = farmacia_mapping.get(farmacia_name, farmacia_name)
    
    # Adicionar coluna da farmácia
    df['farmacia'] = farmacia_name
    
    return df

def consolidar_arquivos_csv():
    """
    Consolida todos os arquivos CSV da pasta bigquery_results
    adicionando uma coluna com o nome da farmácia
    """
    # Lista dos arquivos CSV
    csv_files = [
        'farmaponte_data.csv',
        'sao_joao_data.csv', 
        'sao_paulo_data.csv',
        'veracruz_data.csv'
    ]
    
    # Carregar todos os arquivos com a coluna da farmácia
    dfs = []
    for filename in csv_files:
        filepath = os.path.join(os.path.dirname(__file__), filename)
        if os.path.exists(filepath):
            print(f"Carregando {filename}...")
            df = load_with_source(filepath)
            print(f"  - {len(df)} registros carregados da farmácia: {df['farmacia'].iloc[0]}")
            dfs.append(df)
        else:
            print(f"Arquivo não encontrado: {filepath}")
    
    if dfs:
        # Consolidar todos os DataFrames
        print("\nConsolidando dados...")
        consolidated_df = pd.concat(dfs, ignore_index=True)
        
        # Reorganizar colunas para ter farmácia no início
        columns = ['farmacia'] + [col for col in consolidated_df.columns if col != 'farmacia']
        consolidated_df = consolidated_df[columns]
        
        # Salvar arquivo consolidado
        output_file = os.path.join(os.path.dirname(__file__), 'dados_consolidados.csv')
        consolidated_df.to_csv(output_file, index=False)
        
        print(f"\nConsolidação concluída!")
        print(f"Total de registros: {len(consolidated_df)}")
        print(f"Arquivo salvo em: {output_file}")
        
        # Mostrar resumo por farmácia
        print("\nResumo por farmácia:")
        resumo = consolidated_df['farmacia'].value_counts()
        for farmacia, count in resumo.items():
            print(f"  - {farmacia}: {count} registros")
        
        return consolidated_df
    else:
        print("Nenhum arquivo CSV encontrado!")
        return None

if __name__ == "__main__":
    consolidar_arquivos_csv()