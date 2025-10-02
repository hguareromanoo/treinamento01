from google.cloud import bigquery

client = bigquery.Client()
# descoberta de dataset
for dataset in client.list_datasets():
    print(f"Dataset ID: {dataset.dataset_id}")

# descoberta de tabelas
for table in client.list_tables('Farmacias'):
    print(f"Table ID: {table.table_id}")

# > Resultados: 

# Dataset ID: Farmacias
# # Table ID: Historico_Vendas_Farma_Ponte
#Table ID: Historico_Vendas_Sao_Joao
#Table ID: Historico_Vendas_Sao_Paulo
#Table ID: Historico_Vendas_Vera_Cruz

for id in ['Historico_Vendas_Farma_Ponte', 'Historico_Vendas_Sao_Joao', 'Historico_Vendas_Sao_Paulo', 'Historico_Vendas_Vera_Cruz']:
    table = client.get_table(f'Farmacias.{id}')
    print(f"Table {table.table_id} has {table.num_rows} rows and {len(table.schema)} columns.")
    for schema_field in table.schema:
        print(schema_field.name, schema_field.field_type, schema_field.mode)