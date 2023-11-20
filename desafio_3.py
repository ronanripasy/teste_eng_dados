import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Obtendo caminho do diretório
diretorio = os.getcwd()

# Create SparkSession
spark = SparkSession.builder \
            .appName('desafio3') \
            .getOrCreate()

# Lendo arquivo json
df_full_nota_fiscal = spark.read.option("multiline", "true").json(f"{diretorio}/arquivos/desafio3_data.json")

# Criando dataframe com columa de ItemList expandida por nota fiscal
df_explode_products = df_full_nota_fiscal.select('*', explode("ItemList").alias("Products")).drop("ItemList")
df_explode_products.show()

# Normalizando dados de NOTA FISCAL e ITEMS DA NOTA FISCAL

# Criando dataframe com informações somente de nota fiscal
df_nota_fiscal = df_explode_products.drop("Products").distinct()
df_nota_fiscal.show(truncate=False)

# Criando dataframe com informações expandidas de Produtos por nota fiscal
df_itens_nf = df_explode_products.select("NFeID", "Products.ProductName", "Products.Value", "Products.Quantity")
df_itens_nf.show()
