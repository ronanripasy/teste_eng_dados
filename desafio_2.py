import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round

# Obtendo caminho do diretório
diretorio = os.getcwd()

# Create SparkSession
spark = SparkSession.builder \
            .appName('desafio2') \
            .getOrCreate()

df_transacoes = spark.read.json(f"{diretorio}/arquivos/desafio2_transacoes.json")

# Tratando dados nulos na coluna de percentual
df_transacoes = df_transacoes.fillna({'desconto_percentual':0})

# Calculando o valor liquido com desconto do percentual
df_valor_desconto_percentual = df_transacoes.withColumn("vlr_desconto_percentual", df_transacoes['total_bruto'] - (df_transacoes['total_bruto'] * df_transacoes['desconto_percentual']/100))

# Somando os valores para obter o total_liquido: soma(total_bruto – desconto_percentual)
df_total_bruto = df_valor_desconto_percentual.select(
    round(sum(df_valor_desconto_percentual.vlr_desconto_percentual), 2).alias("total_liquido")
)

df_total_bruto.show()
