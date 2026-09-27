# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Camada Silver
# MAGIC
# MAGIC Limpeza e tipagem com base no que encontrei no notebook de qualidade.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

df = spark.table("workspace.bronze.precos_combustiveis")

# guardo a contagem depois de cada etapa pra saber onde perdi registros
contagem = [("bronze", df.count())]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tipos e padronização de texto

# COMMAND ----------

df = (
    df.withColumn("preco_venda", F.regexp_replace(F.trim("valor_venda"), ",", ".").cast("double"))
      .withColumn("preco_compra", F.regexp_replace(F.trim("valor_compra"), ",", ".").cast("double"))
      .withColumn("data_coleta", F.to_date(F.trim("data_coleta"), "dd/MM/yyyy"))
      .withColumn("cnpj_revenda", F.regexp_replace("cnpj_revenda", "[^0-9]", ""))
      .withColumn("cep", F.regexp_replace("cep", "[^0-9]", ""))
)

for c in ["regiao_sigla", "estado_sigla", "municipio", "revenda", "bairro", "produto", "bandeira"]:
    df = df.withColumn(c, F.upper(F.trim(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtros

# COMMAND ----------

# sem preço, data, produto ou UF não tem o que analisar
df = df.filter(
    F.col("preco_venda").isNotNull()
    & F.col("data_coleta").isNotNull()
    & F.col("produto").isNotNull()
    & F.col("estado_sigla").isNotNull()
)
contagem.append(("campos obrigatorios", df.count()))

# só os produtos da série de automotivos
produtos = ["GASOLINA", "GASOLINA ADITIVADA", "ETANOL", "DIESEL", "DIESEL S10", "GNV"]
df = df.filter(F.col("produto").isin(produtos))
contagem.append(("produtos validos", df.count()))

# faixa de preço
df = df.filter(F.col("preco_venda").between(0.5, 20))
contagem.append(("faixa de preco", df.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicatas
# MAGIC
# MAGIC Fico com uma linha por posto + produto + data (a de menor preço, pra ser sempre o mesmo resultado).

# COMMAND ----------

janela = Window.partitionBy("cnpj_revenda", "produto", "data_coleta").orderBy("preco_venda")

df = df.withColumn("rn", F.row_number().over(janela)).filter("rn = 1").drop("rn")
contagem.append(("duplicatas", df.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Colunas novas

# COMMAND ----------

df = (
    df.withColumn("ano", F.year("data_coleta"))
      .withColumn("mes", F.month("data_coleta"))
      .withColumn("semestre", F.when(F.col("mes") <= 6, 1).otherwise(2))
      # GNV é R$/m3, o resto R$/litro
      .withColumn("tipo_unidade", F.when(F.col("produto") == "GNV", "M3").otherwise("LITRO"))
      .withColumn("ano_eleitoral", F.col("ano").isin([2016, 2018, 2020, 2022, 2024, 2026]))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outliers
# MAGIC
# MAGIC IQR por produto e ano. Só marco, não removo: a variação de preço entre postos é uma
# MAGIC das perguntas do trabalho.

# COMMAND ----------

limites = (
    df.groupBy("produto", "ano")
      .agg(F.expr("percentile_approx(preco_venda, 0.25)").alias("q1"),
           F.expr("percentile_approx(preco_venda, 0.75)").alias("q3"))
      .withColumn("inf", F.col("q1") - 1.5 * (F.col("q3") - F.col("q1")))
      .withColumn("sup", F.col("q3") + 1.5 * (F.col("q3") - F.col("q1")))
)

df = (
    df.join(limites, ["produto", "ano"])
      .withColumn("outlier", (F.col("preco_venda") < F.col("inf")) | (F.col("preco_venda") > F.col("sup")))
      .drop("q1", "q3", "inf", "sup")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravando

# COMMAND ----------

silver = df.select(
    "regiao_sigla", "estado_sigla", "municipio",
    "cnpj_revenda", "revenda", "bairro", "cep", "bandeira",
    "produto", "tipo_unidade",
    "preco_venda", "preco_compra", "outlier",
    "data_coleta", "ano", "mes", "semestre", "ano_eleitoral",
    "arquivo_origem",
)

(
    silver.write
    .mode("overwrite")
    .option("overwriteSchema", True)
    .partitionBy("ano")
    .saveAsTable("workspace.silver.precos_combustiveis")
)

contagem.append(("silver", spark.table("workspace.silver.precos_combustiveis").count()))

# COMMAND ----------

# registros em cada etapa
log = spark.createDataFrame(contagem, ["etapa", "registros"])
log.write.mode("overwrite").saveAsTable("workspace.silver.log_limpeza")
display(log)

# COMMAND ----------

# conferência: tudo aqui tem que dar zero
s = spark.table("workspace.silver.precos_combustiveis")

print("preço nulo:", s.filter("preco_venda is null").count())
print("data nula:", s.filter("data_coleta is null").count())
print("cnpj com caractere:", s.filter(F.col("cnpj_revenda").rlike("[^0-9]")).count())
print("duplicatas:", s.groupBy("cnpj_revenda", "produto", "data_coleta").count().filter("count > 1").count())

# COMMAND ----------

display(s.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descrição no catálogo

# COMMAND ----------

spark.sql("""
COMMENT ON TABLE workspace.silver.precos_combustiveis IS
'Precos de combustiveis limpos. Preco em numero, data em date, texto em maiusculo,
CNPJ so com digitos. Sem duplicatas de posto/produto/data. Outliers marcados na coluna outlier.
Particionada por ano.'
""")

descricoes = {
    "regiao_sigla": "Sigla da regiao (N, NE, CO, SE, S)",
    "estado_sigla": "Sigla da UF",
    "municipio": "Municipio do posto",
    "cnpj_revenda": "CNPJ do posto, somente digitos",
    "revenda": "Razao social do posto",
    "bairro": "Bairro",
    "cep": "CEP, somente digitos",
    "bandeira": "Bandeira do posto. BRANCA = sem bandeira",
    "produto": "GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10 ou GNV",
    "tipo_unidade": "LITRO ou M3. GNV e vendido em m3 e nao deve ser comparado com os outros",
    "preco_venda": "Preco de venda ao consumidor (R$). Faixa 0,50 a 20,00",
    "preco_compra": "Preco de compra do posto. Vazio a partir de ago/2020",
    "outlier": "Verdadeiro se o preco esta fora de 1,5 IQR do produto no ano",
    "data_coleta": "Data da coleta",
    "ano": "Ano da coleta",
    "mes": "Mes da coleta",
    "semestre": "Semestre (1 ou 2)",
    "ano_eleitoral": "Verdadeiro para 2016, 2018, 2020, 2022, 2024 e 2026",
    "arquivo_origem": "Arquivo CSV de origem",
}

for col, desc in descricoes.items():
    spark.sql(f"ALTER TABLE workspace.silver.precos_combustiveis ALTER COLUMN {col} COMMENT '{desc}'")