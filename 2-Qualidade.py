# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Qualidade dos dados
# MAGIC
# MAGIC Antes de limpar, olho o que tem de problema na bronze: nulos, formatos,
# MAGIC valores estranhos, duplicatas e outliers. O que aparecer aqui é tratado na silver.

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("workspace.bronze.precos_combustiveis")

total = df.count()
print("total de registros:", total)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Completude (nulos e vazios)

# COMMAND ----------

colunas = [c for c in df.columns if c not in ("arquivo_origem", "data_ingestao")]

nulos = df.select([
    F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
    for c in colunas
]).collect()[0].asDict()

for c, n in nulos.items():
    print(f"{c:<16} {n:>12,}  ({n / total * 100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consistência
# MAGIC
# MAGIC Preço: vem como texto com vírgula (`7,97`). Testo se todos convertem pra número.
# MAGIC Data: formato esperado `dd/MM/yyyy`.

# COMMAND ----------

teste = (
    df.withColumn("preco", F.regexp_replace("valor_venda", ",", ".").cast("double"))
      .withColumn("data", F.to_date(F.trim("data_coleta"), "dd/MM/yyyy"))
)

preco_invalido = teste.filter(F.col("valor_venda").isNotNull() & F.col("preco").isNull()).count()
data_invalida = teste.filter(F.col("data_coleta").isNotNull() & F.col("data").isNull()).count()

print("preços que não convertem:", preco_invalido)
print("datas que não convertem:", data_invalida)

# COMMAND ----------

# período real de cada arquivo, pra conferir se bate com o nome
display(
    teste.groupBy("arquivo_origem")
         .agg(F.min("data").alias("primeira"), F.max("data").alias("ultima"), F.count("*").alias("registros"))
         .orderBy("arquivo_origem")
)

# COMMAND ----------

# MAGIC %md
# MAGIC CNPJ: reparei que ele vem com um espaço na frente (`" 01.492.748/0003-83"`).
# MAGIC Se não tratar, agrupar por posto dá errado.

# COMMAND ----------

cnpj_espaco = df.filter(F.col("cnpj_revenda") != F.trim("cnpj_revenda")).count()
print(f"CNPJs com espaço: {cnpj_espaco:,} de {total:,}")

# COMMAND ----------

# UFs
display(df.groupBy("estado_sigla").count().orderBy("estado_sigla"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Acurácia
# MAGIC
# MAGIC Produtos que aparecem na base e a unidade de medida de cada um.

# COMMAND ----------

display(
    teste.groupBy("produto", "unidade_medida")
         .agg(F.count("*").alias("registros"),
              F.min("preco").alias("min"),
              F.max("preco").alias("max"),
              F.round(F.avg("preco"), 3).alias("media"))
         .orderBy("produto")
)

# COMMAND ----------

# MAGIC %md
# MAGIC O GNV é vendido em R$/m³ e os outros em R$/litro. Não dá pra comparar preço de GNV
# MAGIC com gasolina, então nas análises vou separar ele.
# MAGIC
# MAGIC Também procuro preços fora de uma faixa que faça sentido (abaixo de R$ 0,50 ou
# MAGIC acima de R$ 20).

# COMMAND ----------

fora_faixa = teste.filter((F.col("preco") < 0.5) | (F.col("preco") > 20))
print("preços fora da faixa:", fora_faixa.count())
display(fora_faixa.select("produto", "valor_venda", "estado_sigla", "data_coleta", "arquivo_origem").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unicidade
# MAGIC
# MAGIC Cada linha deveria ser um posto, um produto, uma data. Vejo se tem repetição.

# COMMAND ----------

duplicadas = (
    df.withColumn("cnpj", F.trim("cnpj_revenda"))
      .groupBy("cnpj", "produto", "data_coleta")
      .count()
      .filter("count > 1")
)

print("combinações repetidas:", duplicadas.count())
print("registros envolvidos:", duplicadas.agg(F.sum("count")).collect()[0][0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outliers
# MAGIC
# MAGIC Uso IQR por produto e por ano. Se calcular na série toda, o ano de 2022 inteiro
# MAGIC ia virar outlier, porque o preço subiu muito. Aqui só conto, não removo.

# COMMAND ----------

base = teste.withColumn("ano", F.year("data")).filter(F.col("preco").isNotNull())

limites = (
    base.groupBy("produto", "ano")
        .agg(F.expr("percentile_approx(preco, 0.25)").alias("q1"),
             F.expr("percentile_approx(preco, 0.75)").alias("q3"))
        .withColumn("inf", F.col("q1") - 1.5 * (F.col("q3") - F.col("q1")))
        .withColumn("sup", F.col("q3") + 1.5 * (F.col("q3") - F.col("q1")))
)

outliers = (
    base.join(limites, ["produto", "ano"])
        .withColumn("outlier", (F.col("preco") < F.col("inf")) | (F.col("preco") > F.col("sup")))
)

qtd = outliers.filter("outlier").count()
print(f"outliers: {qtd:,} ({qtd / total * 100:.2f}%)")

display(
    outliers.groupBy("produto", "ano")
            .agg(F.sum(F.col("outlier").cast("int")).alias("outliers"), F.count("*").alias("total"))
            .orderBy("produto", "ano")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo
# MAGIC
# MAGIC Problemas encontrados e o que vou fazer na silver:
# MAGIC
# MAGIC - Preço em texto com vírgula → converter pra número
# MAGIC - Data em texto → converter pra date
# MAGIC - CNPJ com espaço na frente → trim e deixar só os dígitos
# MAGIC - GNV em R$/m³ misturado com os outros em R$/litro → criar coluna separando
# MAGIC - Valor de compra vazio (ANP descontinuou em 2020) → manter, não uso nas análises
# MAGIC - Duplicatas de posto/produto/data → manter uma linha
# MAGIC - Preços fora da faixa → remover
# MAGIC - Outliers → marcar com uma flag, sem remover
# MAGIC