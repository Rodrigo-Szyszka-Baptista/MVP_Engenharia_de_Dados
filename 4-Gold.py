# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Camada Gold (esquema estrela)
# MAGIC
# MAGIC Fato: `fato_precos`, uma linha por posto + produto + data.
# MAGIC Dimensões: tempo, produto, localidade, revenda e bandeira.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

silver = spark.table("workspace.silver.precos_combustiveis")
gold = "workspace.gold"

def salvar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{gold}.{nome}")
    print(nome, spark.table(f"{gold}.{nome}").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_tempo
# MAGIC Chave no formato yyyyMMdd.

# COMMAND ----------

dim_tempo = (
    silver.select("data_coleta").distinct()
    .withColumn("sk_tempo", F.date_format("data_coleta", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data_coleta"))
    .withColumn("semestre", F.when(F.month("data_coleta") <= 6, 1).otherwise(2))
    .withColumn("trimestre", F.quarter("data_coleta"))
    .withColumn("mes", F.month("data_coleta"))
    .withColumn("ano_mes", F.date_format("data_coleta", "yyyy-MM"))
    .withColumn("ano_eleitoral", F.col("ano").isin([2016, 2018, 2020, 2022, 2024, 2026]))
    .select("sk_tempo", "data_coleta", "ano", "semestre", "trimestre", "mes", "ano_mes", "ano_eleitoral")
)
salvar(dim_tempo, "dim_tempo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_produto

# COMMAND ----------

dim_produto = (
    silver.select("produto", "tipo_unidade").distinct()
    .withColumn("sk_produto", F.row_number().over(Window.orderBy("produto")))
    .withColumn("categoria",
        F.when(F.col("produto").startswith("GASOLINA"), "GASOLINA")
         .when(F.col("produto").startswith("DIESEL"), "DIESEL")
         .otherwise(F.col("produto")))
    .select("sk_produto", "produto", "categoria", "tipo_unidade")
)
salvar(dim_produto, "dim_produto")
display(spark.table(f"{gold}.dim_produto"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_localidade

# COMMAND ----------

regioes = {"N": "Norte", "NE": "Nordeste", "CO": "Centro-Oeste", "SE": "Sudeste", "S": "Sul"}
mapa_regiao = F.create_map([F.lit(x) for par in regioes.items() for x in par])

dim_localidade = (
    silver.select("regiao_sigla", "estado_sigla", "municipio").distinct()
    .withColumn("sk_localidade", F.row_number().over(Window.orderBy("estado_sigla", "municipio")))
    .withColumn("regiao", mapa_regiao[F.col("regiao_sigla")])
    .select("sk_localidade", "regiao_sigla", "regiao", "estado_sigla", "municipio")
)
salvar(dim_localidade, "dim_localidade")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_revenda
# MAGIC Um registro por CNPJ. Se o posto mudou de nome ao longo dos anos, fico com o mais recente.

# COMMAND ----------

ultimo = Window.partitionBy("cnpj_revenda").orderBy(F.desc("data_coleta"))

dim_revenda = (
    silver.select("cnpj_revenda", "revenda", "bairro", "cep", "municipio", "estado_sigla", "data_coleta")
    .withColumn("rn", F.row_number().over(ultimo))
    .filter("rn = 1")
    .withColumn("sk_revenda", F.row_number().over(Window.orderBy("cnpj_revenda")))
    .select("sk_revenda", "cnpj_revenda", "revenda", "bairro", "cep", "municipio", "estado_sigla")
)
salvar(dim_revenda, "dim_revenda")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_bandeira

# COMMAND ----------

dim_bandeira = (
    silver.select("bandeira").distinct()
    .withColumn("sk_bandeira", F.row_number().over(Window.orderBy("bandeira")))
    .withColumn("bandeira_branca", F.col("bandeira") == "BRANCA")
    .select("sk_bandeira", "bandeira", "bandeira_branca")
)
salvar(dim_bandeira, "dim_bandeira")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fato_precos
# MAGIC Troco os textos pelas chaves das dimensões.

# COMMAND ----------

fato = (
    silver
    .join(spark.table(f"{gold}.dim_tempo").select("sk_tempo", "data_coleta"), "data_coleta")
    .join(spark.table(f"{gold}.dim_produto").select("sk_produto", "produto"), "produto")
    .join(spark.table(f"{gold}.dim_localidade").select("sk_localidade", "estado_sigla", "municipio"),
          ["estado_sigla", "municipio"])
    .join(spark.table(f"{gold}.dim_revenda").select("sk_revenda", "cnpj_revenda"), "cnpj_revenda")
    .join(spark.table(f"{gold}.dim_bandeira").select("sk_bandeira", "bandeira"), "bandeira")
    .select("sk_tempo", "sk_produto", "sk_localidade", "sk_revenda", "sk_bandeira",
            "preco_venda", "outlier", "ano")
)

(
    fato.write.mode("overwrite").option("overwriteSchema", True)
    .partitionBy("ano")
    .saveAsTable(f"{gold}.fato_precos")
)

print("silver:", silver.count())
print("fato:  ", spark.table(f"{gold}.fato_precos").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### View juntando tudo
# MAGIC Pra facilitar as consultas da análise.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.vw_precos AS
SELECT f.preco_venda, f.outlier,
       t.data_coleta, t.ano, t.semestre, t.mes, t.ano_mes, t.ano_eleitoral,
       p.produto, p.categoria, p.tipo_unidade,
       l.regiao, l.estado_sigla, l.municipio,
       r.cnpj_revenda, r.revenda,
       b.bandeira, b.bandeira_branca
FROM {gold}.fato_precos f
JOIN {gold}.dim_tempo t      ON f.sk_tempo = t.sk_tempo
JOIN {gold}.dim_produto p    ON f.sk_produto = p.sk_produto
JOIN {gold}.dim_localidade l ON f.sk_localidade = l.sk_localidade
JOIN {gold}.dim_revenda r    ON f.sk_revenda = r.sk_revenda
JOIN {gold}.dim_bandeira b   ON f.sk_bandeira = b.sk_bandeira
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descrições no catálogo

# COMMAND ----------

tabelas = {
    "fato_precos": "Fato de precos coletados. Uma linha por posto, produto e data. Particionada por ano.",
    "dim_tempo": "Datas das coletas. Chave sk_tempo no formato yyyyMMdd.",
    "dim_produto": "Combustiveis. tipo_unidade separa GNV (m3) dos demais (litro).",
    "dim_localidade": "Regiao, UF e municipio.",
    "dim_revenda": "Postos, um por CNPJ, com o cadastro mais recente.",
    "dim_bandeira": "Bandeiras dos postos.",
}
for t, d in tabelas.items():
    spark.sql(f"COMMENT ON TABLE {gold}.{t} IS '{d}'")

colunas = {
    "fato_precos": {
        "sk_tempo": "Chave para dim_tempo",
        "sk_produto": "Chave para dim_produto",
        "sk_localidade": "Chave para dim_localidade",
        "sk_revenda": "Chave para dim_revenda",
        "sk_bandeira": "Chave para dim_bandeira",
        "preco_venda": "Preco de venda (R$ por litro ou por m3 no GNV)",
        "outlier": "Preco fora de 1,5 IQR do produto no ano",
        "ano": "Ano, usado para particionar",
    },
    "dim_tempo": {
        "sk_tempo": "Chave yyyyMMdd",
        "data_coleta": "Data",
        "ano": "Ano",
        "semestre": "1 ou 2",
        "trimestre": "1 a 4",
        "mes": "1 a 12",
        "ano_mes": "yyyy-MM",
        "ano_eleitoral": "Ano com eleicao no Brasil",
    },
    "dim_produto": {
        "sk_produto": "Chave",
        "produto": "Nome do combustivel",
        "categoria": "GASOLINA, DIESEL, ETANOL ou GNV",
        "tipo_unidade": "LITRO ou M3",
    },
    "dim_localidade": {
        "sk_localidade": "Chave",
        "regiao_sigla": "N, NE, CO, SE, S",
        "regiao": "Nome da regiao",
        "estado_sigla": "UF",
        "municipio": "Municipio",
    },
    "dim_revenda": {
        "sk_revenda": "Chave",
        "cnpj_revenda": "CNPJ, somente digitos",
        "revenda": "Razao social",
        "bairro": "Bairro",
        "cep": "CEP",
        "municipio": "Municipio",
        "estado_sigla": "UF",
    },
    "dim_bandeira": {
        "sk_bandeira": "Chave",
        "bandeira": "Nome da bandeira",
        "bandeira_branca": "Posto sem bandeira",
    },
}
for t, cols in colunas.items():
    for c, d in cols.items():
        spark.sql(f"ALTER TABLE {gold}.{t} ALTER COLUMN {c} COMMENT '{d}'")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {gold}"))