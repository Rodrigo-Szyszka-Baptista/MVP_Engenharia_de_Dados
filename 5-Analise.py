# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 - Análise
# MAGIC
# MAGIC Respondendo as perguntas do objetivo. Nas comparações de preço uso só os
# MAGIC combustíveis vendidos por litro (o GNV fica de fora, é por m³).
# MAGIC
# MAGIC Nos gráficos deixei só gasolina, etanol e diesel. A aditivada anda junto com a gasolina
# MAGIC e o S10 junto com o diesel, então com 5 linhas o gráfico só ficava mais poluído.
# MAGIC Os valores de todos os produtos aparecem nas tabelas.

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA gold")

# mesmo estilo em todos os gráficos
plt.rcParams.update({
    "figure.dpi": 110,
    "font.size": 10,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "axes.titlelocation": "left",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.edgecolor": "#c3c2b7",
    "axes.labelcolor": "#52514e",
    "axes.grid": True,
    "axes.grid.axis": "y",
    "axes.axisbelow": True,
    "grid.color": "#e1e0d9",
    "grid.linewidth": 0.8,
    "xtick.color": "#52514e",
    "ytick.color": "#52514e",
    "legend.frameon": False,
})

# cada combustível tem sempre a mesma cor
cores = {"GASOLINA": "#2a78d6", "ETANOL": "#eb6834", "DIESEL": "#1baf7a"}
cinza = "#c3c2b7"

# azul = mais barato, vermelho = mais caro (usado nos mapas de calor)
divergente = LinearSegmentedColormap.from_list("div", ["#104281", "#6da7ec", "#f0efec", "#ec8a89", "#a82d2c"])

reais = mtick.FuncFormatter(lambda v, _: f"R$ {v:.2f}".replace(".", ","))
pct = mtick.FuncFormatter(lambda v, _: f"{v:.0f}%")
nomes_meses = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]

def fonte(fig):
    fig.text(0.01, 0.01, "Fonte: ANP - Série Histórica de Preços de Combustíveis", fontsize=8, color="#898781")

def legenda(ax, **kw):
    # legenda numa linha entre o título e o gráfico, pra não ficar em cima das linhas
    ax.legend(ncol=3, loc="lower left", bbox_to_anchor=(0, 1.0), borderaxespad=0.3, **kw)

def rotulo_fim(ax, x, y, texto):
    # nome da série no fim da linha
    ax.annotate(texto, (x, y), xytext=(6, 0), textcoords="offset points",
                va="center", fontsize=9, color="#0b0b0b")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS coletas,
# MAGIC        COUNT(DISTINCT cnpj_revenda) AS postos,
# MAGIC        COUNT(DISTINCT municipio) AS municipios,
# MAGIC        MIN(data_coleta) AS inicio,
# MAGIC        MAX(data_coleta) AS fim
# MAGIC FROM workspace.gold.vw_precos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Como evoluíram os preços de 2016 a 2026?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, produto, ROUND(AVG(preco_venda), 2) AS preco_medio
# MAGIC FROM workspace.gold.vw_precos
# MAGIC WHERE tipo_unidade = 'LITRO'
# MAGIC GROUP BY ano, produto
# MAGIC ORDER BY produto, ano

# COMMAND ----------

# MAGIC %md
# MAGIC Por ano a curva fica muito suave, então no gráfico uso a média de cada mês.

# COMMAND ----------

mensal = spark.sql("""
    SELECT ano_mes, produto, AVG(preco_venda) AS preco
    FROM vw_precos
    WHERE produto IN ('GASOLINA', 'ETANOL', 'DIESEL')
    GROUP BY ano_mes, produto
""").toPandas()
mensal["data"] = pd.to_datetime(mensal["ano_mes"])

fig, ax = plt.subplots(figsize=(11, 5))
for produto, cor in cores.items():
    d = mensal[mensal["produto"] == produto].sort_values("data")
    ax.plot(d["data"], d["preco"], color=cor, linewidth=2, label=produto.capitalize())
    ultimo = d.iloc[-1]
    ax.plot(ultimo["data"], ultimo["preco"], "o", color=cor, markersize=6, markeredgecolor="white", markeredgewidth=1.5)
    rotulo_fim(ax, ultimo["data"], ultimo["preco"], f"{produto.capitalize()}  R$ {ultimo['preco']:.2f}".replace(".", ","))

ax.set_title("Preço médio mensal por litro", pad=28)
ax.yaxis.set_major_formatter(reais)
ax.set_xlim(mensal["data"].min(), mensal["data"].max() + pd.Timedelta(days=500))
legenda(ax)
fonte(fig)
plt.show()

# COMMAND ----------

anual = spark.sql("""
    WITH a AS (
      SELECT ano, produto, AVG(preco_venda) AS preco
      FROM vw_precos
      WHERE produto IN ('GASOLINA', 'ETANOL', 'DIESEL')
      GROUP BY ano, produto
    )
    SELECT ano, produto, ROUND(preco, 2) AS preco,
           ROUND((preco / LAG(preco) OVER (PARTITION BY produto ORDER BY ano) - 1) * 100, 1) AS variacao_pct
    FROM a
    ORDER BY produto, ano
""").toPandas()
display(anual)

# COMMAND ----------

# variação de um ano pro outro, barras lado a lado
v = anual.dropna().pivot(index="ano", columns="produto", values="variacao_pct")[list(cores)]
larg = 0.26

fig, ax = plt.subplots(figsize=(11, 4.5))
for i, (produto, cor) in enumerate(cores.items()):
    ax.bar(v.index + (i - 1) * larg, v[produto], width=larg - 0.03, color=cor, label=produto.capitalize())

ax.axhline(0, color="#898781", linewidth=1)
ax.set_title("Variação do preço médio em relação ao ano anterior", pad=28)
ax.yaxis.set_major_formatter(pct)
ax.set_xticks(v.index)
legenda(ax)
fonte(fig)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Obs: 2026 só tem o primeiro semestre, então a média desse ano não é comparável com as outras.

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:**
# MAGIC Os combustíveis estão em crescente desde 2016, tendo um grande crescimento em 2021 e 2022. Em 2026 houve mais um aumento repentino, ocorrido pela guerra

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Existe muita variação de preço entre postos?
# MAGIC
# MAGIC Comparo postos da mesma cidade, no mesmo ano, porque comparar postos de estados
# MAGIC diferentes misturaria imposto e frete. Só cidades com pelo menos 5 postos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, estado_sigla, municipio,
# MAGIC        COUNT(DISTINCT cnpj_revenda) AS postos,
# MAGIC        ROUND(MIN(preco_venda), 2) AS minimo,
# MAGIC        ROUND(MAX(preco_venda), 2) AS maximo,
# MAGIC        ROUND(MAX(preco_venda) - MIN(preco_venda), 2) AS diferenca,
# MAGIC        ROUND(STDDEV(preco_venda) / AVG(preco_venda) * 100, 1) AS coef_variacao
# MAGIC FROM workspace.gold.vw_precos
# MAGIC WHERE produto = 'GASOLINA'
# MAGIC   AND ano = (SELECT MAX(ano) FROM workspace.gold.vw_precos)
# MAGIC GROUP BY ano, estado_sigla, municipio
# MAGIC HAVING COUNT(DISTINCT cnpj_revenda) >= 5
# MAGIC ORDER BY coef_variacao DESC
# MAGIC LIMIT 20

# COMMAND ----------

# diferença entre o posto mais caro e o mais barato de cada cidade, no último ano
cidades = spark.sql("""
    SELECT municipio, estado_sigla,
           MAX(preco_venda) - MIN(preco_venda) AS diferenca
    FROM vw_precos
    WHERE produto = 'GASOLINA'
      AND ano = (SELECT MAX(ano) FROM vw_precos)
    GROUP BY municipio, estado_sigla
    HAVING COUNT(DISTINCT cnpj_revenda) >= 5
""").toPandas()

mediana = cidades["diferenca"].median()

fig, ax = plt.subplots(figsize=(10, 4.5))
ax.hist(cidades["diferenca"], bins=30, color="#2a78d6", edgecolor="white", linewidth=1)
ax.axvline(mediana, color="#0b0b0b", linewidth=1)
ax.annotate(f"mediana: R$ {mediana:.2f}".replace(".", ","), (mediana, ax.get_ylim()[1] * 0.95),
            xytext=(6, 0), textcoords="offset points", fontsize=9, va="top")
ax.set_title("Diferença entre o posto mais caro e o mais barato da cidade (gasolina)")
ax.set_xlabel("Diferença por litro")
ax.set_ylabel("Cidades")
ax.xaxis.set_major_formatter(reais)
ax.yaxis.set_major_locator(mtick.MaxNLocator(integer=True))
fonte(fig)
plt.show()

# COMMAND ----------

# a variação dentro das cidades mudou com o tempo?
cv = spark.sql("""
    WITH c AS (
      SELECT ano, produto, municipio, estado_sigla,
             STDDEV(preco_venda) / AVG(preco_venda) * 100 AS cv,
             MAX(preco_venda) - MIN(preco_venda) AS diferenca
      FROM vw_precos
      WHERE produto IN ('GASOLINA', 'ETANOL', 'DIESEL')
      GROUP BY ano, produto, municipio, estado_sigla
      HAVING COUNT(DISTINCT cnpj_revenda) >= 5
    )
    SELECT ano, produto,
           ROUND(AVG(cv), 2) AS cv_medio,
           ROUND(AVG(diferenca), 2) AS diferenca_media
    FROM c
    GROUP BY ano, produto
    ORDER BY produto, ano
""").toPandas()
display(cv)

fig, ax = plt.subplots(figsize=(10, 4.5))
for produto, cor in cores.items():
    d = cv[cv["produto"] == produto]
    ax.plot(d["ano"], d["cv_medio"], color=cor, linewidth=2, marker="o", markersize=5,
            markeredgecolor="white", label=produto.capitalize())

ax.set_title("Coeficiente de variação médio dos preços dentro da mesma cidade", pad=28)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v:.1f}%"))
ax.set_ylim(bottom=0)
ax.set_xticks(sorted(cv["ano"].unique()))
legenda(ax)
fonte(fig)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:**
# MAGIC
# MAGIC Existe grande variações de preço de combustível entre as bandeiras, a mediana sendo R$1,16 por litro. Além disso, o coeficiente de variação é bem considerável, porém teve uma grande redução a partir de 2023

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quais estados têm os preços mais altos e mais baixos?

# COMMAND ----------

uf = spark.sql("""
    SELECT estado_sigla, regiao, ROUND(AVG(preco_venda), 2) AS preco_medio
    FROM vw_precos
    WHERE produto = 'GASOLINA'
      AND ano = (SELECT MAX(ano) FROM vw_precos)
    GROUP BY estado_sigla, regiao
    ORDER BY preco_medio
""").toPandas()
display(uf)

ultimo_ano = spark.sql("SELECT MAX(ano) FROM vw_precos").collect()[0][0]

media_br = spark.sql("""
    SELECT AVG(preco_venda) FROM vw_precos
    WHERE produto = 'GASOLINA' AND ano = (SELECT MAX(ano) FROM vw_precos)
""").collect()[0][0]

# COMMAND ----------

# gráfico de pontos: como os preços são próximos, barra começando no zero esconderia a diferença
fig, ax = plt.subplots(figsize=(8, 8))
y = range(len(uf))
ax.hlines(y, uf["preco_medio"].min() - 0.05, uf["preco_medio"], color="#e1e0d9", linewidth=1)
ax.plot(uf["preco_medio"], y, "o", color="#2a78d6", markersize=8, markeredgecolor="white", markeredgewidth=1.5)
ax.axvline(media_br, color="#898781", linewidth=1)
ax.annotate(f"média Brasil\nR$ {media_br:.2f}".replace(".", ","), (media_br, len(uf) - 1),
            xytext=(6, 0), textcoords="offset points", fontsize=9, color="#52514e", va="center")

ax.set_yticks(list(y))
ax.set_yticklabels(uf["estado_sigla"] + "  (" + uf["regiao"] + ")")
ax.xaxis.set_major_formatter(reais)
ax.grid(axis="x", visible=True)
ax.grid(axis="y", visible=False)
ax.set_title(f"Preço médio da gasolina por estado em {ultimo_ano}")
fonte(fig)
plt.tight_layout(rect=(0, 0.02, 1, 1))
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mais barato e mais caro em cada ano
# MAGIC WITH uf AS (
# MAGIC   SELECT ano, estado_sigla, AVG(preco_venda) AS preco
# MAGIC   FROM workspace.gold.vw_precos
# MAGIC   WHERE produto = 'GASOLINA'
# MAGIC   GROUP BY ano, estado_sigla
# MAGIC )
# MAGIC SELECT ano,
# MAGIC        MIN_BY(estado_sigla, preco) AS mais_barato,
# MAGIC        ROUND(MIN(preco), 2) AS menor_preco,
# MAGIC        MAX_BY(estado_sigla, preco) AS mais_caro,
# MAGIC        ROUND(MAX(preco), 2) AS maior_preco
# MAGIC FROM uf
# MAGIC GROUP BY ano
# MAGIC ORDER BY ano

# COMMAND ----------

# MAGIC %md
# MAGIC Pra ver se é sempre o mesmo estado que fica caro ou barato, comparo cada estado com a
# MAGIC média do Brasil em cada ano (em %). Assim o aumento geral dos preços não atrapalha.

# COMMAND ----------

dif_uf = spark.sql("""
    WITH uf AS (
      SELECT ano, estado_sigla, AVG(preco_venda) AS preco
      FROM vw_precos WHERE produto = 'GASOLINA'
      GROUP BY ano, estado_sigla
    ),
    br AS (
      SELECT ano, AVG(preco_venda) AS preco
      FROM vw_precos WHERE produto = 'GASOLINA'
      GROUP BY ano
    )
    SELECT uf.ano, uf.estado_sigla, (uf.preco / br.preco - 1) * 100 AS dif_pct
    FROM uf JOIN br ON uf.ano = br.ano
""").toPandas()

mapa = dif_uf.pivot(index="estado_sigla", columns="ano", values="dif_pct")
mapa = mapa.loc[mapa.mean(axis=1).sort_values().index]  # mais barato em cima
lim = max(abs(mapa.min().min()), abs(mapa.max().max()))

fig, ax = plt.subplots(figsize=(10, 9))
im = ax.imshow(mapa, cmap=divergente, norm=TwoSlopeNorm(0, -lim, lim), aspect="auto")
ax.set_xticks(range(len(mapa.columns)), mapa.columns)
ax.set_yticks(range(len(mapa.index)), mapa.index)
ax.grid(False)
ax.spines[:].set_visible(False)
ax.tick_params(length=0)
cb = fig.colorbar(im, ax=ax, shrink=0.6, format=pct)
cb.set_label("diferença para a média do Brasil no ano", color="#52514e")
cb.outline.set_visible(False)
ax.set_title("Gasolina: preço de cada estado comparado com a média do Brasil")
fonte(fig)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:**
# MAGIC
# MAGIC Acre tem se mostrado o estado mais constante em preços elevados, a região Norte em geral apresenta preços acima da média. Isso provavelmente deve ser ocasionado por maior custos de logística para acessar certas regiões ou impostos mais altos. 
# MAGIC
# MAGIC Minas Gerais é o estado com o menor preço atual, entretanto apenas a partir de 2023 os preços ficaram abaixo da média 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Qual bandeira tem os menores preços?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT bandeira,
# MAGIC        COUNT(DISTINCT cnpj_revenda) AS postos,
# MAGIC        ROUND(AVG(preco_venda), 3) AS preco_medio
# MAGIC FROM workspace.gold.vw_precos
# MAGIC WHERE produto = 'GASOLINA'
# MAGIC GROUP BY bandeira
# MAGIC HAVING COUNT(*) >= 1000
# MAGIC ORDER BY preco_medio

# COMMAND ----------

# MAGIC %md
# MAGIC A média nacional pode enganar: se uma bandeira está mais presente em estados baratos,
# MAGIC ela parece mais barata sem ser. Então comparo cada bandeira com a média do estado e ano
# MAGIC onde ela está. No gráfico ficam só as 12 bandeiras com mais coletas.

# COMMAND ----------

band = spark.sql("""
    WITH media_uf AS (
      SELECT estado_sigla, ano, AVG(preco_venda) AS media
      FROM vw_precos WHERE produto = 'GASOLINA'
      GROUP BY estado_sigla, ano
    )
    SELECT v.bandeira,
           COUNT(*) AS coletas,
           ROUND(AVG((v.preco_venda / m.media - 1) * 100), 2) AS dif_media_local_pct
    FROM vw_precos v
    JOIN media_uf m ON v.estado_sigla = m.estado_sigla AND v.ano = m.ano
    WHERE v.produto = 'GASOLINA'
    GROUP BY v.bandeira
    HAVING COUNT(*) >= 1000
    ORDER BY dif_media_local_pct
""").toPandas()
display(band)

# COMMAND ----------

top = band.nlargest(12, "coletas").sort_values("dif_media_local_pct", ascending=False)
cor_barra = ["#2a78d6" if x < 0 else "#e34948" for x in top["dif_media_local_pct"]]

fig, ax = plt.subplots(figsize=(9, 6))
ax.barh(top["bandeira"], top["dif_media_local_pct"], height=0.6, color=cor_barra)
ax.axvline(0, color="#898781", linewidth=1)
for i, x in enumerate(top["dif_media_local_pct"]):
    ax.annotate(f"{x:+.1f}%".replace(".", ","), (x, i), xytext=(5 if x >= 0 else -5, 0),
                textcoords="offset points", ha="left" if x >= 0 else "right", va="center", fontsize=9)

ax.set_title("Preço da gasolina por bandeira, comparado com a média do estado")
ax.set_xlabel("abaixo da média  ←     → acima da média")
ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v:+.1f}%".replace(".", ",")))
m = top["dif_media_local_pct"].abs().max() * 1.3
ax.set_xlim(-m, m)
ax.grid(axis="x", visible=True)
ax.grid(axis="y", visible=False)
fonte(fig)
plt.tight_layout(rect=(0, 0.02, 1, 1))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:**
# MAGIC
# MAGIC Era esperado que os menores preços seriam de bandeira branca ou de marcas não conhecidas, entretanto não estão tão abaixo de bandeiras com renome como a RAIZEN ou a Petrobras

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Relação entre etanol e gasolina
# MAGIC
# MAGIC O etanol compensa quando custa até 70% da gasolina.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH p AS (
# MAGIC   SELECT ano,
# MAGIC          AVG(CASE WHEN produto = 'ETANOL' THEN preco_venda END) AS etanol,
# MAGIC          AVG(CASE WHEN produto = 'GASOLINA' THEN preco_venda END) AS gasolina
# MAGIC   FROM workspace.gold.vw_precos
# MAGIC   GROUP BY ano
# MAGIC )
# MAGIC SELECT ano, ROUND(etanol, 2) AS etanol, ROUND(gasolina, 2) AS gasolina,
# MAGIC        ROUND(etanol / gasolina * 100, 1) AS relacao_pct
# MAGIC FROM p
# MAGIC ORDER BY ano

# COMMAND ----------

rel = spark.sql("""
    SELECT ano_mes,
           AVG(CASE WHEN produto = 'ETANOL' THEN preco_venda END) /
           AVG(CASE WHEN produto = 'GASOLINA' THEN preco_venda END) * 100 AS relacao
    FROM vw_precos
    GROUP BY ano_mes
""").toPandas()
rel["data"] = pd.to_datetime(rel["ano_mes"])
rel = rel.sort_values("data")

fig, ax = plt.subplots(figsize=(11, 4.5))
ax.axhspan(0, 70, color="#1baf7a", alpha=0.1, label="faixa em que o etanol compensa")
ax.axhline(70, color="#898781", linewidth=1, linestyle="--", label="limite de 70%")
ax.plot(rel["data"], rel["relacao"], color="#eb6834", linewidth=2, label="etanol / gasolina")

ax.set_title("Preço do etanol como % do preço da gasolina (média Brasil, por mês)", pad=28)
ax.yaxis.set_major_formatter(pct)
ax.set_ylim(rel["relacao"].min() - 5, rel["relacao"].max() + 3)
legenda(ax)
fonte(fig)
plt.show()

# COMMAND ----------

rel_uf = spark.sql("""
    WITH p AS (
      SELECT estado_sigla, ano_mes,
             AVG(CASE WHEN produto = 'ETANOL' THEN preco_venda END) /
             AVG(CASE WHEN produto = 'GASOLINA' THEN preco_venda END) AS relacao
      FROM vw_precos
      GROUP BY estado_sigla, ano_mes
    )
    SELECT estado_sigla,
           ROUND(AVG(relacao) * 100, 1) AS relacao_media_pct,
           ROUND(AVG(CASE WHEN relacao <= 0.7 THEN 1 ELSE 0 END) * 100, 1) AS pct_meses_compensa
    FROM p
    WHERE relacao IS NOT NULL
    GROUP BY estado_sigla
    ORDER BY pct_meses_compensa
""").toPandas()
display(rel_uf)

fig, ax = plt.subplots(figsize=(8, 8))
ax.barh(rel_uf["estado_sigla"], rel_uf["pct_meses_compensa"], height=0.6, color="#eb6834")
ax.set_title("% dos meses em que o etanol compensou, por estado")
ax.xaxis.set_major_formatter(pct)
ax.set_xlim(0, 100)
ax.grid(axis="x", visible=True)
ax.grid(axis="y", visible=False)
fonte(fig)
plt.tight_layout(rect=(0, 0.02, 1, 1))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:**
# MAGIC
# MAGIC Olhando apenas o gráfico de preço ao longo do tempo é perceptível que a maior parte do tempo etanol não compensa, entretanto acrescentando o gráfico por estado permite analisar melhor a competitividade do etanol

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Existe sazonalidade?
# MAGIC
# MAGIC Pra não confundir com o aumento ao longo dos anos, divido o preço de cada mês pela
# MAGIC média do próprio ano. 100 = igual à média do ano. Só entram anos com os 12 meses
# MAGIC (2026 fica de fora).

# COMMAND ----------

indice = spark.sql("""
    WITH mensal AS (
      SELECT ano, mes, produto, AVG(preco_venda) AS preco
      FROM vw_precos
      WHERE produto IN ('GASOLINA', 'ETANOL', 'DIESEL')
      GROUP BY ano, mes, produto
    ),
    anual AS (
      SELECT ano, produto, AVG(preco) AS media_ano
      FROM mensal
      GROUP BY ano, produto
      HAVING COUNT(*) = 12
    )
    SELECT m.ano, m.mes, m.produto, m.preco / a.media_ano * 100 AS indice
    FROM mensal m
    JOIN anual a ON m.ano = a.ano AND m.produto = a.produto
""").toPandas()

media_mes = indice.groupby(["produto", "mes"], as_index=False)["indice"].mean()
display(media_mes.pivot(index="mes", columns="produto", values="indice").round(2))

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 4.5))
ax.axhline(100, color="#898781", linewidth=1)
for produto, cor in cores.items():
    d = media_mes[media_mes["produto"] == produto].sort_values("mes")
    ax.plot(d["mes"], d["indice"], color=cor, linewidth=2, marker="o", markersize=5,
            markeredgecolor="white", label=produto.capitalize())

ax.set_title("Preço de cada mês em relação à média do ano (média dos anos completos)", pad=28)
ax.set_xticks(range(1, 13), nomes_meses)
ax.set_ylabel("índice (100 = média do ano)")
legenda(ax)
fonte(fig)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC A média dos anos pode esconder que cada ano foi diferente. Aqui o mesmo índice da
# MAGIC gasolina, ano por ano.

# COMMAND ----------

g = indice[indice["produto"] == "GASOLINA"].pivot(index="ano", columns="mes", values="indice")
lim = max(abs(g.min().min() - 100), abs(g.max().max() - 100))

fig, ax = plt.subplots(figsize=(10, 5))
im = ax.imshow(g, cmap=divergente, norm=TwoSlopeNorm(100, 100 - lim, 100 + lim), aspect="auto")
ax.set_xticks(range(12), nomes_meses)
ax.set_yticks(range(len(g.index)), g.index)
ax.grid(False)
ax.spines[:].set_visible(False)
ax.tick_params(length=0)
cb = fig.colorbar(im, ax=ax, shrink=0.8)
cb.set_label("índice (100 = média do ano)", color="#52514e")
cb.outline.set_visible(False)
ax.set_title("Gasolina: índice mensal em cada ano")
fonte(fig)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:**
# MAGIC
# MAGIC Entre os combustíveis, o etanol que apresenta uma possível sazionalidade com uma curva que visualmente se assemelha a uma senoide. Potêncialmente esse fato se faz devido a associação direta das estações do ano com a matéria prima do etanol, enquanto os combustíveis fosseis não possuem

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Anos eleitorais influenciam os preços?
# MAGIC
# MAGIC Comparo o segundo semestre (período de campanha) de anos eleitorais e não eleitorais.
# MAGIC Isso é só comparação, não prova causa: o preço também depende de petróleo, dólar e
# MAGIC política da Petrobras, que não estão nessa base.

# COMMAND ----------

sem = spark.sql("""
    WITH s AS (
      SELECT ano, ano_eleitoral, produto,
             AVG(CASE WHEN semestre = 1 THEN preco_venda END) AS s1,
             AVG(CASE WHEN semestre = 2 THEN preco_venda END) AS s2
      FROM vw_precos
      WHERE produto IN ('GASOLINA', 'ETANOL', 'DIESEL')
      GROUP BY ano, ano_eleitoral, produto
    )
    SELECT ano, ano_eleitoral, produto, ROUND((s2 / s1 - 1) * 100, 2) AS variacao_s1_s2_pct
    FROM s
    WHERE s1 IS NOT NULL AND s2 IS NOT NULL
    ORDER BY produto, ano
""").toPandas()
display(sem)

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(13, 4.5), sharey=True)
for ax, produto in zip(axes, cores):
    d = sem[sem["produto"] == produto]
    cor = [cores[produto] if e else cinza for e in d["ano_eleitoral"]]
    ax.bar(d["ano"], d["variacao_s1_s2_pct"], width=0.7, color=cor)
    ax.axhline(0, color="#898781", linewidth=1)
    ax.set_title(produto.capitalize(), fontsize=11)
    ax.set_xticks(d["ano"], d["ano"].astype(str).str[2:])

axes[0].yaxis.set_major_formatter(pct)
axes[0].set_ylabel("variação do 1º para o 2º semestre")
fig.suptitle("Variação do preço do 1º para o 2º semestre", x=0.01, y=1.04, ha="left", fontweight="bold", fontsize=13)
fig.text(0.01, 0.97, "Barras coloridas: anos eleitorais (2016, 2018, 2020, 2022, 2024). Cinza: outros anos.",
         fontsize=9, color="#52514e")
fonte(fig)
plt.tight_layout(rect=(0, 0.03, 1, 0.95))
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sem AS (
# MAGIC   SELECT ano, ano_eleitoral, produto,
# MAGIC          AVG(CASE WHEN semestre = 1 THEN preco_venda END) AS s1,
# MAGIC          AVG(CASE WHEN semestre = 2 THEN preco_venda END) AS s2
# MAGIC   FROM workspace.gold.vw_precos
# MAGIC   WHERE produto IN ('GASOLINA', 'ETANOL', 'DIESEL')
# MAGIC   GROUP BY ano, ano_eleitoral, produto
# MAGIC )
# MAGIC SELECT produto, ano_eleitoral,
# MAGIC        COUNT(*) AS anos,
# MAGIC        ROUND(AVG((s2 / s1 - 1) * 100), 2) AS variacao_media_pct,
# MAGIC        ROUND(STDDEV((s2 / s1 - 1) * 100), 2) AS desvio
# MAGIC FROM sem
# MAGIC WHERE s1 IS NOT NULL AND s2 IS NOT NULL
# MAGIC GROUP BY produto, ano_eleitoral
# MAGIC ORDER BY produto, ano_eleitoral

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta:**
# MAGIC
# MAGIC Anos eleitorais não afetam o preço do combustível, com apenas 2022 apresentando um comportamento de queda de preços.

# COMMAND ----------

# MAGIC %md
# MAGIC ## GNV
# MAGIC Separado, em R$/m³.

# COMMAND ----------

gnv = spark.sql("""
    SELECT ano_mes, AVG(preco_venda) AS preco, COUNT(DISTINCT cnpj_revenda) AS postos
    FROM vw_precos
    WHERE produto = 'GNV'
    GROUP BY ano_mes
""").toPandas()
gnv["data"] = pd.to_datetime(gnv["ano_mes"])
gnv = gnv.sort_values("data")

fig, ax = plt.subplots(figsize=(11, 4))
ax.plot(gnv["data"], gnv["preco"], color="#2a78d6", linewidth=2)
ax.set_title("GNV: preço médio mensal por m³")
ax.yaxis.set_major_formatter(reais)
fonte(fig)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outliers mudam alguma conclusão?
# MAGIC Média com e sem os outliers marcados na silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT produto,
# MAGIC        ROUND(AVG(preco_venda), 3) AS com_outliers,
# MAGIC        ROUND(AVG(CASE WHEN NOT outlier THEN preco_venda END), 3) AS sem_outliers
# MAGIC FROM workspace.gold.vw_precos
# MAGIC WHERE tipo_unidade = 'LITRO'
# MAGIC GROUP BY produto
# MAGIC ORDER BY produto