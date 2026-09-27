# MVP Engenharia de Dados - Preços de Combustíveis no Brasil

Pipeline de dados no Databricks com os preços de combustíveis automotivos publicados pela ANP, de 2016 a 2026.

Autor: Rodrigo Szyszka Baptista

## 1. Contexto de Negócio e Perguntas

O preço do combustível muda muito dependendo de onde, quando e em qual posto a pessoa abastece. A ANP faz uma pesquisa semanal de preços em postos do país inteiro e publica os resultados em arquivos CSV semestrais.

A ideia do trabalho é auxiliar no planejamento logístico, permitindo estudo de rotas, assim como planejamento de abertura de franquias.

Perguntas:

1. Como evoluíram os preços dos combustíveis de 2016 a 2026?
2. Existe muita variação de preço entre postos da mesma cidade?
3. Quais estados têm os preços médios mais altos e mais baixos?
4. Qual bandeira tende a ter os menores preços?
5. Qual a relação histórica entre o preço do etanol e da gasolina?
6. Existe sazonalidade nos preços ao longo do ano?
7. Anos eleitorais influenciam os preços?

### Dados

- Fonte: ANP - Série Histórica de Preços de Combustíveis
- Link: https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis
- Arquivos: 21 CSVs semestrais, de 2016 a 2026 (série "Automotivos")
- Formato: CSV separado por `;`, UTF-8, preço com vírgula decimal

Licença: são dados abertos do governo federal, publicados pela ANP no portal de dados abertos.

Cada linha é uma coleta de preço: um posto, um combustível, uma data, com o preço de venda (e de compra, até 2020). Tem também o endereço completo do posto, a bandeira e a região/UF/município — o catálogo completo de colunas está em [docs/catalogo_dados.md](docs/catalogo_dados.md).

## 2. Carga dos Dados

Baixei os 21 arquivos no site da ANP e subi num volume do Unity Catalog (`workspace.default.dados_anp`), pelo Catalog Explorer. Tentei primeiro pelo "Create Table", mas ele só aceita 10 arquivos por vez, então subi direto no volume em lotes.

O notebook [1-Bronze](notebooks/1-Bronze.py) lê todos os CSVs do volume de uma vez. Antes disso ele confere se todos os arquivos têm o mesmo cabeçalho, porque se algum ano tivesse colunas diferentes o Spark ia desalinhar os dados sem dar erro.

## 3. Modelagem e Catálogo de Dados

Organizei em três camadas (arquitetura medalhão), cada uma em um schema do catálogo `workspace`:

- **bronze**: dados como vieram dos CSVs, tudo em texto
- **silver**: dados limpos e com tipos corretos
- **gold**: modelo estrela


A fato tem uma linha por posto + produto + data, com o preço de venda. Escolhi o esquema estrela porque os dados têm um evento que se repete milhões de vezes (a coleta de preço) e poucas informações descritivas que se repetem muito (posto, cidade, produto, bandeira). Na estrela esses textos ficam uma vez só nas dimensões.

O catálogo com todas as tabelas e colunas está em [docs/catalogo_dados.md](docs/catalogo_dados.md). As descrições também foram colocadas no Unity Catalog pelos próprios notebooks.

## 4. Pipeline de Dados

Separei em um notebook por etapa:

- [1-Bronze](notebooks/1-Bronze.py): cria os schemas, lê os CSVs e grava a bronze
- [2-Qualidade](notebooks/2-Qualidade.py): levanta os problemas dos dados
- [3-Silver](notebooks/3-Silver.py): limpa, converte tipos e grava a silver
- [4-Gold](notebooks/4-Gold.py): monta as dimensões e a fato
- [5-Analise](notebooks/5-Analise.py): consultas que respondem as perguntas

Principais transformações na silver:

- Preço de texto com vírgula para número
- Data de texto para date
- CNPJ e CEP só com dígitos
- Texto em maiúsculo e sem espaço sobrando
- Remoção de linhas sem preço/data/produto/UF, produtos fora da lista e preços fora de R$ 0,50 a R$ 20
- Uma linha por posto + produto + data
- Coluna `tipo_unidade` pra separar o GNV (m³) dos outros (litro)
- Coluna `outlier` marcando preços fora de 1,5 IQR por produto e ano
- Colunas de ano, mês, semestre e ano eleitoral

A silver e a fato são particionadas por ano.

## 5. Qualidade de Dados

O notebook [2-Qualidade](notebooks/2-Qualidade.py) verifica completude, consistência, unicidade, acurácia e outliers antes da limpeza.

O que encontrei:

- **CNPJ com espaço na frente** em todos os registros (`" 01.492.748/0003-83"`). Sem tratar, o mesmo posto aparece como dois diferentes ao agrupar.
- **GNV em R$/m³** na mesma coluna dos outros combustíveis, que são em R$/litro. Misturar os dois numa média não faz sentido, então criei a coluna `tipo_unidade` e deixei o GNV fora das comparações.
- **Valor de compra vazio**: a ANP parou de publicar esse campo em 2020. Mantive, mas não uso.

A quantidade de registros depois de cada etapa da limpeza fica salva em `silver.log_limpeza`.

## 6. Análise de Dados

As consultas e os gráficos estão no notebook [5-Analise](notebooks/5-Analise.py). Nas comparações de preço usei só os combustíveis vendidos por litro (o GNV é separado, fica em m³). Nos gráficos com linhas por produto deixei só gasolina, etanol e diesel — a aditivada segue a gasolina e o S10 segue o diesel, então entrar com os 5 só deixava mais poluído. Os valores de todos os produtos continuam nas tabelas.

### 1. Evolução dos preços

<img width="1120" height="562" alt="image" src="https://github.com/user-attachments/assets/01dad66f-b0fa-4d01-a425-aa9df96039b9" />

<img width="1098" height="514" alt="image" src="https://github.com/user-attachments/assets/615cee6d-7c18-4bf0-8e29-1e45054ddf68" />


**Resposta:** Os combustíveis estão em crescente desde 2016, tendo um grande crescimento em 2021 e 2022. Em 2026 houve mais um aumento repentino, provavelmente devido a guerra, entretanto com os dados atuais se torna impossível criar certezas sobre motivos de mudanças de preços

### 2. Variação entre postos

<img width="1013" height="479" alt="image" src="https://github.com/user-attachments/assets/a9e79e1f-49ae-4fd7-bac5-7768d6238e93" />

<img width="1001" height="514" alt="image" src="https://github.com/user-attachments/assets/ac30b13b-f60d-4674-b9b5-8e29c50a2cb5" />


**Resposta:** Existe grande variações de preço de combustível entre as bandeiras, a mediana sendo R$1,16 por litro. Além disso, o coeficiente de variação é bem considerável, porém teve uma grande redução a partir de 2023.

### 3. Estados

<img width="876" height="879" alt="image" src="https://github.com/user-attachments/assets/56ec91e4-112d-481d-88e7-180281d6ebf7" />

<img width="968" height="911" alt="image" src="https://github.com/user-attachments/assets/9361ad24-c266-4fa9-b232-2b59f4056cde" />


**Resposta:** Acre tem se mostrado o estado mais constante em preços elevados, a região Norte em geral apresenta preços acima da média. Isso provavelmente deve ser ocasionado por maior custos de logística para acessar certas regiões ou impostos mais altos.

Minas Gerais é o estado com o menor preço atual, entretanto apenas a partir de 2023 os preços ficaram abaixo da média.

### 4. Bandeiras

<img width="1047" height="661" alt="image" src="https://github.com/user-attachments/assets/a9631267-6997-45d8-af96-247a13c11058" />


**Resposta:** Era esperado que os menores preços seriam de bandeira branca ou de marcas não conhecidas, entretanto não estão tão abaixo de bandeiras com renome como a RAIZEN ou a Petrobras.

### 5. Etanol x gasolina

<img width="1098" height="514" alt="image" src="https://github.com/user-attachments/assets/4bbfe08c-3d75-40fe-b00f-5e8040daec42" />

<img width="876" height="879" alt="image" src="https://github.com/user-attachments/assets/b0fb60a9-680e-4ae2-98f1-ae2d478649f5" />


**Resposta:** Olhando apenas o gráfico de preço ao longo do tempo é perceptível que a maior parte do tempo etanol não compensa, entretanto acrescentando o gráfico por estado permite analisar melhor a competitividade do etanol.

### 6. Sazonalidade

<img width="1001" height="514" alt="image" src="https://github.com/user-attachments/assets/f1070a5f-5be9-4a6b-9f67-bf0ac5d867da" />

<img width="951" height="527" alt="image" src="https://github.com/user-attachments/assets/8df36baa-506e-4e7b-a526-758c264f7ea7" />


**Resposta:** Entre os combustíveis, o etanol que apresenta uma possível sazonalidade com uma curva que visualmente se assemelha a uma senoide. Potencialmente esse fato se faz devido a associação direta das estações do ano com a matéria prima do etanol, enquanto os combustíveis fósseis não possuem.

### 7. Anos eleitorais

<img width="1421" height="534" alt="image" src="https://github.com/user-attachments/assets/99b8ea1a-464d-41b8-826f-659a88592f1e" />


**Resposta:** Anos eleitorais não afetam o preço do combustível, com apenas 2022 apresentando um comportamento de queda de preços.


## 7. Autoavaliação

Os dados escolhidos foram de grande qualidade, com formatação padronizada, o que facilitou bastante o processo de limpeza e organização. O objetivo de conseguir mapear os melhores e piores lugares de planejamento de rotas foi realizado com sucesso. Entretanto, foi observado que apenas os dados de preço não são capazes de trazer todas as respostas buscadas.

Para a compreensão completa dos dados seria necessário complementar com dados de impostos e conhecimento dos acontecimentos que ocasionaram as mudanças de preço.

Alguns gráficos se provaram difíceis de mostrar os dados da forma que eu queria, precisando buscar mais e mais formas, ainda não ficando da forma exata em que eu queria.

## Como rodar

1. Criar o volume `dados_anp` em `workspace.default`
2. Subir os CSVs da ANP no volume
3. Importar os notebooks da pasta `notebooks`
4. Rodar na ordem: 1, 2, 3, 4, 5
