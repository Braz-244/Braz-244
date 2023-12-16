- 👋 Hi, I’m @Braz-244
- 👀 I’m interested in ...
- 🌱 I’m currently learning ...
- 💞️ I’m looking to collaborate on ...
- 📫 How to reach me ...

<!---
Braz-244/Braz-244 is a ✨ special ✨ repository because its `README.md` (this file) appears on your GitHub profile.
You can click the Preview link to take a look at your changes.
--->
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuração do Spark
spark = SparkSession.builder.appName("TratamentoDados").getOrCreate()

# Dados fictícios - Substitua isso pelos seus dados reais
dados = [("USD", 5.30, "2023-01-01"),
         ("EUR", 6.20, "2023-01-01"),
         ("GBP", 7.50, "2023-01-01"),
         ("JPY", 0.049, "2023-01-01")]

# Criar DataFrame Spark a partir dos dados
schema = ["Moeda", "Valor", "Data"]
df_spark = spark.createDataFrame(dados, schema=schema)

# Função para tratar os dados no Spark
def tratar_dados_spark(dataframe_spark):
    # Ordenar por valor
    dataframe_spark = dataframe_spark.orderBy("Valor")

    # Adicionar uma coluna de classificação
    dataframe_spark = dataframe_spark.withColumn("Classificacao", col("Valor").rank())

    return dataframe_spark

# Aplicar a função aos dados do Spark
df_spark_tratado = tratar_dados_spark(df_spark)

# Exibir os dados tratados no Spark
df_spark_tratado.show()

# Salvar os dados tratados em um formato que o Power BI pode ler (por exemplo, CSV)
caminho_saida = "caminho/para/saida/dados_tratados.csv"
df_spark_tratado.write.option("header", "true").csv(caminho_saida)
