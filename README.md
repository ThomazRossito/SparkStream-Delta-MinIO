# Spark Structured Streaming + Delta + MinIO

## Arquitetura 

![image](https://tarn-cert.s3.amazonaws.com/desafio/sparkStream_Delta_MinIO.png)

### Objetivo:
Configurar os recursos `Apache Spark Structured Streaming, Delta, MinIO` em um ambiente local

### Recursos utilizados:
1. **Sistema operacional - `macOS Ventura`**
2. **Apache Spark - `v3.3.2`** [link](https://spark.apache.org/downloads.html)
3. **Delta Lake - `v2.3.0`** [link](https://delta.io/)
4. **MinIO - `RELEASE.2023-04-07T05-28-58Z`** [link](https://min.io/)

### Experimento:
Executar conexão do `Apache Spark` com o `MinIO` e realizar pelo menos uma etapa de ingestão

### Procedimentos:
1. Read Stream com Apache Spark em um bucket `raw` no MinIO 
2. Write Stream com Apache Spark em um bucket `bronze` no MinIO

### Camada Raw
Contém arquivos no formato `Json`, tendo sua leitura de dados através do `readStream` do Apache Spark

### Camada Bronze
Contém arquivos no formato `Delta`,  tendo sua escrita de dados através do `writeStream` do Apache Spark


## Contido nos arquivos 

## raw2Bronze.py

1. Step: import Lib functions
2. Step: Spark Session
3. Step: set log level
4. Step: Shuffle Spark
5. Step: Paths
6. Step: Dataframe Spark Read Strem: Json - Raw
7. Step: Dataframe Spark Write Strem: Delta - Bronze
8. Step: Optimize - Bronze


## functions.py

1. import Libs
2. Functions: session_spark
3. Functions: Schema Evolution
4. Functions: Read Stream format Json
5. Functions: Write Stream format Delta
6. Functions: Tranformation rawToBronze
7. Functions: Optimize Path Delta



### Desenvolvido por:

> ## Thomaz Antonio Rossito Neto 
> #### **Master Data Specialist @ CI&T** [link](https://ciandt.com/br/) </code> 
> #### **Linkedin:** <a href="https://www.linkedin.com/in/thomaz-antonio-rossito-neto/"> Thomaz A. Rossito Neto </a> </code>
> #### **GitHub:** <a href="https://github.com/ThomazRossito"> github.com/ThomazRossito </a> </code>


### Certificações e Credenciais:

[Databricks](https://credentials.databricks.com/profile/thomazantoniorossitoneto39867/wallet#gs.in4ak9) <br>
[AWS](https://www.credly.com/users/thomaz-antonio-rossito-neto/badges) <br>
[AZURE](https://www.credly.com/users/thomaz-antonio-rossito-neto/badges) <br>
[AirFlow](https://www.credly.com/users/thomaz-antonio-rossito-neto/badges) <br>