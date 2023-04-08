# Spark Structured Streaming + Delta + MinIO

## Arquitetura 

![image](https://tarn-cert.s3.amazonaws.com/desafio/sparkStream_Delta_MinIO.png)

### Objetivo:
Configurar os recursos `Apache Spark Structured Streaming, Delta, MinIO` em um ambiente local

### Experimento:
Executar conexão do `Apache Spark` com o `MinIO` e realizar pelo menos uma etapa de ingestão

### Procedimentos:
1. Read Stream com Apache Spark em um bucket `raw` no MinIO 
2. Write Stream com Apache Spark em um bucket `bronze` no MinIO

### Camada Raw
Contém arquivos no formato `Json`, tendo sua leitura de dados através do `readStream` do Apache Spark

### Camada Bronze
Contém arquivos no formato `Delta`,  tendo sua escrita de dados através do `writeStream` do Apache Spark