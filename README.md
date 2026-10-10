# Proyecto 4 · Big Data urbano (Valencia)



## Descripción y objetivo

Pipeline completo de ingeniería de datos (batch + streaming) sobre datos abiertos de tráfico, calidad del aire y meteorología de Valencia. Los datos se cruzan con Spark y el proyecto termina con dos modelos predictivos: uno principal de NO₂ horario y uno secundario de intensidad de tráfico.

## Arquitectura
 
### Vía batch
 
```mermaid
graph LR
    A["Histórico de aire 2016-2022 (carga única)<br/>y recolector (recogida continua)"] --> B[Data Lake]
    B --> C["Spark<br/>Limpieza, cruce por hora y zona,<br/>indicadores"]
    C --> D[Power BI]
    C --> E[Modelos predictivos]
```
 
### Vía streaming
 
```mermaid
graph LR
    A["Estado del tráfico real (capa 192)<br/>y simulador del histórico recogido"] --> B[Kafka]
    B --> C[Spark Structured Streaming]
    C --> D[(Cassandra)]
    D --> E[Grafana]
```
 
