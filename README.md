# Projeto Universitário - Pipeline CDC (Change Data Capture)

**Integrantes do Grupo:**
- Vinícius Pereira Polli

## Sobre o Projeto
Este projeto implementa um pipeline de dados utilizando a arquitetura de Change Data Capture (CDC). O fluxo consiste em capturar alterações realizadas no banco de dados PostgreSQL, transmiti-las via Apache Kafka (com auxílio do Debezium via Kafka Connect), processá-las em tempo real utilizando Apache Flink (PyFlink), e finalmente armazenar os dados processados no Redis (Valkey).

## Tecnologias Utilizadas
- **PostgreSQL**: Banco de dados relacional que atua como origem (Source) dos dados.
- **Debezium**: Ferramenta de CDC que lê as mudanças no log de transações do banco e as envia para o Kafka.
- **Apache Kafka & KafBat UI**: Message broker responsável por intermediar a entrega dos eventos (Broker).
- **Apache Flink (PyFlink)**: Motor de processamento de streams responsável por ler as mensagens, transformar e rotear os eventos em tempo real (Stream Processing).
- **Redis (Valkey)**: Banco de dados em memória que armazena o resultado final do pipeline em uma estrutura de fluxo contínuo (Sink).

## Estrutura do Repositório
- `g1_pipeline.py`: Script em Python (PyFlink) para o processamento dos dados lidos do Kafka e envio ao Redis.
- `postgres_setup.sql`: Comandos SQL necessários para criar a tabela, gerar os dados sintéticos, configurar o usuário de replicação e criar a Publication.
- `debezium_config.json`: Arquivo com a configuração do conector PostgreSQL do Debezium usado no KafBat UI.
- `outputs/`: Diretório que contém as evidências de execução da atividade, como capturas de tela e GIFs do funcionamento do pipeline.

## Sobre os Dados Gerados
Foram gerados dados sintéticos simulando o registro de novos usuários. No banco de dados, eles estão organizados em 4.000 linhas na tabela `public.teste` contendo a seguinte estrutura:
- `id` (INT): Identificador único e chave primária da tabela, gerado de forma incremental.
- `name` (STRING): Nome do usuário gerado sinteticamente (ex: `Usuario_Fake_1`).
- `year` (DATE): Uma data gerada aleatoriamente nos últimos 10 anos em relação ao dia da execução do script.

## Como Rodar o Projeto

Siga o passo a passo abaixo para levantar toda a infraestrutura e colocar o pipeline em funcionamento na sua máquina local.

### Passo 1: Subir a Infraestrutura (Docker)
Certifique-se de ter o Docker instalado e rodando. No diretório raiz do projeto, execute:
```bash
docker compose up -d
```
Isso fará o download e iniciará os containers: PostgreSQL, Kafka, Kafka Connect, KafBat UI, Flink (JobManager e TaskManager), Valkey e Redis Insight.

### Passo 2: Configurar o PostgreSQL e Gerar Dados
Acesse o terminal do container do banco de dados e rode o script SQL `postgres_setup.sql` (ou utilize uma ferramenta como pgAdmin/DBeaver) para preparar a tabela e inserir os 4.000 registros sintéticos.
```bash
docker exec -i postgres psql -U postgres -d bigdata < postgres_setup.sql
```

### Passo 3: Criar o Conector do Debezium (Kafka Connect)
1. Abra o **KafBat UI** em `http://localhost:8080`
2. Vá em **Kafka Connect** no menu lateral, escolha o seu cluster e clique em **Create New Connector**.
3. Copie o conteúdo do arquivo `debezium_config.json` e cole na área de configuração.
4. Salve. O Debezium começará a escutar o banco e mandará as 4.000 linhas iniciais para o tópico `postgres.bigdata.cdc.public.teste`.

### Passo 4: Rodar a Esteira no Apache Flink
1. Copie o arquivo do pipeline para dentro do container do Flink:
   ```bash
   docker cp g1_pipeline.py flink-jobmanager:/opt/flink/
   ```
2. Acesse o container do Flink JobManager:
   ```bash
   docker exec -it flink-jobmanager bash
   ```
3. Submeta o Job Python para o cluster Flink:
   ```bash
   flink run -m localhost:8081 -py /opt/flink/g1_pipeline.py
   ```
4. Opcional: Acesse o **Flink Dashboard** (`http://localhost:8081`) e verifique se o Job "Pipeline CDC G1" consta como **RUNNING**.

### Passo 5: Testar o Pipeline e Conferir no Redis
1. Abra o **Redis Insight** em `http://localhost:5540` e adicione a conexão preenchendo o host como `valkey` e porta `6379`.
2. Em um terminal do seu computador, execute um comando de `UPDATE` ou `DELETE` no banco de dados para gerar um novo evento:
   ```bash
   docker exec postgres psql -U postgres -d bigdata -c "UPDATE public.teste SET name='Teste do redis' WHERE id=800;"
   ```
3. Volte no Redis Insight, clique em atualizar: a chave `cdc_redis_stream` aparecerá na lista de Stream Data, contendo todos os dados processados em tempo real pelo Apache Flink.
