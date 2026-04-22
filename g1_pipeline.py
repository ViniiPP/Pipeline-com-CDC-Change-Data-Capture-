from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
import json
import logging
import redis


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CDCChangeEventProcessor(MapFunction):

    # processa os eventos de mudança lidos do Kafka vindos do PostgreSQL
    def map(self, value):
        try:
            change_event = json.loads(value)
    
            if 'payload' in change_event:
                change_event = change_event['payload']
            
            op = change_event.get('op')
            op_name = {
                'c': 'INSERT',
                'u': 'UPDATE',
                'd': 'DELETE',
                'r': 'READ (Snapshot)'
            }.get(op, f'UNKNOWN ({op})')
            
            before = change_event.get('before')
            after = change_event.get('after')
            
            source = change_event.get('source', {})
            table = source.get('table', 'unknown')
            schema = source.get('schema', 'unknown')
            
            logger.info(f"Evento CDC recebido: {op_name} na tabela {schema}.{table}")
            
            if op == 'c':
                logger.info(f"   Novo registro inserido: {after}")
            elif op == 'u':
                logger.info(f"   Registro atualizado:")
                logger.info(f"      Antes: {before}")
                logger.info(f"      Depois: {after}")
            elif op == 'd':
                logger.info(f"   Registro apagado: {before}")
            
            # novo dicionário mais limpo para passar adiante no Flink
            return json.dumps({
                'op': op,
                'op_name': op_name,
                'before': before,
                'after': after,
                'raw_event': change_event
            })
            
        except Exception as e:
            logger.error(f"Erro ao tentar processar o evento CDC: {str(e)}")
            return None

class RedisSinkMapper(MapFunction):
    # recebe os eventos já processados e salva em um Stream dentro do Redis (Valkey)
    def open(self, runtime_context): 
        self.r = redis.Redis(host='valkey', port=6379, db=0) 
        logger.info("Conexão com o Redis aberta com sucesso.") 
    
    def map(self, value): 
        event_data = json.loads(value) 
        op = event_data.get('op') 
        after = event_data.get('after') 
        before = event_data.get('before') 
        
        # O xadd grava os dados em um streamm do redis chamado cdc_redis_stream
        if op == 'c': # se for um INSERT
            id = after.get('id') 
            name = after.get('name') 
            self.r.xadd("cdc_redis_stream", {"id": id, "name": name, "operation" : "INSERT"}) 
            logger.info(f"Gravado INSERT no Redis para ID {id}")

        elif op == 'u': # se for um UPDATE
            id = after.get('id') 
            name = after.get('name') 
            old_name = before.get('name') 
            self.r.xadd("cdc_redis_stream", {"id": id, "name": name, "old_name": old_name, "operation" : "UPDATE"}) 
            logger.info(f"Gravado UPDATE no Redis para ID {id}")
        
        elif op == 'd': # se for um DELETE
            id = before.get('id') 
            self.r.xadd("cdc_redis_stream", {"id": id, "operation": "DELETE"}) 
            logger.info(f"Gravado DELETE no Redis para ID {id}")
            
        return value 
    
    def close(self): 
        # fecha a conexão de forma segura quando o processamento for encerrado
        if hasattr(self, 'r'): 
            self.r.close() 
            logger.info("Conexão com o Redis fechada.") 
            
def create_flink_cdc_job():

    # cria a esteira (Job) do Flink, unindo quem lê (Kafka), quem processa e quem salva (Redis).
    env = StreamExecutionEnvironment.get_execution_environment() 
    
    env.add_jars("file:///opt/flink/flink-sql-connector-kafka-1.17.1.jar")
    env.set_parallelism(1) 

    kafka_consumer = FlinkKafkaConsumer( 
        topics="postgres.bigdata.cdc.public.teste", 
        deserialization_schema=SimpleStringSchema(), 
        properties={ 
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'flink-cdc-consumer-v2', 
            'auto.offset.reset': 'latest',
            'enable.auto.commit': 'true' 
        } 
    ) 

    # lê a origem do Kafka
    cdc_stream = env.add_source(kafka_consumer) 
    
    # processa cada mensagem
    processed_stream = cdc_stream.map(CDCChangeEventProcessor()) 
    
    # filtra mensagens nulas/vazias que deram erro
    filtered_stream = processed_stream.filter(lambda x: x is not None) 

    # envia o resultado para o destino final
    filtered_stream.map(RedisSinkMapper()) 

    return env 

if __name__ == '__main__':
    create_flink_cdc_job().execute("Job Flink - Pipeline CDC G1")
