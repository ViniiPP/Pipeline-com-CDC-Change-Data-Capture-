-- configurar o ambiente do PostgreSQL

-- criação de um usuário exclusivo para replicação e leitura pelo Debezium
CREATE ROLE rep_user REPLICATION LOGIN PASSWORD 'rep_pass';

-- garantir acesso da role ao banco bigdata
GRANT ALL PRIVILEGES ON DATABASE bigdata TO rep_user;

-- criação da tabela destino dos dados
CREATE TABLE public.teste (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    year DATE
);

-- garantir acesso da role na tabela criada
GRANT ALL PRIVILEGES ON TABLE public.teste TO rep_user;

-- modificar a forma de armazenamento no log de réplica para registrar a linha inteira antes da modificação
ALTER TABLE public.teste REPLICA IDENTITY FULL;

-- criação do Publication
CREATE PUBLICATION dbz_publication FOR TABLE public.teste;

-- Inserção de dados sintéticos iniciais: 4.000 linhas com user fake e ano aleatório

INSERT INTO public.teste (name, year)
SELECT 
    'Usuario_Fake_' || generate_series(1, 4000),
    CURRENT_DATE - (random() * 3650)::integer;



-- EVIDÊNCIAS DE ALTERAÇÃO E EXCLUSÃO (CDC)
-- Exemplo de comando executado no terminal para acionar um evento de UPDATE no fluxo
UPDATE public.teste 
SET name = 'Teste do redis' 
WHERE id = 800;

-- Exemplo de comando executado no terminal para acionar um evento de DELETE no fluxo
DELETE FROM public.teste 
WHERE id = 900;
