CREATE KEYSPACE local WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '4'}  AND
durable_writes = true;

USE local;

create table local.dummy
(
	num_cpf_cnpj text,
	mes_ano_lancamento int,
	cod_chav_lancamento text,
	dat_autr date,
 	codigo_moeda_origem text,
	valor double,
	PRIMARY KEY ( num_cpf_cnpj,mes_ano_lancamento,cod_chav_lancamento )
)
with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
	and compaction = {'max_threshold': '32', 'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
	and compression = {'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'chunk_length_in_kb': '64'};