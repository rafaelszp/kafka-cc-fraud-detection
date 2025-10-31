#!/usr/bin/fish

## Criando um topico com compactação
### $PARTITIONS partições caso precise de $PARTITIONS clients simultâneos sem necessitar de rebalancear
### replication factor 3 para tolerar falha de 2 brokers (em dev local, somente 1)
### Represtenta o saldo atual que será gerenciado em uma KTable
set KAFKA localhost:9092
set REPLICATION_FACTOR 1
set PARTITIONS 10


kafka-topics.sh --bootstrap-server $KAFKA --delete \
--topic account-transactions || echo "Tópicos não existiam"


kafka-topics.sh --bootstrap-server $KAFKA --delete \
--topic account-transactions-by-account || echo "Tópicos não existiam"

kafka-topics.sh --bootstrap-server $KAFKA --delete \
--topic account-balance || echo "Tópicos não existiam"

kafka-topics.sh --bootstrap-server $KAFKA --delete \
--topic processed-account-transactions || echo "Tópicos não existiam"


kafka-topics.sh --bootstrap-server $KAFKA --create \
--topic account-balance \
--partitions $PARTITIONS \
--replication-factor $REPLICATION_FACTOR \
--config cleanup.policy=compact


# Criando tópico sem compactação
kafka-topics.sh --bootstrap-server $KAFKA --create \
--topic account-transactions \
--partitions $PARTITIONS \
--replication-factor $REPLICATION_FACTOR \
--config cleanup.policy=delete \
--config retention.ms=604800000 # 7 dias

kafka-topics.sh --bootstrap-server $KAFKA --create \
--topic account-transactions-by-account \
--partitions $PARTITIONS \
--replication-factor $REPLICATION_FACTOR \
--config cleanup.policy=delete \
--config retention.ms=604800000 # 7 dias


# Criando tópico sem compactação
kafka-topics.sh --bootstrap-server $KAFKA --create \
--topic processed-account-transactions \
--partitions $PARTITIONS \
--replication-factor $REPLICATION_FACTOR \
--config cleanup.policy=delete \
--config retention.ms=604800000 # 7 dias