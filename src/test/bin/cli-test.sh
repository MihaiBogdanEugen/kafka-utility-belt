#!/usr/bin/env bash

set -o nounset \
    -o verbose \
    -o xtrace

ps ax | grep -i 'de.mbe1224.utils.kafka.infrastructure.EmbeddedKafkaCluster' | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM

./gradlew clean build shadowJar

export JAAS_CONF=/tmp/jaas.conf
export MINI_KDC_DIR=/tmp/minikdc

rm -rf "$MINI_KDC_DIR" && mkdir -p "$MINI_KDC_DIR"

java -cp build/libs/kub.jar \
    de.mbe1224.utils.kafka.infrastructure.EmbeddedKafkaCluster \
    3 \
    3 \
    true \
    /tmp/client.properties \
    "$JAAS_CONF" \
    "$MINI_KDC_DIR" &>/tmp/test-kafka-cluster.log &

KAFKA_PID=$!

# Wait for Kafka to get ready. This is required because ZK Client fails (with AuthFailed event) if the MiniKDC is not up.
sleep 10

echo "Kafka : $KAFKA_PID"

java -Djava.security.auth.login.config="$JAAS_CONF" \
     -Djava.security.krb5.conf="$MINI_KDC_DIR/krb5.conf" \
     -cp build/libs/kub.jar \
     de.mbe1224.utils.kafka.infrastructure.cli.ZooKeeperReadyCommand \
     localhost:11117 \
     30000 &> /tmp/test-zookeeper-ready.log

ZOOKEEPER_TEST=$([ $? -eq 0 ] && echo "PASS" || echo "FAIL")

java -Djava.security.auth.login.config="$JAAS_CONF" \
     -Djava.security.krb5.conf="$MINI_KDC_DIR/krb5.conf" \
     -cp build/libs/kub.jar \
     de.mbe1224.utils.kafka.infrastructure.cli.KafkaReadyCommand \
     3 \
     30000 \
     -b localhost:49092 \
     --config /tmp/client.properties &> /tmp/test-kafka-ready.log

KAFKA_BROKER_OPTION_TEST=$([ $? -eq 0 ] && echo "PASS" || echo "FAIL")

java -Djava.security.auth.login.config="$JAAS_CONF" \
     -Djava.security.krb5.conf="$MINI_KDC_DIR/krb5.conf" \
     -cp build/libs/kub.jar \
     de.mbe1224.utils.kafka.infrastructure.cli.KafkaReadyCommand \
     3 \
     30000 \
     -z localhost:11117 \
     -s SASL_SSL \
     --config /tmp/client.properties &> /tmp/test-kafka-zk-ready.log

KAFKA_ZK_OPTION_TEST=$([ $? -eq 0 ] && echo "PASS" || echo "FAIL")

kill "$KAFKA_PID"

# Wait for Kafka to die.
sleep 10

echo "TEST RESULTS:"
echo "ZOOKEEPER_TEST=$ZOOKEEPER_TEST"
echo "KAFKA_ZK_OPTION_TEST=$KAFKA_ZK_OPTION_TEST"
echo "KAFKA_BROKER_OPTION_TEST=$KAFKA_BROKER_OPTION_TEST"

[ ${ZOOKEEPER_TEST} == "PASS" ] \
    && [ ${KAFKA_ZK_OPTION_TEST} == "PASS" ] \
    && [ ${KAFKA_BROKER_OPTION_TEST} == "PASS" ] \
    && exit 0 \
    || exit 1