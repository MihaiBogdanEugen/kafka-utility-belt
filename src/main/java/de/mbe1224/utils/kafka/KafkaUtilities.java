package de.mbe1224.utils.kafka;


import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode.Disabled$;
import kafka.client.ClientUtils$;
import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.common.BrokerEndPointNotAvailableException;
import kafka.log.LogConfig;
import kafka.server.BrokerShuttingDown;
import kafka.server.KafkaServer;
import kafka.server.PendingControlledShutdown;
import kafka.server.RunningAsBroker;
import kafka.utils.ZkUtils;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaUtilities {

    private static final Logger log = LoggerFactory.getLogger(KafkaUtilities.class);

    /**
     * When we verify if a topic is created, this enum keeps track of whether the desired #replicas
     * and #partitions match exactly what we asked for, or are less than what we asked for, or have an
     * unacceptable value (i.e., 0).
     */
    public enum VerifyTopicState {

        Exactly(0), Less(1), Greater(2), Inadequate(3);

        private final int stateId;

        VerifyTopicState(int stateId) {
            this.stateId = stateId;
        }

        public int getStateId() {
            return stateId;
        }
    }

    /**
     * Get the total number of topics in the cluster by querying ZooKeeper.
     *
     * @return The total number of topics in the cluster, or -1 if there was an error.
     * @throws IllegalArgumentException if zkUtils is null
     */
    public long getNumTopics(ZkUtils zkUtils) {
        if (zkUtils == null) {
            throw new IllegalArgumentException("zkUtils must not be null");
        }

        try {
            Seq<String> topics = zkUtils.getAllTopics();
            return topics.length();
        } catch (Exception e) {
            log.error("Could not retrieve number of topics from ZooKeeper: {}", e.getMessage());
            return -1L;
        }
    }

    /**
     * Gets a list of servers that are up in the cluster
     *
     * @param maxNumServers Maximum number of bootstrap servers that should be returned.  Note that
     *     less servers may be returned than the maximum.
     * @return A list of bootstrap servers, or an empty list if there are none or if there were
     *     errors.  Note that only servers with PLAINTEXT ports will be returned.
     */
    public List<String> getBootstrapServers(ZkUtils zkUtils, int maxNumServers) {
        if (zkUtils == null) {
            throw new IllegalArgumentException("zkUtils must not be null");
        }
        if (maxNumServers < 1) {
            throw new IllegalArgumentException("maximum number of requested servers must be >= 1");
        }

        // Note that we only support PLAINTEXT ports for this version
        Seq<BrokerEndPoint> brokerList = ClientUtils$.MODULE$.getPlaintextBrokerEndPoints(zkUtils);
        if (brokerList == null || brokerList.isEmpty()) {
            return new ArrayList<>();
        } else {
            int actualServers = Math.min(maxNumServers, brokerList.size());
            List<String> bootstrapServers = new ArrayList<>();
            Iterator<BrokerEndPoint> it = brokerList.iterator();
            int i = 0;
            while (it.hasNext() && i < actualServers) {
                BrokerEndPoint brokerEndPoint = it.next();
                try {
                    bootstrapServers.add(brokerEndPoint.connectionString());
                    i++;
                } catch (BrokerEndPointNotAvailableException e) {
                    // try next one
                }
            }
            return bootstrapServers;
        }
    }

    /**
     * Creates a topic in Kafka, if it is not already there, and verifies that it is properly created
     *
     * @param partitions Desired number of partitions
     * @param replication Desired number of replicas
     * @param retentionMs Desired retention time in milliseconds
     * @return True if topic was created and verified successfully. False if topic could not be
     *     created, or it is created but verification reveals that the number of replicas or
     *     partitions have dropped to unacceptable levels.
     */
    public boolean createAndVerifyTopic(ZkUtils zkUtils, String topic, int partitions, int replication, long retentionMs) {
        if (zkUtils == null) {
            throw new IllegalArgumentException("zkUtils must not be null");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic must not be null or empty");
        }
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be >= 1");
        }
        if (replication < 1) {
            throw new IllegalArgumentException("replication factor must be >= 1");
        }
        if (retentionMs <= 0) {
            throw new IllegalArgumentException("retention.ms must be >= 1");
        }

        boolean topicCreated = true;
        try {
            if (AdminUtils.topicExists(zkUtils, topic)) {
                return (
                        verifySupportTopic(zkUtils, topic, partitions, replication)
                                != VerifyTopicState.Inadequate
                );
            }
            Seq<Broker> brokerList = zkUtils.getAllBrokersInCluster();
            int actualReplication = Math.min(replication, brokerList.size());
            if (actualReplication < replication) {
                log.warn(
                        "The replication factor of topic {} will be set to {}, which is less than the "
                                + "desired replication factor of {} (reason: this cluster contains only {} brokers).  "
                                + "If you happen to add more brokers to this cluster, then it is important to increase "
                                + "the replication factor of the topic to eventually {} to ensure reliable and "
                                + "durable metrics collection.",
                        topic,
                        actualReplication,
                        replication,
                        brokerList.size(),
                        replication
                );
            }

            Properties metricsTopicProps = new Properties();
            metricsTopicProps.put(LogConfig.RetentionMsProp(), String.valueOf(retentionMs));
            log.info("Attempting to create topic {} with {} replicas, assuming {} total brokers",
                    topic, actualReplication, brokerList.size()
            );
            AdminUtils.createTopic(
                    zkUtils,
                    topic,
                    partitions,
                    actualReplication,
                    metricsTopicProps,
                    Disabled$.MODULE$
            );
        } catch (TopicExistsException te) {
            log.info("Topic {} already exists", topic);
            topicCreated = false;
        } catch (AdminOperationException e) {
            topicCreated = false;
            log.error("Could not create topic {}: {}", topic, e.getMessage());
        } catch (Exception e) {
            // there are several other Zookeeper exceptions possible deep in Zookeeper
            topicCreated = false;
            log.error("Zookeeper is unavailable. Could not create topic {}: {}", topic, e.getMessage());
        }

        return topicCreated;
    }

    /**
     * Verifies that the Kafka topic exists and is healthy.
     *
     * @param topic Topic to be validated.
     * @param expPartitions Expected number of partitions
     * @param expReplication Expected number of replicas
     * @return an enum describing the topic state
     */
    @SuppressWarnings("unchecked")
    public VerifyTopicState verifySupportTopic(ZkUtils zkUtils, String topic, int expPartitions, int expReplication) {

        if (zkUtils == null) {
            throw new IllegalArgumentException("zkUtils must not be null");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic must not be null or empty");
        }
        if (expPartitions < 1) {
            throw new IllegalArgumentException("expected partitions must be >= 1");
        }
        if (expReplication < 1) {
            throw new IllegalArgumentException("expected replication factor must be >= 1");
        }

        VerifyTopicState verifyTopicState = VerifyTopicState.Exactly;
        try {
            Set<String> topics = new HashSet<>();
            topics.add(topic);
            scala.Option<scala.collection.Map<Object, Seq<Object>>> partitionAssignmentOption = zkUtils
                    .getPartitionAssignmentForTopics(
                            JavaConversions
                                    .asScalaSet(topics)
                                    .toSeq()
                    ).get(topic);
            if (!partitionAssignmentOption.isEmpty()) {
                scala.collection.Map partitionAssignment = partitionAssignmentOption.get();
                int actualNumPartitions = partitionAssignment.size();
                if (actualNumPartitions != expPartitions) {
                    log.warn(
                            "The topic {} should have only {} partitions.  Having more partitions should "
                                    + "not hurt but it is only needed under special circumstances.",
                            topic,
                            expPartitions
                    );
                    verifyTopicState = VerifyTopicState.Less;
                }
                int firstPartitionId = 0;
                scala.Option<Seq<Object>> replicasOfFirstPartitionOption =
                        partitionAssignment.get(firstPartitionId);
                if (!replicasOfFirstPartitionOption.isEmpty()) {
                    int actualReplication = replicasOfFirstPartitionOption.get().size();
                    if (actualReplication < expReplication) {
                        log.warn(
                                "The replication factor of topic {} is {}, which is less than "
                                        + "the desired replication factor of {}.  If you happen to add more brokers to this"
                                        + " cluster, then it is important to increase the replication factor of the "
                                        + "topic to eventually {} to ensure reliable and durable metrics collection.",
                                topic,
                                actualReplication,
                                expReplication,
                                expReplication
                        );
                        verifyTopicState = VerifyTopicState.Less;
                    }
                } else {
                    log.error("No replicas known for partition 0 of support metrics topic {}", topic);
                    verifyTopicState = VerifyTopicState.Inadequate;
                }
            } else {
                log.error("No partitions are assigned to support metrics topic {}", topic);
                verifyTopicState = VerifyTopicState.Inadequate;
            }
        } catch (Exception e) {
            // there are several Zookeeper exceptions possible deep in Zookeeper
            log.error("Zookeeper is unavailable. Could not verify topic {}", topic);
            verifyTopicState = VerifyTopicState.Inadequate;
        }

        return verifyTopicState;
    }

    public boolean isReadyForMetricsCollection(KafkaServer server) {
        return server.brokerState().currentState() == RunningAsBroker.state();
    }

    public boolean isShuttingDown(KafkaServer server) {
        return server.brokerState().currentState() == PendingControlledShutdown.state() || server.brokerState().currentState() == BrokerShuttingDown.state();
    }

}
