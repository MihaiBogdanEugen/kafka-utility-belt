package de.mbe1224.utils.kafka;


import de.mbe1224.utils.kafka.infrastructure.EmbeddedKafkaCluster;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import kafka.cluster.Broker;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The tests in this class should not be run in parallel.  This limitation is caused by how the
 * current implementation of EmbeddedKafkaCLuster works.
 */
public class KafkaUtilitiesTest {

    private static final ZkUtils mockZkUtils = mock(ZkUtils.class);
    private static final int anyMaxNumServers = 2;
    private static final String anyTopic = "valueNotRelevant";
    private static final int anyPartitions = 1;
    private static final int anyReplication = 1;
    private static final long anyRetentionMs = 1000L;
    private static final long oneYearRetention = 365 * 24 * 60 * 60 * 1000L;
    private static final String[] exampleTopics = {"__confluent.support.metrics", "anyTopic", "basketball"};

    @Test
    public void getNumTopicsThrowsIAEWhenZkUtilsIsNull() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();

        // When/Then
        try {
            kUtil.getNumTopics(null);
            fail("IllegalArgumentException expected because zkUtils is null");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void getNumTopicsReturnsMinusOneOnError() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        ZkUtils zkUtils = mock(ZkUtils.class);
        when(zkUtils.getAllTopics()).thenThrow(new RuntimeException("exception intentionally thrown by test"));

        // When/Then
        assertEquals(-1L, kUtil.getNumTopics(zkUtils));
    }

    @Test
    public void getNumTopicsReturnsZeroWhenThereAreNoTopics() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        ZkUtils zkUtils = mock(ZkUtils.class);
        List<String> zeroTopics = new ArrayList<>();
        when(zkUtils.getAllTopics()).thenReturn(JavaConversions.asScalaBuffer(zeroTopics).toList());

        // When/Then
        assertEquals(0L, kUtil.getNumTopics(zkUtils));
    }

    @Test
    public void getNumTopicsReturnsCorrectNumber() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        ZkUtils zkUtils = mock(ZkUtils.class);
        List<String> topics = new ArrayList<>();
        topics.add("topic1");
        topics.add("topic2");
        when(zkUtils.getAllTopics()).thenReturn(JavaConversions.asScalaBuffer(topics).toList());

        // When/Then
        assertEquals(topics.size(), kUtil.getNumTopics(zkUtils));
    }

    @Test
    public void getBootstrapServersThrowsIAEWhenZkUtilsIsNull() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();

        // When/Then
        try {
            kUtil.getBootstrapServers(null, anyMaxNumServers);
            fail("IllegalArgumentException expected because zkUtils is null");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void getBootstrapServersThrowsIAEWhenMaxNumServersIsZero() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        int zeroMaxNumServers = 0;

        // When/Then
        try {
            kUtil.getBootstrapServers(mockZkUtils, zeroMaxNumServers);
            fail("IllegalArgumentException expected because max number of servers is zero");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void getBootstrapServersReturnsEmptyListWhenThereAreNoLiveBrokers() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        ZkUtils zkUtils = mock(ZkUtils.class);
        List<Broker> empty = new ArrayList<>();
        when(zkUtils.getAllBrokersInCluster()).thenReturn(JavaConversions.asScalaBuffer(empty).toList());

        // When/Then
        assertTrue(kUtil.getBootstrapServers(zkUtils, anyMaxNumServers).isEmpty());
    }

    @Test
    public void createTopicThrowsIAEWhenZkUtilsIsNull() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();

        // When/Then
        try {
            kUtil.createAndVerifyTopic(null, anyTopic, anyPartitions, anyReplication, anyRetentionMs);
            fail("IllegalArgumentException expected because zkUtils is null");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void createTopicThrowsIAEWhenTopicIsNull() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        String nullTopic = null;

        // When/Then
        try {
            kUtil.createAndVerifyTopic(mockZkUtils, nullTopic, anyPartitions, anyReplication, anyRetentionMs);
            fail("IllegalArgumentException expected because topic is null");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void createTopicThrowsIAEWhenTopicIsEmpty() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        String emptyTopic = "";

        // When/Then
        try {
            kUtil.createAndVerifyTopic(mockZkUtils, emptyTopic, anyPartitions, anyReplication, anyRetentionMs);
            fail("IllegalArgumentException expected because topic is empty");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void createTopicThrowsIAEWhenNumberOfPartitionsIsZero() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        int zeroPartitions = 0;

        // When/Then
        try {
            kUtil.createAndVerifyTopic(mockZkUtils, anyTopic, zeroPartitions, anyReplication, anyRetentionMs);
            fail("IllegalArgumentException expected because number of partitions is zero");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void createTopicThrowsIAEWhenReplicationFactorIsZero() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        int zeroReplication = 0;

        // When/Then
        try {
            kUtil.createAndVerifyTopic(mockZkUtils, anyTopic, anyPartitions, zeroReplication, anyRetentionMs);
            fail("IllegalArgumentException expected because replication factor is zero");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void createTopicThrowsIAEWhenRetentionMsIsZero() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        long zeroRetentionMs = 0;

        // When/Then
        try {
            kUtil.createAndVerifyTopic(mockZkUtils, anyTopic, anyPartitions, anyReplication, zeroRetentionMs);
            fail("IllegalArgumentException expected because retention.ms is zero");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }


    @Test
    public void verifySupportTopicThrowsIAEWhenZkUtilsIsNull() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();

        // When/Then
        try {
            kUtil.verifySupportTopic(null, anyTopic, anyPartitions, anyReplication);
            fail("IllegalArgumentException expected because zkUtils is null");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }


    @Test
    public void verifySupportTopicThrowsIAEWhenTopicIsNull() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        String nullTopic = null;

        // When/Then
        try {
            kUtil.verifySupportTopic(mockZkUtils, nullTopic, anyPartitions, anyReplication);
            fail("IllegalArgumentException expected because topic is null");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void verifySupportTopicThrowsIAEWhenTopicIsEmpty() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        String emptyTopic = "";

        // When/Then
        try {
            kUtil.verifySupportTopic(mockZkUtils, emptyTopic, anyPartitions, anyReplication);
            fail("IllegalArgumentException expected because topic is empty");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void verifySupportTopicThrowsIAEWhenNumberOfPartitionsIsZero() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        int zeroPartitions = 0;

        // When/Then
        try {
            kUtil.verifySupportTopic(mockZkUtils, anyTopic, zeroPartitions, anyReplication);
            fail("IllegalArgumentException expected because number of partitions is zero");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void verifySupportTopicThrowsIAEWhenReplicationFactorIsZero() {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        int zeroReplication = 0;

        // When/Then
        try {
            kUtil.verifySupportTopic(mockZkUtils, anyTopic, anyPartitions, zeroReplication);
            fail("IllegalArgumentException expected because replication factor is zero");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @Test
    public void underreplicatedTopicsCanBeCreatedAndVerified() throws IOException {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        int numBrokers = 1;
        EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(numBrokers);
        int partitions = numBrokers + 1;
        int replication = numBrokers + 1;
        cluster.start();
        KafkaServer broker = cluster.getBroker(0);

        // When/Then
        for (String topic : exampleTopics) {
            assertTrue(kUtil.createAndVerifyTopic(broker.zkUtils(), topic, partitions, replication, oneYearRetention));
            // Only one broker is up, so the actual number of replicas will be only 1.
            assertEquals(KafkaUtilities.VerifyTopicState.Less, kUtil.verifySupportTopic(broker.zkUtils(), topic, partitions, replication));
        }
        assertEquals(exampleTopics.length, kUtil.getNumTopics(broker.zkUtils()));

        // Cleanup
        cluster.shutdown();
    }


    @Test
    public void underreplicatedTopicsCanBeRecreatedAndVerified() throws IOException {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        int numBrokers = 1;
        EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(numBrokers);

        int partitions = numBrokers + 1;
        int replication = numBrokers + 1;
        cluster.start();
        KafkaServer broker = cluster.getBroker(0);

        // When/Then
        for (String topic : exampleTopics) {
            assertTrue(kUtil.createAndVerifyTopic(broker.zkUtils(), topic, partitions, replication, oneYearRetention));
            assertTrue(kUtil.createAndVerifyTopic(broker.zkUtils(), topic, partitions, replication, oneYearRetention));
            assertEquals(KafkaUtilities.VerifyTopicState.Less, kUtil.verifySupportTopic(broker.zkUtils(), topic, partitions, replication));
        }
        assertEquals(exampleTopics.length, kUtil.getNumTopics(broker.zkUtils()));

        // Cleanup
        cluster.shutdown();
    }

    @Test
    public void createTopicFailsWhenThereAreNoLiveBrokers() throws IOException {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        // Provide us with a realistic but, once the cluster is stopped, defunct instance of zkutils.
        int numBrokers = 1;
        EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(numBrokers);
        cluster.start();
        KafkaServer broker = cluster.getBroker(0);
        ZkUtils defunctZkUtils = broker.zkUtils();
        cluster.shutdown();

        // When/Then
        assertFalse(kUtil.createAndVerifyTopic(defunctZkUtils, anyTopic, anyPartitions, anyReplication, anyRetentionMs));
    }

    @Test
    public void replicatedTopicsCanBeCreatedAndVerified() throws IOException {
        // Given
        KafkaUtilities kUtil = new KafkaUtilities();
        int numBrokers = 3;
        EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(numBrokers);

        cluster.start();
        KafkaServer broker = cluster.getBroker(0);
        ZkUtils zkUtils = broker.zkUtils();
        Random random = new Random();
        int replication = 3;

        // When/Then
        for (String topic : exampleTopics) {
            int morePartitionsThanBrokers = random.nextInt(10) + numBrokers + 1;
            assertTrue(kUtil.createAndVerifyTopic(zkUtils, topic, morePartitionsThanBrokers, replication, oneYearRetention));
            assertEquals(KafkaUtilities.VerifyTopicState.Exactly, kUtil.verifySupportTopic(zkUtils, topic, morePartitionsThanBrokers, replication));
        }

        // Cleanup
        cluster.shutdown();
    }

}