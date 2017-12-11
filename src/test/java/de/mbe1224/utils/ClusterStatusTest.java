package de.mbe1224.utils;

import de.mbe1224.utils.infrastructure.EmbeddedKafkaCluster;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.*;

class ClusterStatusTest {

    private static EmbeddedKafkaCluster KafkaCluster;

    @BeforeAll
    static void setup() throws IOException {
        KafkaCluster = new EmbeddedKafkaCluster(3, 3);
        KafkaCluster.start();
    }

    @AfterAll
    static void tearDown() {
        KafkaCluster.shutdown();
    }

    @Test
    void zookeeperReady() {
        assertTimeoutPreemptively(ofMillis(120000), () -> {
            try {
                assertTrue(ClusterStatus.isZooKeeperReady(KafkaCluster.getZookeeperConnectString(), 10000));
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    @Test
    void zookeeperReadyWithBadConnectString() {
        assertTimeoutPreemptively(ofMillis(120000), () -> {
            try {
                assertFalse(ClusterStatus.isZooKeeperReady("localhost:3245", 10000));
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    @Test
    void isKafkaReady() {
        assertTimeoutPreemptively(ofMillis(120000), () -> {
            try {
                Map<String, String> config = new HashMap<>();
                config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaCluster.getBootstrapBroker(SecurityProtocol.PLAINTEXT));
                assertTrue(ClusterStatus.isKafkaReady(config, 3, 10000));
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    @Test
    void isKafkaReadyFailWithLessBrokers() {
        assertTimeoutPreemptively(ofMillis(120000), () -> {
            try {
                Map<String, String> config = new HashMap<>();
                config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaCluster.getBootstrapBroker(SecurityProtocol.PLAINTEXT));
                assertFalse(ClusterStatus.isKafkaReady(config, 5, 10000));
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    @Test
    void isKafkaReadyWaitFailureWithNoBroker() {
        assertTimeoutPreemptively(ofMillis(120000), () -> {
            try {
                Map<String, String> config = new HashMap<>();
                config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:6789");
                assertFalse(ClusterStatus.isKafkaReady(config, 3, 10000));
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }
}
