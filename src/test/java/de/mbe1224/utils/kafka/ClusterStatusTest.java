package de.mbe1224.utils.kafka;

import de.mbe1224.utils.kafka.infrastructure.EmbeddedKafkaCluster;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterStatusTest {

    private static EmbeddedKafkaCluster KafkaCluster;

    @BeforeClass
    public static void setup() throws IOException {
        KafkaCluster = new EmbeddedKafkaCluster(3, 3);
        KafkaCluster.start();
    }

    @AfterClass
    public static void tearDown() {
        KafkaCluster.shutdown();
    }

    @Test(timeout = 120000)
    public void zookeeperReady() {
        try {
            assertTrue(ClusterStatus.isZooKeeperReady(KafkaCluster.getZookeeperConnectString(), 10000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void zookeeperReadyWithBadConnectString() {
        try {
            assertFalse(ClusterStatus.isZooKeeperReady("localhost:3245", 10000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void isKafkaReady() {
        try {
            Map<String, String> config = new HashMap<>();
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaCluster.getBootstrapBroker(SecurityProtocol.PLAINTEXT));
            assertTrue(ClusterStatus.isKafkaReady(config, 3, 10000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void isKafkaReadyFailWithLessBrokers() {
        try {
            Map<String, String> config = new HashMap<>();
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaCluster.getBootstrapBroker(SecurityProtocol.PLAINTEXT));
            assertFalse(ClusterStatus.isKafkaReady(config, 5, 10000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void isKafkaReadyWaitFailureWithNoBroker() {
        try {
            Map<String, String> config = new HashMap<>();
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:6789");
            assertFalse(ClusterStatus.isKafkaReady(config, 3, 10000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
