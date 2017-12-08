package de.mbe1224.utils.kafka;

import de.mbe1224.utils.kafka.infrastructure.EmbeddedKafkaCluster;
import de.mbe1224.utils.kafka.infrastructure.EmbeddedZooKeeperEnsemble;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterWaitTest {

    @Test(timeout = 180000)
    public void isZookeeperReadyWait() throws IOException, InterruptedException {
        final EmbeddedZooKeeperEnsemble zookeeperWait = new EmbeddedZooKeeperEnsemble(3, 22222);
        Thread zkClusterThread = new Thread(() -> {
            try {
                Thread.sleep(20000);
                zookeeperWait.start();
                while (zookeeperWait.isRunning()) {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });

        zkClusterThread.start();

        try {
            assertTrue(ClusterStatus.isZooKeeperReady(zookeeperWait.connectString(), 30000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            zookeeperWait.shutdown();
        }
        zkClusterThread.join(60000);
    }

    @Test(timeout = 180000)
    public void isKafkaReadyWait() throws Exception {
        final EmbeddedKafkaCluster kafkaWait = new EmbeddedKafkaCluster(3, 3);

        Thread kafkaClusterThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
                kafkaWait.start();
                while (kafkaWait.isRunning()) {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });

        kafkaClusterThread.start();

        try {
            Map<String, String> config = new HashMap<>();
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaWait.getBootstrapBroker(SecurityProtocol.PLAINTEXT));

            assertTrue(ClusterStatus.isKafkaReady(config, 3, 10000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            kafkaWait.shutdown();
        }
        kafkaClusterThread.join(60000);
    }

    @Test(timeout = 180000)
    public void isKafkaReadyWaitUsingZooKeeper() throws Exception {
        final EmbeddedKafkaCluster kafkaWait = new EmbeddedKafkaCluster(3, 3);

        Thread kafkaClusterThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
                kafkaWait.start();
                while (kafkaWait.isRunning()) {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });

        kafkaClusterThread.start();
        try {

            boolean zkReady = ClusterStatus.isZooKeeperReady(kafkaWait.getZookeeperConnectString(),30000);

            if (!zkReady) {
                fail("Could not reach zookeeper " + kafkaWait.getZookeeperConnectString());
            }

            Map<String, String> endpoints = ClusterStatus.getKafkaEndpointFromZooKeeper(kafkaWait.getZookeeperConnectString(),30000);

            String bootstrap_broker = endpoints.get("PLAINTEXT");
            Map<String, String> config = new HashMap<>();
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap_broker);

            assertTrue(ClusterStatus.isKafkaReady(config, 3, 10000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            kafkaWait.shutdown();
        }
        kafkaClusterThread.join(60000);
    }
}
