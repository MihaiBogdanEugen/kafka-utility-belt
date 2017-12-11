package de.mbe1224.utils;

import de.mbe1224.utils.ClusterStatus;
import de.mbe1224.utils.infrastructure.EmbeddedKafkaCluster;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class ClusterStatusSASLTest {

    private static EmbeddedKafkaCluster SecureKafkaCluster;

    @BeforeAll
    static void setup() throws IOException {
        SecureKafkaCluster = new EmbeddedKafkaCluster(3, 3, true);
        SecureKafkaCluster.start();
    }

    @AfterAll
    static void tearDown() {
        SecureKafkaCluster.shutdown();
    }

    @Test
    void zookeeperReadyWithSASL() {
        assertTimeoutPreemptively(ofMillis(120000), () -> {
            try {
                assertTrue(ClusterStatus.isZooKeeperReady(SecureKafkaCluster.getZookeeperConnectString(), 10000));
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    @Test
    void isKafkaReadyWithSASLAndSSL() {
        assertTimeoutPreemptively(ofMillis(120000), () -> {
            try {
                Properties clientSecurityProps = SecureKafkaCluster.getClientSecurityConfig();

                Map<String, String> config = Utils.propsToStringMap(clientSecurityProps);
                config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SecureKafkaCluster.getBootstrapBroker(SecurityProtocol.SASL_SSL));

                // Set password and enabled protocol as the Utils.propsToStringMap just returns toString()
                // representations and these properties don't have a valid representation.
                Password trustStorePassword = (Password) clientSecurityProps.get("ssl.truststore.password");
                config.put("ssl.truststore.password", trustStorePassword.value());
                config.put("ssl.enabled.protocols", "TLSv1.2");

                assertTrue(ClusterStatus.isKafkaReady(config, 3, 10000));
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    @Test
    void isKafkaReadyWithSASLAndSSLUsingZK() {
        assertTimeoutPreemptively(ofMillis(120000), () -> {
            try {
                Properties clientSecurityProps = SecureKafkaCluster.getClientSecurityConfig();

                boolean zkReady = ClusterStatus.isZooKeeperReady(SecureKafkaCluster.getZookeeperConnectString(), 30000);
                if (!zkReady) {
                    throw new RuntimeException("Could not reach zookeeper " + SecureKafkaCluster.getZookeeperConnectString());
                }
                Map<String, String> endpoints = ClusterStatus.getKafkaEndpointFromZooKeeper(SecureKafkaCluster.getZookeeperConnectString(), 30000);

                String bootstrap_broker = endpoints.get("SASL_SSL");
                Map<String, String> config = Utils.propsToStringMap(clientSecurityProps);
                config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap_broker);

                // Set password and enabled protocol as the Utils.propsToStringMap just returns toString()
                // representations and these properties don't have a valid representation.
                Password trustStorePassword = (Password) clientSecurityProps.get("ssl.truststore.password");
                config.put("ssl.truststore.password", trustStorePassword.value());
                config.put("ssl.enabled.protocols", "TLSv1.2");

                assertTrue(ClusterStatus.isKafkaReady(config, 3, 10000));
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }
}
