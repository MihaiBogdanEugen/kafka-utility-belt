package de.mbe1224.utils.kafka;

import de.mbe1224.utils.kafka.infrastructure.EmbeddedKafkaCluster;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterStatusSASLTest {

    private static EmbeddedKafkaCluster KAFKA;

    @BeforeClass
    public static void setup() throws IOException {
        KAFKA = new EmbeddedKafkaCluster(3, 3, true);
        KAFKA.start();
    }

    @AfterClass
    public static void tearDown() {
        KAFKA.shutdown();
    }

    @Test(timeout = 120000)
    public void zookeeperReadyWithSASL() throws Exception {
        try {
            assertTrue(ClusterStatus.isZooKeeperReady(KAFKA.getZookeeperConnectString(), 10000));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void isKafkaReadyWithSASLAndSSL() {
        try {
            Properties clientSecurityProps = KAFKA.getClientSecurityConfig();

            Map<String, String> config = Utils.propsToStringMap(clientSecurityProps);
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapBroker(SecurityProtocol.SASL_SSL));

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
    }

    @Test(timeout = 120000)
    public void isKafkaReadyWithSASLAndSSLUsingZK() {
        try {
            Properties clientSecurityProps = KAFKA.getClientSecurityConfig();

            boolean zkReady = ClusterStatus.isZooKeeperReady(KAFKA.getZookeeperConnectString(), 30000);
            if (!zkReady) {
                throw new RuntimeException("Could not reach zookeeper " + KAFKA.getZookeeperConnectString());
            }
            Map<String, String> endpoints = ClusterStatus.getKafkaEndpointFromZooKeeper(KAFKA.getZookeeperConnectString(), 30000);

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
    }
}