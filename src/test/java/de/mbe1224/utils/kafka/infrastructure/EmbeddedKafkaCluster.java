package de.mbe1224.utils.kafka.infrastructure;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.login.Configuration;

import kafka.security.minikdc.MiniKdc;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.utils.MockTime;
import scala.Option;
import scala.Option$;
import scala.collection.JavaConversions;

/**
 * This class is based on code from
 * src/test/java/io/confluent/support/metrics/common/kafka/EmbeddedKafkaCluster.java
 * at
 * https://github.com/confluentinc/support-metrics-common/support-metrics-common/
 *
 * Starts an embedded Kafka cluster including a backing ZooKeeper ensemble. It adds support for
 * 1. Zookeeper in clustered mode with SASL security
 * 2. Kafka with SASL_SSL security
 * <p>
 * This class should be used for unit/integration testing only.
 */
public class EmbeddedKafkaCluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
    private static final Option<SecurityProtocol> INTER_BROKER_SECURITY_PROTOCOL = Option.apply(SecurityProtocol.PLAINTEXT);
    private static final boolean ENABLE_CONTROLLED_SHUTDOWN = true;
    private static final boolean ENABLE_DELETE_TOPIC = false;
    private static final int BROKER_PORT_BASE = 39092;
    private static final boolean ENABLE_PLAINTEXT = true;
    private static final boolean ENABLE_SASL_PLAINTEXT = false;
    private static final int SASL_PLAINTEXT_PORT = 0;
    private static final boolean ENABLE_SSL = false;
    private static final int SSL_PORT = 0;
    private static final int SASL_SSL_PORT_BASE = 49092;
    private static Option<Properties> brokerSaslProperties = Option$.MODULE$.empty();
    private static MiniKdc kdc;
    private static File trustStoreFile;
    private static Properties saslProperties;
    private final Map<Integer, KafkaServer> brokersById = new ConcurrentHashMap<>();
    private File jaasFilePath = null;
    private Option<File> brokerTrustStoreFile = Option$.MODULE$.empty();
    private boolean enableSASLSSL = false;
    private EmbeddedZooKeeperEnsemble zookeeper = null;
    private int numBrokers;
    private boolean isRunning = false;

    public EmbeddedKafkaCluster(int numBrokers, int numZookeeperPeers) throws IOException {
        this(numBrokers, numZookeeperPeers, false);
    }

    public EmbeddedKafkaCluster(int numBrokers, int numZookeeperPeers, boolean enableSASLSSL)
            throws IOException {
        this(numBrokers, numZookeeperPeers, enableSASLSSL, null, null);
    }

    public EmbeddedKafkaCluster(int numBrokers, int numZookeeperPeers, boolean enableSASLSSL, String jaasFilePath, String miniKDCDir)
            throws IOException {
        this.enableSASLSSL = enableSASLSSL;

        if (numBrokers <= 0 || numZookeeperPeers <= 0) {
            throw new IllegalArgumentException("number of servers must be >= 1");
        }

        if (jaasFilePath != null) {
            this.jaasFilePath = new File(jaasFilePath);
        }
        this.numBrokers = numBrokers;

        if (this.enableSASLSSL) {
            File workDir;
            if (miniKDCDir != null) {
                workDir = new File(miniKDCDir);
            } else {
                workDir = Files.createTempDirectory("kdc").toFile();
            }
            Properties kdcConf = MiniKdc.createConfig();
            kdc = new MiniKdc(kdcConf, workDir);
            kdc.start();

            String jaasFile = createJAASFile();

            System.setProperty("java.security.auth.login.config", jaasFile);
            System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
            // Uncomment this to debug Kerberos issues.
            // System.setProperty("sun.security.krb5.debug","true");

            trustStoreFile = File.createTempFile("truststore", ".jks");

            saslProperties = new Properties();
            saslProperties.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
            saslProperties.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, "GSSAPI");

            this.brokerTrustStoreFile = Option.apply(trustStoreFile);
            brokerSaslProperties = Option.apply(saslProperties);
        }

        zookeeper = new EmbeddedZooKeeperEnsemble(numZookeeperPeers);
    }

    private String createJAASFile() throws IOException {

        String zkServerPrincipal = "zookeeper/localhost";
        String zkClientPrincipal = "zkclient/localhost";
        String kafkaServerPrincipal = "kafka/localhost";
        String kafkaClientPrincipal = "client/localhost";

        if (jaasFilePath == null) {
            jaasFilePath = new File(Files.createTempDirectory("sasl").toFile(), "jaas.conf");
        }

        FileWriter fwriter = new FileWriter(jaasFilePath);

        String template =
                "" +
                        "Server {\n" +
                        "   com.sun.security.auth.module.Krb5LoginModule required\n" +
                        "   useKeyTab=true\n" +
                        "   keyTab=\"$ZK_SERVER_KEYTAB$\"\n" +
                        "   storeKey=true\n" +
                        "   useTicketCache=false\n" +
                        "   principal=\"$ZK_SERVER_PRINCIPAL$@EXAMPLE.COM\";\n" +
                        "};\n" +
                        "Client {\n" +
                        "com.sun.security.auth.module.Krb5LoginModule required\n" +
                        "   useKeyTab=true\n" +
                        "   keyTab=\"$ZK_CLIENT_KEYTAB$\"\n" +
                        "   storeKey=true\n" +
                        "   useTicketCache=false\n" +
                        "   principal=\"$ZK_CLIENT_PRINCIPAL$@EXAMPLE.COM\";" +
                        "};" + "\n" +
                        "KafkaServer {\n" +
                        "   com.sun.security.auth.module.Krb5LoginModule required\n" +
                        "   useKeyTab=true\n" +
                        "   keyTab=\"$KAFKA_SERVER_KEYTAB$\"\n" +
                        "   storeKey=true\n" +
                        "   useTicketCache=false\n" +
                        "   serviceName=kafka\n" +
                        "   principal=\"$KAFKA_SERVER_PRINCIPAL$@EXAMPLE.COM\";\n" +
                        "};\n" +
                        "KafkaClient {\n" +
                        "com.sun.security.auth.module.Krb5LoginModule required\n" +
                        "   useKeyTab=true\n" +
                        "   keyTab=\"$KAFKA_CLIENT_KEYTAB$\"\n" +
                        "   storeKey=true\n" +
                        "   useTicketCache=false\n" +
                        "   serviceName=kafka\n" +
                        "   principal=\"$KAFKA_CLIENT_PRINCIPAL$@EXAMPLE.COM\";" +
                        "};" + "\n";

        String output = template
                .replace("$ZK_SERVER_KEYTAB$", createKeytab(zkServerPrincipal))
                .replace("$ZK_SERVER_PRINCIPAL$", zkServerPrincipal)
                .replace("$ZK_CLIENT_KEYTAB$", createKeytab(zkClientPrincipal))
                .replace("$ZK_CLIENT_PRINCIPAL$", zkClientPrincipal)
                .replace("$KAFKA_SERVER_KEYTAB$", createKeytab(kafkaServerPrincipal))
                .replace("$KAFKA_SERVER_PRINCIPAL$", kafkaServerPrincipal)
                .replace("$KAFKA_CLIENT_KEYTAB$", createKeytab(kafkaClientPrincipal))
                .replace("$KAFKA_CLIENT_PRINCIPAL$", kafkaClientPrincipal);

        LOGGER.debug("JAAS Config: " + output);

        fwriter.write(output);

        fwriter.close();
        return jaasFilePath.getAbsolutePath();

    }

    private String createKeytab(String principal) {

        File keytabFile = TestUtils.tempFile();

        List<String> principals = new ArrayList<>();
        principals.add(principal);
        kdc.createPrincipal(keytabFile, JavaConversions.asScalaBuffer(principals).toList());

        LOGGER.debug("Keytab file for " + principal + " : " + keytabFile.getAbsolutePath());
        return keytabFile.getAbsolutePath();
    }

    public Properties getClientSecurityConfig() {
        return enableSASLSSL
                ? TestUtils.producerSecurityConfigs(SecurityProtocol.SASL_SSL, Option.apply(trustStoreFile), Option.apply(saslProperties))
                : new Properties();
    }

    public void start() {
        initializeZookeeper();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            LOGGER.debug("Starting broker with id {} ...", brokerId);
            startBroker(brokerId, zookeeper.connectString());
        }
        isRunning = true;
    }

    public void shutdown() {
        for (int brokerId : brokersById.keySet()) {
            LOGGER.debug("Stopping broker with id {} ...", brokerId);
            stopBroker(brokerId);
        }
        zookeeper.shutdown();
        if (kdc != null) {
            kdc.stop();
        }
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("zookeeper.authProvider.1");
        Configuration.setConfiguration(null);
        isRunning = false;
    }

    private void initializeZookeeper() {
        try {
            zookeeper.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void startBroker(int brokerId, String zkConnectString) {
        if (brokerId < 0) {
            throw new IllegalArgumentException("broker id must not be negative");
        }

        Properties props = TestUtils.createBrokerConfig(brokerId, zkConnectString, ENABLE_CONTROLLED_SHUTDOWN, ENABLE_DELETE_TOPIC, BROKER_PORT_BASE + brokerId, INTER_BROKER_SECURITY_PROTOCOL, this.brokerTrustStoreFile, brokerSaslProperties, ENABLE_PLAINTEXT, ENABLE_SASL_PLAINTEXT, SASL_PLAINTEXT_PORT, ENABLE_SSL, SSL_PORT, this.enableSASLSSL, SASL_SSL_PORT_BASE + brokerId, Option.empty(), 1);

        KafkaServer broker = TestUtils.createServer(KafkaConfig.fromProps(props), new MockTime());
        brokersById.put(brokerId, broker);
    }

    private void stopBroker(int brokerId) {
        if (brokersById.containsKey(brokerId)) {
            KafkaServer broker = brokersById.get(brokerId);
            broker.shutdown();
            broker.awaitShutdown();
            CoreUtils.delete(broker.config().logDirs());
            brokersById.remove(brokerId);
        }
    }

    public String getBootstrapBroker(SecurityProtocol securityProtocol) {
        switch (securityProtocol) {
            case PLAINTEXT:
                // The first broker will always listen on this port.
                return "localhost:" + BROKER_PORT_BASE;
            case SASL_SSL:
                // The first broker will always listen on this port.
                return "localhost:" + SASL_SSL_PORT_BASE;
            default:
                throw new RuntimeException(securityProtocol.name() + " is not supported.");
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public String getZookeeperConnectString() {
        return this.zookeeper.connectString();
    }
}
