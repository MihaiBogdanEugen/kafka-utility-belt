package de.mbe1224.utils.kafka.cli;

import de.mbe1224.utils.kafka.ClusterStatus;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * This command checks if the kafka cluster has the expected number of brokers and is ready to accept requests.
 *
 * <config>                                     : path to properties with client config.
 * <min-expected-brokers>                       : minimum brokers to wait for.
 * <timeout>                                    : timeout in ms for all operations. This includes looking up metadata in ZooKeeper or fetching metadata for the brokers.
 * (<bootstrap-brokers> or <zookeeper-connect>) : Either a bootstrap broker list or ZooKeeper connection string
 * <security-protocol>                          : Security protocol to use to connect to the broker.
 */
public class KafkaReadyCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReadyCommand.class);
    private static final String KAFKA_READY = "kafka-ready";
    private static final String MIN_EXPECTED_BROKERS = "min-expected-brokers";
    private static final String ZOOKEEPER_CONNECT = "zookeeper-connect";
    private static final String TIMEOUT = "timeout";
    private static final String SECURITY_PROTOCOL = "security-protocol";
    private static final String BOOTSTRAP_SERVERS = "bootstrap-servers";
    private static final String CONFIG = "config";

    private static ArgumentParser createArgsParser() {

        ArgumentParser parser = ArgumentParsers
                .newFor(KAFKA_READY)
                .build()
                .defaultHelp(true)
                .description("Check if Kafka is ready.");
        parser.addArgument(MIN_EXPECTED_BROKERS)
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar(MIN_EXPECTED_BROKERS.toUpperCase())
                .help("Minimum number of brokers to wait for.");
        parser.addArgument(TIMEOUT)
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar(TIMEOUT.toUpperCase())
                .help("Time (in ms) to wait for service to be ready.");
        parser.addArgument(CONFIG)
                .action(store())
                .type(String.class)
                .metavar(CONFIG.toUpperCase())
                .help("List of bootstrap brokers.");

        MutuallyExclusiveGroup mutexGroup = parser.addMutuallyExclusiveGroup();
        mutexGroup.addArgument(BOOTSTRAP_SERVERS)
                .action(store())
                .type(String.class)
                .metavar(BOOTSTRAP_SERVERS.toUpperCase())
                .help("List of bootstrap brokers.");
        mutexGroup.addArgument(ZOOKEEPER_CONNECT)
                .action(store())
                .type(String.class)
                .metavar(ZOOKEEPER_CONNECT.toUpperCase())
                .help("ZooKeeper connection string.");

        parser.addArgument(SECURITY_PROTOCOL)
                .action(store())
                .type(String.class)
                .metavar(SECURITY_PROTOCOL.toUpperCase())
                .setDefault(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL)
                .help("Which endpoint to connect to.");
        return parser;
    }

    public static void main(String[] args) {

        ArgumentParser parser = createArgsParser();
        boolean success;
        try {
            Namespace arguments = parser.parseArgs(args);
            LOGGER.debug("Arguments {}. ", arguments);

            Map<String, String> workerProps = new HashMap<>();

            int timeout = arguments.getInt(TIMEOUT);
            int minExpectedBrokers = arguments.getInt(MIN_EXPECTED_BROKERS);
            String config = arguments.getString(CONFIG);
            String bootstrapServers = arguments.getString(BOOTSTRAP_SERVERS);
            String zkConnectionString = arguments.getString(ZOOKEEPER_CONNECT);
            String securityProtocol = arguments.getString(SECURITY_PROTOCOL);

            if (config == null && !(securityProtocol.equals(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL))) {
                LOGGER.error("config is required for all protocols except {}", CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);
                success = false;
            } else {

                if (config != null) {
                    workerProps = Utils.propsToStringMap(Utils.loadProps(config));
                }
                if (bootstrapServers != null) {
                    workerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                } else {

                    if (!ClusterStatus.isZooKeeperReady(zkConnectionString, timeout)) {
                        throw new RuntimeException("Could not reach ZooKeeper " + zkConnectionString);
                    }

                    Map<String, String> endpoints = ClusterStatus.getKafkaEndpointFromZooKeeper(zkConnectionString, timeout);
                    String bootstrapBroker = endpoints.get(securityProtocol);
                    if (bootstrapBroker == null) {
                        throw new RuntimeException("No endpoints found for security protocol [" + securityProtocol + "]. Endpoints found in ZooKeeper [" + endpoints + "]");
                    }

                    workerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker);
                }

                success = ClusterStatus.isKafkaReady(workerProps, minExpectedBrokers, timeout);
            }

        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                success = true;
            } else {
                parser.handleError(e);
                success = false;
            }
        } catch (Exception e) {
            LOGGER.error("Error while running {}: {}", KAFKA_READY, e.getMessage());
            success = false;
        }

        System.exit(success ? 0 : 1);
    }
}
