package de.mbe1224.utils.kafka.cli;

import de.mbe1224.utils.kafka.ClusterStatus;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * This command checks if the zookeeper cluster is ready to accept requests.
 *
 * <zookeeper-connect>                          : ZooKeeper connection string
 * <timeout>                                    : timeout in millisecs for all operations.
 */
public class ZooKeeperReadyCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperReadyCommand.class);
    private static final String ZOOKEEPER_READY = "zookeeper-ready";
    private static final String ZOOKEEPER_CONNECT = "zookeeper-connect";
    private static final String TIMEOUT = "timeout";

    private static ArgumentParser createArgsParser() {

        ArgumentParser parser = ArgumentParsers
                .newFor(ZOOKEEPER_READY)
                .build()
                .defaultHelp(true)
                .description("Check if ZooKeeper is ready.");
        parser.addArgument(ZOOKEEPER_CONNECT)
                .action(store())
                .required(true)
                .type(String.class)
                .metavar(ZOOKEEPER_CONNECT.toUpperCase())
                .help("ZooKeeper connect string.");
        parser.addArgument(TIMEOUT)
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar(TIMEOUT.toUpperCase())
                .help("Time (in ms) to wait for service to be ready.");
        return parser;
    }

    public static void main(String[] args) {

        ArgumentParser parser = createArgsParser();
        boolean success;
        try {
            Namespace arguments = parser.parseArgs(args);
            LOGGER.debug("Arguments {}. ", arguments);
            success = ClusterStatus.isZooKeeperReady(arguments.getString(ZOOKEEPER_CONNECT), arguments.getInt(TIMEOUT));
        } catch (ArgumentParserException e) {
            success = BaseReadyCommand.handleParserExcepiton(args, parser, e);
        } catch (Exception e) {
            LOGGER.error("Error while running {}: {}", ZOOKEEPER_READY, e.getMessage());
            success = false;
        }

        System.exit(success ? 0 : 1);
    }
}
