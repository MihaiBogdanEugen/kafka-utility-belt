package de.mbe1224.utilities.kafka.cli;

import de.mbe1224.utilities.kafka.ClusterStatus;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * This command checks if the zookeeper cluster is ready to accept requests.
 * where:
 * <zookeeper_connect>  : Zookeeper connect string
 * <timeout>            : timeout in millisecs for all operations.
 */
public class ZooKeeperReadyCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperReadyCommand.class);
    private static final String ZK_READY = "zk-ready";

    private static ArgumentParser createArgsParser() {
        ArgumentParser zkReady = ArgumentParsers
                .newFor(ZK_READY)
                .build()
                .defaultHelp(true)
                .description("Check if ZK is ready.");

        zkReady.addArgument("zookeeper_connect")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("ZOOKEEPER_CONNECT")
                .help("Zookeeper connect string.");

        zkReady.addArgument("timeout")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("TIMEOUT_IN_MS")
                .help("Time (in ms) to wait for service to be ready.");

        return zkReady;
    }

    public static void main(String[] args) {

        ArgumentParser parser = createArgsParser();
        boolean success;
        try {
            Namespace res = parser.parseArgs(args);
            LOGGER.debug("Arguments {}. ", res);
            success = ClusterStatus.isZooKeeperReady(res.getString("zookeeper_connect"), res.getInt("timeout"));
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                success = true;
            } else {
                parser.handleError(e);
                success = false;
            }
        } catch (Exception e) {
            LOGGER.error("Error while running zk-ready {}.", e);
            success = false;
        }

        if (success) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
