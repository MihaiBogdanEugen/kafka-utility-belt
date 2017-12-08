package de.mbe1224.utils.kafka;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.Node;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Checks status of ZooKeeper or Kafka cluster.
 */
public class ClusterStatus {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterStatus.class);
    private static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
    private static final String BROKERS_IDS_PATH = "/brokers/ids";
    private static final int BROKER_METADATA_REQUEST_BACKOFF_MS = 1000;

    /**
     * Checks if the zookeeper cluster is ready to accept requests.
     *
     * @param zkConnectString ZooKeeper connect string
     * @param timeoutMs timeoutMs in milliseconds
     * @return true if the cluster is ready, false otherwise.
     */
    public static boolean isZooKeeperReady(String zkConnectString, int timeoutMs) {

        LOGGER.debug("Check if ZooKeeper is ready: {} ", zkConnectString);
        ZooKeeper zookeeper = null;
        boolean isSASLEnabled = false;

        try {

            CountDownLatch waitForConnection = new CountDownLatch(1);
            Object javaSecurityAuthLoginConfig = System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, null);
            if (javaSecurityAuthLoginConfig != null) {
                isSASLEnabled = true;
                LOGGER.info("SASL is enabled. java.security.auth.login.config={}", javaSecurityAuthLoginConfig);
            }

            ZooKeeperConnectionWatcher connectionWatcher = new ZooKeeperConnectionWatcher(waitForConnection, isSASLEnabled);
            zookeeper = new ZooKeeper(zkConnectString, timeoutMs, connectionWatcher);

            boolean timedOut = !waitForConnection.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (timedOut) {
                LOGGER.error("Timed out waiting for connection to ZooKeeper server [{}].", zkConnectString);
                return false;
            } else if (!connectionWatcher.isSuccessful()) {
                LOGGER.error("Error occurred while connecting to ZooKeeper server[{}]. {} ", zkConnectString, connectionWatcher.getFailureMessage());
                return false;
            } else {
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Error while waiting for ZooKeeper client to connect to the server [{}].", zkConnectString, e);
            return false;
        } finally {
            cleanupZooKeeper(zookeeper);
        }
    }

    /**
     * Checks if the kafka cluster is accepting client requests and has at least minBrokerCount brokers.
     *
     * @param minBrokerCount Expected no of brokers
     * @param timeoutMs timeoutMs in milliseconds
     * @return true is the cluster is ready, false otherwise.
     */
    public static boolean isKafkaReady(Map<String, String> config, int minBrokerCount, int timeoutMs) {

        LOGGER.debug("Check if Kafka is ready: {}", config);

        // Need to copy because `config` is Map<String, String> and `create` expects Map<String, Object>
        AdminClient adminClient = AdminClient.create(new HashMap<>(config));

        long begin = System.currentTimeMillis();
        long remainingWaitMs = timeoutMs;
        Collection<Node> brokers = new ArrayList<>();
        while (remainingWaitMs > 0) {

            // describeCluster does not wait for all brokers to be ready before returning the brokers. So, wait until
            // expected brokers are present or the time out expires.
            try {
                brokers = adminClient.describeCluster(new DescribeClusterOptions().timeoutMs((int) Math.min(Integer.MAX_VALUE, remainingWaitMs))).nodes().get();
                LOGGER.debug("Broker list: {}", (brokers != null ? brokers : "[]"));
                if ((brokers != null) && (brokers.size() >= minBrokerCount)) {
                    return true;
                }
            } catch (Exception e) {
                LOGGER.error("Error while getting broker list.", e);
                // Swallow exceptions because we want to retry until timeoutMs expires.
            }

            sleep(Math.min(BROKER_METADATA_REQUEST_BACKOFF_MS, remainingWaitMs));

            LOGGER.info("Expected {} brokers but found only {}. Trying to query Kafka for metadata again ...", minBrokerCount, brokers == null ? 0 : brokers.size());
            long elapsed = System.currentTimeMillis() - begin;
            remainingWaitMs = timeoutMs - elapsed;
        }

        LOGGER.error("Expected {} brokers but found only {}. Brokers found {}.", minBrokerCount, brokers == null ? 0 : brokers.size(), brokers != null ? brokers : "[]");

        return false;
    }

    /**
     * Gets metadata from ZooKeeper. This method only waits for at least one broker to be present.
     *
     * @param zkConnectString ZooKeeper connect string
     * @param timeoutMs Timeout in milliseconds
     * @return A list of broker metadata with at least one broker.
     */
    private static List<String> getBrokerMetadataFromZooKeeper(String zkConnectString, int timeoutMs)
            throws KeeperException, InterruptedException, IOException {
        LOGGER.debug("Get a bootstrap broker from ZooKeeper [{}].", zkConnectString);
        ZooKeeper zookeeper = null;
        try {

            zookeeper = createZooKeeperClient(zkConnectString, timeoutMs);
            boolean isBrokerRegisted = isKafkaRegisteredInZooKeeper(zookeeper, timeoutMs);

            if (!isBrokerRegisted) {
                return Collections.emptyList();
            }
            final List<String> brokers = getRawKafkaMetadataFromZK(zookeeper, timeoutMs);

            // Get metadata for brokers.
            List<String> brokerMetadata = new ArrayList<>();
            for (String broker : brokers) {
                brokerMetadata.add(new String(zookeeper.getData(String.format("%s/%s", BROKERS_IDS_PATH, broker), false, null)));
            }
            return brokerMetadata;
        } finally {
            cleanupZooKeeper(zookeeper);
        }
    }

    /**
     * Gets raw Kafka metadata from ZooKeeper.
     *
     * @param timeoutMs timeout in ms.
     * @param zookeeper ZooKeeper client.
     * @return List of Kafka metadata strings.
     */
    private static List<String> getRawKafkaMetadataFromZK(ZooKeeper zookeeper, int timeoutMs)
            throws InterruptedException, KeeperException {
        // Get the data.
        final CountDownLatch waitForBroker = new CountDownLatch(1);

        // Get children async. Countdown when one of the following happen:
        // 1. NodeChildrenChanged is triggered (this happens when children are created after the call
        // is made).
        // 2. ChildrenCallback gets a callback with children present (this happens when node has
        //    children when the call is made) .
        final List<String> brokers = new CopyOnWriteArrayList<>();

        Watcher watcher = event -> {
            LOGGER.debug("Got event when checking for children of /brokers/ids. type={} path={}", event.getType(), event.getPath());
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                waitForBroker.countDown();
            }
        };
        ChildrenCallback cb = (rc, path, ctx, children) -> {
            LOGGER.debug("ChildrenCallback got data for path={} children={}", path, children);
            if (children != null && children.size() > 0) {
                children.addAll(brokers);
                waitForBroker.countDown();
            }
        };

        zookeeper.getChildren(BROKERS_IDS_PATH, watcher, cb, null);

        boolean waitForBrokerTimedOut = !waitForBroker.await(timeoutMs, TimeUnit.MILLISECONDS);
        if (waitForBrokerTimedOut) {
            throw new TimeoutException(String.format("Timed out waiting for Kafka to register brokers in ZooKeeper. timeout (ms) = %s", timeoutMs));
        }

        if (brokers.isEmpty()) {
            // Get children. Broker list will be empty if the getChildren call above is
            // made before the children are present. In that case, the ChildrenCallback will
            // be called with an empty children list and we will wait for the NodeChildren
            // event to be fired. At this point, this has happened and we can the children
            // safely using a sync call.
            List<String> children = zookeeper.getChildren(BROKERS_IDS_PATH, false, null);
            brokers.addAll(children);
        }
        return brokers;
    }

    /**
     * Checks whether /brokers/ids is present.
     * This signifies that at least one Kafka broker has registered in ZooKeepers.
     *
     * @param timeoutMs timeout in ms.
     * @param zookeeper ZooKeeper client.
     * @return True if /brokers/ids is present.
     */
    private static boolean isKafkaRegisteredInZooKeeper(ZooKeeper zookeeper, int timeoutMs)
            throws InterruptedException {
        // Make sure /brokers/ids exists. Countdown when one of the following happen:
        // 1. node created event is triggered (this happens when /brokers/ids is created after the
        // call is made).
        // 2. StatCallback gets a non-null callback (this happens when /brokers/ids exists when the
        // call is made) .
        final CountDownLatch kafkaRegistrationSignal = new CountDownLatch(1);

        Watcher watcher = event -> {
            LOGGER.debug("Got event when checking for existence of /brokers/ids. type={} path={}", event.getType(), event.getPath());
            if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                kafkaRegistrationSignal.countDown();
            }
        };
        StatCallback cb = (rc, path, ctx, stat) -> {
            LOGGER.debug("StatsCallback got data for path={}, stat={}", path, stat);
            if (stat != null) {
                kafkaRegistrationSignal.countDown();
            }
        };

        zookeeper.exists(BROKERS_IDS_PATH, watcher, cb, null);

        boolean kafkaRegistrationTimedOut = !kafkaRegistrationSignal.await(timeoutMs, TimeUnit.MILLISECONDS);
        if (kafkaRegistrationTimedOut) {
            throw new TimeoutException(String.format("Timed out waiting for Kafka to create /brokers/ids in ZooKeeper. timeout (ms) = %s", timeoutMs));
        }

        return true;
    }

    /**
     * Create a ZooKeeper Client.
     *
     * @param zkConnectString ZooKeeper connect string.
     * @param timeoutMs timeout in ms.
     * @return ZooKeeper Client.
     */
    private static ZooKeeper createZooKeeperClient(String zkConnectString, int timeoutMs) throws IOException, InterruptedException {

        CountDownLatch waitForConnection = new CountDownLatch(1);
        ZooKeeper zookeeper;
        boolean isSASLEnabled = false;
        if (System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, null) != null) {
            isSASLEnabled = true;
        }
        ZooKeeperConnectionWatcher connectionWatcher = new ZooKeeperConnectionWatcher(waitForConnection, isSASLEnabled);
        zookeeper = new ZooKeeper(zkConnectString, timeoutMs, connectionWatcher);

        boolean timedOut = !waitForConnection.await(timeoutMs, TimeUnit.MILLISECONDS);
        if (timedOut) {
            throw new TimeoutException(String.format("Timed out waiting for connection to ZooKeeper. timeout(ms) = %s, ZooKeeper connect = %s", timeoutMs, zkConnectString));
        } else if (!connectionWatcher.isSuccessful()) {
            throw new RuntimeException(String.format("Error occurred while connecting to ZooKeeper server [%s]. %s", zkConnectString, connectionWatcher.getFailureMessage()));
        }
        return zookeeper;
    }

    /**
     * Gets a kafka endpoint for one broker from ZooKeeper. This method is used to get a broker for
     * the
     * bootstrap broker list. This method is expected to used in conjunction with isKafkaReady to
     * determine if Kafka is ready.
     *
     * @param zkConnectString ZooKeeper connect string
     * @param timeoutMs Timeout in milliseconds
     * @return A map of security-protocol->endpoints
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> getKafkaEndpointFromZooKeeper(String zkConnectString, int timeoutMs)
            throws InterruptedException, IOException, KeeperException {

        List<String> brokerMetadata = getBrokerMetadataFromZooKeeper(zkConnectString, timeoutMs);

        // Get the first broker. We will use this as the bootstrap broker for isKafkaReady method.
        if (brokerMetadata.isEmpty()) {
            throw new RuntimeException("No brokers found in ZooKeeper [" + zkConnectString + "] .");
        }
        String broker = brokerMetadata.get(0);

        Map<String, String> endpointMap = new HashMap<>();
        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Object>>() { }.getType();
        Map<String, Object> parsedBroker = gson.fromJson(broker, type);

        for (String rawEndpoint : (List<String>) parsedBroker.get("endpoints")) {
            String[] protocolAddress = rawEndpoint.split("://");

            String protocol = protocolAddress[0];
            String address = protocolAddress[1];
            endpointMap.put(protocol, address);
        }
        return endpointMap;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // this is okay, we just wake up early
            Thread.currentThread().interrupt();
        }
    }

    private static void cleanupZooKeeper(ZooKeeper zookeeper) {
        if (zookeeper != null) {
            try {
                zookeeper.close();
            } catch (InterruptedException e) {
                LOGGER.error("Error while shutting down ZooKeeper client.", e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
