package de.mbe1224.utils.kafka;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper Watcher that waits for SyncConnected.
 * When SASL is enabled, waits for both SyncConnected and SaslAuthenticated events.
 * In case of an AuthFailed event, isSuccessful is set to false.
 * This should be verified in the code using this class and failure message should be logged.
 */
public class ZooKeeperConnectionWatcher implements Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperConnectionWatcher.class);

    private CountDownLatch connectSignal;
    private boolean isSuccessful = true;
    private String failureMessage = null;
    private boolean isSASLEnabled;

    public ZooKeeperConnectionWatcher(CountDownLatch connectSignal, boolean isSASLEnabled) {
        this.connectSignal = connectSignal;
        this.isSASLEnabled = isSASLEnabled;
    }

    boolean isSuccessful() {
        return isSuccessful;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    @Override
    public void process(WatchedEvent event) {

        LOGGER.debug("Event state: {}", event.getState().name());
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    if (!isSASLEnabled) {
                        connectSignal.countDown();
                    }
                    break;
                case Expired:
                    failureMessage = "Session expired.";
                    isSuccessful = false;
                    connectSignal.countDown();
                    break;
                case Disconnected:
                    failureMessage = "Disconnected from the server.";
                    isSuccessful = false;
                    connectSignal.countDown();
                    break;
                case AuthFailed:
                    failureMessage = "Authentication failed.";
                    isSuccessful = false;
                    connectSignal.countDown();
                    break;
                case SaslAuthenticated:
                    connectSignal.countDown();
                    break;
            }
        }
    }
}
