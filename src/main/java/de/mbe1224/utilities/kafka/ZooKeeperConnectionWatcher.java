package de.mbe1224.utilities.kafka;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

/**
 * Waits for SyncConnected. When SASL is enabled, waits for both SyncConnected and SaslAuthenticated events.
 * In case of an AuthFailed event, isSuccessful is set to false.
 * This should be verified in the code using this class and failure message should be logged.
 */
public class ZooKeeperConnectionWatcher implements Watcher {

    private CountDownLatch connectSignal;
    private boolean isSuccessful = true;
    private String failureMessage = null;
    private boolean isSASLEnabled;

    ZooKeeperConnectionWatcher(CountDownLatch connectSignal, boolean isSASLEnabled) {
        this.connectSignal = connectSignal;
        this.isSASLEnabled = isSASLEnabled;
    }

    boolean isSuccessful() {
        return isSuccessful;
    }

    String getFailureMessage() {
        return failureMessage;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    // If SASL is enabled, we want to wait for the SaslAuthenticated event.
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
