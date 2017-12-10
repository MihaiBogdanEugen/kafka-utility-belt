package de.mbe1224.utils.kafka.infrastructure;

import org.apache.zookeeper.client.FourLetterWordMain;
import org.apache.zookeeper.server.quorum.Election;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.JMXEnv;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * This class is based on code from Apache Zookeeper unit tests listed below.
 *
 * src/java/test/org/apache/zookeeper/test/ClientBase.java
 * src/java/test/org/apache/zookeeper/test/SaslClientTest.java
 * src/java/test/org/apache/zookeeper/test/SaslAuthTest.java
 */
public class EmbeddedZooKeeperEnsemble {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedZooKeeperEnsemble.class);
    private final Map<Integer, QuorumPeer> quorumPeersById = new ConcurrentHashMap<>();
    private int basePort;
    private String hostPort = "";
    private boolean isRunning = false;
    private int numNodes;

    public EmbeddedZooKeeperEnsemble(int numNodes, int basePort) throws IOException {
        this.numNodes = numNodes;
        this.basePort = basePort;
        initialize();
    }

    private void initialize() throws IOException {
        Map<Long, QuorumPeer.QuorumServer> peers = new HashMap<>();
        String localAddr = "localhost";
        for (int i = 0; i < numNodes; i++) {

            int port = basePort++;
            int portLE = basePort++;
            Long id = (long) i;

            peers.put(id, new QuorumPeer.QuorumServer(id, localAddr, port + 1000, portLE + 1000, QuorumPeer.LearnerType.PARTICIPANT));
        }

        for (int i = 0; i < numNodes; i++) {

            File dir = Files.createTempDirectory("zk" + i).toFile();

            int portClient = basePort++;
            LOGGER.info("creating QuorumPeer " + i + " port " + portClient);
            QuorumPeer s = new QuorumPeer(peers, dir, dir, portClient, 3, i, 2000, 3, 3);
            Assert.assertEquals(portClient, s.getClientPort());

            quorumPeersById.put(i, s);

            if (i == 0) {
                hostPort = localAddr + ":" + portClient;
            } else {
                hostPort += "," + localAddr + ":" + portClient;
            }
        }
    }

    public String connectString() {
        return hostPort;
    }

    public void start() throws IOException {

        JMXEnv.setUp();

        for (int i = 0; i < numNodes; i++) {
            LOGGER.info("start QuorumPeer " + i);
            QuorumPeer s = quorumPeersById.get(i);
            s.start();
        }

        LOGGER.info("Checking ports " + hostPort);

        for (String hp : hostPort.split(",")) {
            int connectionTimeout = 30000;
            Assert.assertTrue("waiting for server up", ClientBase.waitForServerUp(hp, connectionTimeout));
            LOGGER.info(hp + " is accepting client connections");
            try {
                LOGGER.info(sendStat(hp, connectionTimeout));
            } catch (TimeoutException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }

        JMXEnv.dump();
        isRunning = true;
    }

    private String sendStat(String hp, long timeout) throws TimeoutException {
        long start = System.currentTimeMillis();

        while (true) {
            try {
                HostPort e = parseHostPortList(hp).get(0);
                return FourLetterWordMain.send4LetterWord(e.host, e.port, "stat");
            } catch (IOException var7) {
                LOGGER.info("server " + hp + " not up " + var7);
            }

            if (System.currentTimeMillis() > start + timeout) {
                throw new TimeoutException();
            }

            try {
                Thread.sleep(250L);
            } catch (InterruptedException e) {
                //Ignore
            }
        }
    }

    private List<HostPort> parseHostPortList(String hplist) {
        ArrayList<HostPort> alist = new ArrayList<>();
        for (String hp : hplist.split(",")) {
            int idx = hp.lastIndexOf(':');
            String host = hp.substring(0, idx);
            int port;
            try {
                port = Integer.parseInt(hp.substring(idx + 1));
            } catch (RuntimeException e) {
                throw new RuntimeException("Problem parsing " + hp + e.toString());
            }
            alist.add(new HostPort(host, port));
        }
        return alist;
    }

    public void shutdown() {
        for (int i = 0; i < quorumPeersById.size(); i++) {
            shutdown(quorumPeersById.get(i));
        }

        String[] hostPorts = this.hostPort.split(",");

        for (String hp : hostPorts) {
            Assert.assertTrue("waiting for server down", ClientBase.waitForServerDown(hp, (long) ClientBase.CONNECTION_TIMEOUT));
            LOGGER.info(hp + " is no longer accepting client connections");
        }

        JMXEnv.tearDown();
        isRunning = false;
    }

    private void shutdown(QuorumPeer qp) {
        try {
            LOGGER.info("Shutting down quorum peer " + qp.getName());
            qp.shutdown();
            Election e = qp.getElectionAlg();
            if (e != null) {
                LOGGER.info("Shutting down leader election " + qp.getName());
                e.shutdown();
            } else {
                LOGGER.info("No election available to shutdown " + qp.getName());
            }

            LOGGER.info("Waiting for " + qp.getName() + " to exit thread");
            qp.join(30000L);
            if (qp.isAlive()) {
                Assert.fail("QP failed to shutdown in 30 seconds: " + qp.getName());
            }
        } catch (InterruptedException var2) {
            LOGGER.debug("QP interrupted: " + qp.getName(), var2);
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public static class HostPort {

        String host;
        int port;

        HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }
}
