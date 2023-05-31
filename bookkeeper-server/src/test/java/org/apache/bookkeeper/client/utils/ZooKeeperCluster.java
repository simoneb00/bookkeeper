package org.apache.bookkeeper.client.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.INSTANCEID;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * Interface for utils.ZooKeeperCluster.
 */
public interface ZooKeeperCluster {
    ZooKeeper getZooKeeperClient();

    String getZooKeeperConnectString();

    String getMetadataServiceUri();

    String getMetadataServiceUri(String zkLedgersRootPath);

    String getMetadataServiceUri(String zkLedgersRootPath, String type);

    void startCluster() throws Exception;

    void stopCluster() throws Exception;

    void restartCluster() throws Exception;

    void killCluster() throws Exception;

    void sleepCluster(int time, TimeUnit timeUnit, CountDownLatch l)
            throws InterruptedException, IOException;

    default void expireSession(ZooKeeper zk) throws Exception {
        long id = zk.getSessionId();
        byte[] password = zk.getSessionPasswd();
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(10000);
        ZooKeeper zk2 = new ZooKeeper(getZooKeeperConnectString(), zk.getSessionTimeout(), w, id, password);
        w.waitForConnection();
        zk2.close();
    }

    default void createBKEnsemble(String ledgersPath) throws KeeperException, InterruptedException {
        Transaction txn = getZooKeeperClient().transaction();
        txn.create(ledgersPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.create(ledgersPath + "/" + AVAILABLE_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.create(ledgersPath + "/" + AVAILABLE_NODE + "/" + READONLY, new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        String instanceId = UUID.randomUUID().toString();
        txn.create(ledgersPath + "/" + INSTANCEID, instanceId.getBytes(UTF_8),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.commit();
    }
}
