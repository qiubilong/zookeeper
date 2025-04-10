import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chenxuegui
 * @since 2025/4/1
 */
public class Zookeeper客户端 {

    static Logger LOG = LoggerFactory.getLogger(ZooKeeper.class);

    public static void main(String[] args) throws Exception{
        System.setProperty("zookeeper.sasl.client", "false");

        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("Zookeeper watcher -->" + event);
            }
        };

        ZooKeeper zooKeeper = new ZooKeeper("192.168.229.130:2181", 50000, watcher);

        Thread.sleep(8000);
        LOG.info("开始写入数据");
        zooKeeper.create("/zookeeperClient","data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        zooKeeper.getData("/test",false,null);

        Thread.currentThread().join();
    }
}
