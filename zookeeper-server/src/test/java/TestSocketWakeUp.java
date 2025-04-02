import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * @author chenxuegui
 * @since 2025/4/1
 */

public class TestSocketWakeUp {
    static Logger LOG = LoggerFactory.getLogger(ZooKeeper.class);

    public static void main(String[] args) throws Exception{
        Selector selector = Selector.open();


        InetSocketAddress serverAddress = new InetSocketAddress("192.168.229.130", 2181);
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false); // 非阻塞模式
        channel.connect(serverAddress);

        SelectionKey selectionKey = channel.register(selector, SelectionKey.OP_CONNECT);

        new Thread(() -> {
            while (true) {
                LOG.info("Thread 1: select() blocking...");
                try {
                    int ready = selector.select(10000); // 阻塞直到事件就绪或超时
                    LOG.info("Thread 1: select() returned with " + ready + " ready channels");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iter = keys.iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove(); // 必须移除！
                    try {
                        if (key.isConnectable()) {
                            SocketChannel ch = (SocketChannel) key.channel();
                            if (ch.finishConnect()) {
                                LOG.info("Thread 1: 连接成功...");
                                key.interestOps(SelectionKey.OP_WRITE); // 注册写事件
                            } else {
                                LOG.error("连接失败，关闭通道");
                                ch.close();
                            }
                        } else if (key.isWritable()) {
                            LOG.info("Thread 1: isWritable...");
                            // 写入数据...
                        } else if (key.isReadable()) {
                            LOG.info("Thread 1: isReadable...");
                        }
                    } catch (IOException e) {
                        LOG.error("处理事件异常", e);
                        key.cancel();
                        try {
                            key.channel().close();
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        }).start();

// 线程 2：唤醒 selector
        new Thread(() -> {

            while (true){
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                LOG.info("Thread 2: calling wakeup()");
               // selector.wakeup(); // 唤醒 select()，但不会添加任何 SelectionKey
            }

        }).start();
    }
}
