package node;

import java.util.ArrayList;
import java.util.List;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZThread;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZThread.IDetachedRunnable;

import database.KeyValueDatabase;
import database.ShopList;
import java.time.Instant;
import java.util.concurrent.*;

public class LoadBalancer {
    // ----------------------------------------------
    // Main node interaction for node state
    private final static int MAIN_NODE_PORT = 5556;

    private final static String REQ_SNAPSHOT = "ALLNODES?";
    private final static String REP_SNAPSHOT = "REPNODESSNAP";

    private final static String SUB_NODES = "/NODE/";
    private final static String FLUSH_SIGNAL = "/FLUSH/";

    private final static int HEARTBEAT = 5 * 1000; // msecs
    private final static int TTL = 2 * HEARTBEAT; // msecs
    private final static int REP_TIMEOUT = 5 * 1000; // msecs

    private final static int alarm = 5 * 1000; // msecs
    // private final static boolean show_stats = true;
    private final static boolean time_stats = false;

    private ZContext ctx;
    private ZLoop loop;

    Socket snapshot; // Request Snapshot from MainNode
    Socket subscriber; // Subscribe to all updates from MainNode
    Socket publisher; // Publish updates to MainNode

    // ----------------------------------------------
    private ConcurrentHashMap<Long, Integer> tokenAddrsMap;

    private static class PrintStatus implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            LoadBalancer self = (LoadBalancer) arg;
            System.out.println("\nChecking tokenAddrsMap");
            for (Long token : self.tokenAddrsMap.keySet()) {
                int addr = self.tokenAddrsMap.get(token);
                System.out.printf("Token: %d, Addr: tcp://localhost:%s\n", token, addr);
            }
            return 0;
        }
    }

    private static class ReceiveUpdate implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {

            LoadBalancer self = (LoadBalancer) arg;

            kvmsg kvMsg = kvmsg.recv(self.subscriber);
            if (kvMsg == null)
                return 0; // Interrupted

            if (kvMsg.getSequence() > 0) {

                if (kvMsg.getKey().startsWith(SUB_NODES + FLUSH_SIGNAL)) {
                    Long old_key = Long.parseLong(kvMsg.getProp("token"));
                    System.out.println("I: flush token " + old_key);
                    self.tokenAddrsMap.remove(old_key);
                } else if (kvMsg.getKey().startsWith(SUB_NODES)) {
                    Long t = Long.parseLong(kvMsg.getProp("token"));
                    int addr = Integer.parseInt(kvMsg.getProp("addr"));

                    if (self.tokenAddrsMap.containsKey(t)) {
                        if (self.tokenAddrsMap.get(t) != addr) {
                            System.out.println("I: update token " + t);
                            self.tokenAddrsMap.replace(t, addr);
                        }
                    } else {
                        System.out.println("I: new token " + t);
                        self.tokenAddrsMap.put(t, addr);
                    }

                } else {
                    System.out.println("E: bad request, aborting");
                    return 0;
                }
            } else {
                kvMsg.destroy();
            }

            return 0;
        }
    }

    private void requestSnapshot() {
        // get state snapshot
        kvmsg kvMsg = new kvmsg(0);
        kvMsg.setKey(REQ_SNAPSHOT);
        kvMsg.setProp("subtree", SUB_NODES);
        kvMsg.send(snapshot);
        kvMsg.destroy();

        // Set timeout
        Long timeOut = System.currentTimeMillis() + REP_TIMEOUT;

        while (System.currentTimeMillis() < timeOut) { // Wait for snapshot
            kvMsg = kvmsg.recv(snapshot);
            if (kvMsg == null) {
                break; // Interrupted
            }

            if (kvMsg.getKey().equals(REP_SNAPSHOT)) {
                String[] body = new String(kvMsg.body(), ZMQ.CHARSET).split("\n");
                int size = Integer.parseInt(kvMsg.getProp("size"));

                for (int i = 0; i < size;) {

                    Long new_token = Long.parseLong(body[i++]);
                    int new_addr = Integer.parseInt(body[i++]);
                    tokenAddrsMap.put(new_token, new_addr);
                }
                kvMsg.destroy();
                break;
            }
        }
    }

    public LoadBalancer() {
        ctx = new ZContext();
        loop = new ZLoop(ctx);
        loop.verbose(false);

        // Main node interaction for node state ------
        snapshot = ctx.createSocket(SocketType.DEALER);
        snapshot.connect("tcp://localhost:" + MAIN_NODE_PORT);

        subscriber = ctx.createSocket(SocketType.SUB);
        subscriber.connect("tcp://localhost:" + (MAIN_NODE_PORT + 1));
        subscriber.subscribe(SUB_NODES.getBytes(ZMQ.CHARSET));

        publisher = ctx.createSocket(SocketType.PUSH);
        publisher.connect("tcp://localhost:" + (MAIN_NODE_PORT + 2));

        tokenAddrsMap = new ConcurrentHashMap<Long, Integer>();
    }

    public void run() {
        ZLoop loop = new ZLoop(ctx);
        loop.verbose(false);

        requestSnapshot();

        PollItem poller = new PollItem(subscriber, ZMQ.Poller.POLLIN);
        loop.addPoller(poller, new ReceiveUpdate(), this);

        loop.addTimer(alarm, 0, new PrintStatus(), this);

        loop.start();
        ctx.close();
    }

    public static void main(String[] args) {
        LoadBalancer loadBalancer = new LoadBalancer();
        loadBalancer.run();

    }
}
