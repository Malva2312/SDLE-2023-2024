package node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;


public class MainNode {
    // -----------------------------------------------
    // Constants
    private final static int MAIN_NODE_PORT = 5556;
    // -----------------------------------------------
    // Predifined Messages
    private final static String SNAP = "SNAP"; // Snapshot Request
    private final static String SNAP_REP = "SNAP_REP"; // Snapshot Reply
    private final static String FLUSH = "FLUSH"; // Flush
    private final static String UPDATE = "UPDATE"; // Update

    private final static String HEARTBEAT = "HEARTBEAT"; // Heartbeat
    private final static int HEARTBEAT_INTERVAL = 1000 * 3; // msecs
    private final static int TTL = HEARTBEAT_INTERVAL * 2; // Heartbeat TTL
    // -----------------------------------------------
    // Communication Channels
    private ZContext ctx;
    private ZLoop loop;

    private int port = MAIN_NODE_PORT;
    private long sequence = 0;

    private Socket snapshot; // (Router) SnapShot Request
    private Socket publisher; // (Pub) Publish Updates
    private Socket collector; // (PULL) Collect Updates

    // -----------------------------------------------
    // HashRing
    private class Node {
        public Long token = 0L;
        public int port = 0;
        public long ttl = 0;
        public String toString() {
            return token.toString() + "," + port +"\n";
        }
        Node(Long t, int p, long ttl) {
            token = t;
            port = p;
            this.ttl = ttl;
        }
    }

    private HashMap<Long, Node> hashring;
    // -----------------------------------------------

    private static class Snapshot implements IZLoopHandler {

        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            System.out.println("Receiving snapshot");
            MainNode node = (MainNode) arg;
            Socket socket = item.getSocket();

            byte[] identity = socket.recv();
            kvmsg request = kvmsg.recv(socket);

            if (request == null)
                return 0; // Interrupted

            if (!request.getKey().equals(SNAP)) {
                System.out.printf("E: bad request: not a snapshot\n");
                return 0;
            }

            String body = "";
            int size = node.hashring.size();
            for (Entry<Long, Node> entry : node.hashring.entrySet()) {
                body += entry.getKey() + "," + entry.getValue().port + "," + entry.getValue().ttl + "\n";
            }

            kvmsg snap = new kvmsg(++node.sequence);
            snap.setKey(SNAP_REP);
            snap.setProp("size", Integer.toString(size));
            snap.fmtBody("%s", body);

            socket.sendMore(identity);
            snap.send(socket);
            System.out.println("Snapshot sent");
            return 0;
        }
    }
    private static class Updates implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            System.out.println("Receiving updates");
            MainNode node = (MainNode) arg;
            Socket socket = item.getSocket();

            kvmsg update = kvmsg.recv(socket);

            if (update == null)
                return 0; // Interrupted

            try {
                if (update.getKey().equals(HEARTBEAT)){
                    if (node.hashring.containsKey(Long.parseLong(update.getProp("token")))){
                        node.hashring.get(Long.parseLong(update.getProp("token"))).ttl = System.currentTimeMillis() + TTL;
                    }
                    else {
                        Long token = Long.parseLong(update.getProp("token"));
                        int port = Integer.parseInt(update.getProp("port"));
                        long ttl = Long.parseLong(update.getProp("ttl"));
                        Node n = node.new Node(token, port, System.currentTimeMillis() + ttl );
                        node.hashring.put(token, n);
                        System.out.println("Added node " + token + " to the hash ring");

                        kvmsg propagate = new kvmsg(++node.sequence);
                        propagate.setKey(UPDATE);
                        propagate.setProp("token", token.toString());
                        propagate.setProp("port", Integer.toString(port));
                        propagate.send(node.publisher);                        
                    }
                }
                else if (update.getKey().equals(FLUSH)){
                    Long token = Long.parseLong(update.getProp("token"));
                    node.hashring.remove(token);
                    System.out.println("Removed node " + token + " from the hash ring");
                }
            }
            catch (Exception e) {
                System.out.println("E: bad request: token is not a number");
                return 0;
            }
            
            return 0;
        }
    }
    private static class FlushTTL implements IZLoopHandler{
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            System.out.println("Flushing TTL");
            MainNode node = (MainNode) arg;

            if (node.hashring == null)
                return 0;
            for (Node n : new ArrayList<Node>( node.hashring.values()) ) {

                if (n.ttl == 0)
                    continue;
                if (n.ttl <= System.currentTimeMillis()) {
                    node.hashring.remove(n.token);
                    System.out.println("Removed node " + n.token + " from the hash ring");

                    kvmsg flush = new kvmsg(++node.sequence);
                    flush.setKey(FLUSH);
                    flush.setProp("token", n.token.toString());
                    flush.send(node.publisher);
                }
            }
            return 0;
        }
    }
    // -----------------------------------------------
    // Constructor
    public MainNode() {
        ctx = new ZContext();

        loop = new ZLoop(ctx);
        loop.verbose(true);  // Enable verbose logging

        snapshot = ctx.createSocket(SocketType.ROUTER);
        snapshot.bind("tcp://*:" + port);

        publisher = ctx.createSocket(SocketType.PUB);
        publisher.bind("tcp://*:" + (port + 1));

        collector = ctx.createSocket(SocketType.PULL);
        collector.bind("tcp://*:" + (port + 2));

        hashring = new HashMap<Long, Node>();
    }

    // Run the node
    private void run() {
        // Main loop to communicate with nodes
        PollItem snapshotItem = new PollItem(snapshot, ZMQ.Poller.POLLIN);
        loop.addPoller(snapshotItem, new Snapshot(), this);
        
        PollItem collectorItem = new PollItem(collector, ZMQ.Poller.POLLIN);
        loop.addPoller(collectorItem, new Updates(), this);

        loop.addTimer(3000, 0, new FlushTTL(), this);
        System.out.println("MainNode is running...");

        loop.start();
        ctx.close();
    }
    // Main method
    public static void main(String[] args) {
        MainNode node = new MainNode();
        node.run();
    }
}
