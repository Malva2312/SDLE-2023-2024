package proj;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;

import database.KeyValueDatabase;
import database.ShopList;

import java.time.Instant;
import java.util.concurrent.*;

public class Proxy {
    // -----------------------------------------------
    // Constants
    private final static int MAIN_NODE_PORT = 5556;
    // -----------------------------------------------
    // Predifined Messages
    private final static String SNAP = "SNAP"; // Snapshot Request
    private final static String SNAP_REP = "SNAP_REP"; // Snapshot Reply
    private final static String FLUSH = "FLUSH"; // Flush
    private final static String UPDATE = "UPDATE"; // Update

    private final static String READ = "READ"; // Read
    private final static String READ_REP = "READ_REP"; // Read Reply
    private final static String WRITE = "WRITE"; // Write
    private final static String WRITE_REP = "WRITE_REP"; // Write Reply
    private final static String OK = "OK"; // OK
    private final static String FAIL = "FAIL"; // FAIL

    private final static String HEARTBEAT = "HEARTBEAT"; // Heartbeat
    private final static int HEARTBEAT_INTERVAL = 1000 * 3; // msecs
    private final static int TTL = HEARTBEAT_INTERVAL * 2; // Heartbeat TTL
    // -----------------------------------------------
    // Communication Channels
    private ZContext ctx;

    private final int port = 5555; // Default port
    private long sequence = 0;

    private Socket snapshot; // Sends snapshot request
    private Socket subscriber; // Collects hash ring updates
    // -----------------------------------------------
    // HashRing
    private ConcurrentHashMap<Long, Integer> hashring;
    // -----------------------------------------------
    // Debugging
    private static boolean token_status = true;
    private static boolean health_status = false;

    // -----------------------------------------------
    // -----------------------------------------------
    // Request memory snapshot from main node
    private void snapshot() {
        System.out.println("Requesting snapshot from main node");
        kvmsg request = new kvmsg(0);
        request.setKey(SNAP);

        request.send(snapshot);
        // TODO: TIMEOUT
        kvmsg reply = kvmsg.recv(snapshot); // Wait for snapshot
        if (reply == null)
            return; // Interrupted
        if (reply.getKey().equals(SNAP_REP)) {

            sequence = reply.getSequence();
            // Update the hash ring
            int size = !reply.getProp("size").equals("")
                    ? Integer.parseInt(reply.getProp("size"))
                    : 0;

            String body = new String(reply.body(), ZMQ.CHARSET);
            String[] node = body.split("\n");
            for (int i = 0; i < size; i++) {
                String[] instance = node[i].split(",");
                hashring.put(Long.parseLong(instance[0]), Integer.parseInt(instance[1]));
                System.out.println("Added node " + instance[0] + " to the hash ring");
            }
        }
    }

    // Receive updates from the main node
    private static class ReceiveUpdate implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Proxy node = (Proxy) arg;
            kvmsg update = kvmsg.recv(node.subscriber);
            if (update == null)
                return -1; // Interrupted

            if (update.getKey().equals(FLUSH)) {
                try {
                    node.sequence = update.getSequence();
                    Long token = Long.parseLong(update.getProp("token"));
                    // Remove token from hash ring if it exists
                    if (node.hashring.containsKey(token)) {
                        node.hashring.remove(token);
                        System.out.println("Removed node " + token + " from the hash ring");
                    } else {
                        System.out.println("E: token does not exist");
                        return 0;
                    }
                } catch (Exception e) {
                    System.out.println("E: bad request: token is not a number");
                    return 0;
                }
                return 0;
            } else if (update.getKey().equals(UPDATE)) {
                try {
                    Long token = Long.parseLong(update.getProp("token"));
                    int port = Integer.parseInt(update.getProp("port"));
                    // Add token to hash ring if it does not exist
                    if (!node.hashring.containsKey(token)) {
                        node.hashring.put(token, port);
                        System.out.println("Added node " + token + " to the hash ring");
                    } else if (node.hashring.get(token) != port) {
                        node.hashring.put(token, port);
                        System.out.println("Updated node " + token + " in the hash ring");
                    } else {
                        System.out.println("E: token already exists");
                        return 0;
                    }
                } catch (Exception e) {
                    System.out.println("E: bad request: token or port is not a number");
                    return 0;
                }
                return 0;
            } else {
                System.out.println("E: bad request: not an update");
                return 0;
            }
        }
    }

    // Print the status of the hash ring (tokens and addresses)
    private static class PrintStatus implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Proxy node = (Proxy) arg;
            System.out.println("\nChecking tokenAddrsMap");
            for (Long token : node.hashring.keySet()) {
                int addr = node.hashring.get(token);
                System.out.printf("Token: %d, Addr: tcp://localhost:%s\n", token, addr);
            }
            return 0;
        }
    }

    // -----------------------------------------------
    // Thread to handle main communication
    private static class Central extends Thread {
        Object args[];

        Central(Object args[]) {
            this.args = args;
        }

        @Override
        public void run() {
            Proxy node = (Proxy) args[0];
            ZLoop loop = new ZLoop(node.ctx);

            PollItem sub = new PollItem(node.subscriber, ZMQ.Poller.POLLIN);
            loop.addPoller(sub, new ReceiveUpdate(), node);
            if (token_status) {
                loop.addTimer(HEARTBEAT_INTERVAL * 2, 0, new PrintStatus(), node);
            }

            loop.start();
        }
    }

    // -----------------------------------------------
    // Constructor
    Proxy() {
        // Initialize the context
        ctx = new ZContext();

        // Initialize the communication channels
        snapshot = ctx.createSocket(SocketType.DEALER);
        subscriber = ctx.createSocket(SocketType.SUB);

        // Connect to the main node
        snapshot.connect("tcp://localhost:" + MAIN_NODE_PORT);
        subscriber.connect("tcp://localhost:" + (MAIN_NODE_PORT + 1));

        // Initialize the hash ring
        hashring = new ConcurrentHashMap<Long, Integer>();
    }

    // Run the proxy
    private void run() {
        snapshot();

        // Start the central thread
        Object args[] = { this };
        Central central = new Central(args);
        central.start();

        // Wait for the central thread to finish
        try {
            central.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Main method
    public static void main(String[] args) {
        Proxy proxy = new Proxy();
        proxy.run();
    }
}
