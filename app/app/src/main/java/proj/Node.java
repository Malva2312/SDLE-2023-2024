package proj;

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

public class Node {
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

    private int port = 5580;
    private long token = 0;
    private long sequence = 0;

    private Socket snapshot; // Sends snapshot request
    private Socket subscriber; // Collects hash ring updates
    private Socket pusher; // Publishes heartbeats

    // -----------------------------------------------
    // HashRing
    private ConcurrentHashMap<Long, Integer> hashring;
    // -----------------------------------------------
    // Database
    private KeyValueDatabase database;

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
    // Send heartbeats to the main node
    private static class Heartbeat implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Node node = (Node) arg;

            kvmsg heartbeat = new kvmsg(0);
            heartbeat.setKey(HEARTBEAT);
            heartbeat.setProp("token", Long.toString(node.token));
            heartbeat.setProp("port", Integer.toString(node.port));
            heartbeat.setProp("ttl", Integer.toString(TTL));
            heartbeat.send(node.pusher);

            System.out.println("Heartbeat sent");
            return 0;
        }
    }
    private static class ReceiveUpdate implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            // TODO Auto-generated method stub
            Node node = (Node) arg;
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
                    }
                    else {
                        System.out.println("E: token does not exist");
                        return 0;
                    }
                }
                catch (Exception e) {
                    System.out.println("E: bad request: token is not a number");
                    return 0;
                }
                return 0;
            }
            else if (update.getKey().equals(UPDATE)) {
                try {
                    Long token = Long.parseLong(update.getProp("token"));
                    int port = Integer.parseInt(update.getProp("port"));
                    // Add token to hash ring if it does not exist
                    if (!node.hashring.containsKey(token)) {
                        node.hashring.put(token, port);
                        System.out.println("Added node " + token + " to the hash ring");
                    }
                    else if (node.hashring.get(token) != port){
                        node.hashring.put(token, port);
                        System.out.println("Updated node " + token + " in the hash ring");
                    } 
                    else {
                        System.out.println("E: token already exists");
                        return 0;
                    }
                }
                catch (Exception e) {
                    System.out.println("E: bad request: token or port is not a number");
                    return 0;
                }
                return 0;
            }
            else {
                System.out.println("E: bad request: not an update");
                return 0;
            }
        }
    }
    private static class PrintStatus implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Node node = (Node) arg;
            System.out.println("\nChecking tokenAddrsMap");
            for (Long token : node.hashring.keySet()) {
                int addr = node.hashring.get(token);
                System.out.printf("Token: %d, Addr: tcp://localhost:%s\n", token, addr);
            }
            return 0;
        }
    }
    // -----------------------------------------------
    // Constructor
    public Node(long token, int port) {
        this.token = token;
        this.port = port;
        // Create the context and sockets
        ctx = new ZContext();
        loop = new ZLoop(ctx);

        // Connect to the main node
        snapshot = ctx.createSocket(SocketType.DEALER);
        subscriber = ctx.createSocket(SocketType.SUB);
        pusher = ctx.createSocket(SocketType.PUSH);
        snapshot.connect("tcp://localhost:" + (MAIN_NODE_PORT));
        subscriber.connect("tcp://localhost:" + (MAIN_NODE_PORT + 1));
        subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);
        pusher.connect("tcp://localhost:" + (MAIN_NODE_PORT + 2));
        
        // Create the hash ring
        hashring = new ConcurrentHashMap<Long, Integer>();

        // Instanciate the database
        database = new KeyValueDatabase();

    }
    // Run the node
    private void run(){
        snapshot();

        // Create the main loop
        ctx = new ZContext();
        loop = new ZLoop(ctx);

        PollItem sub = new PollItem(subscriber, ZMQ.Poller.POLLIN);
        loop.addPoller(sub, new ReceiveUpdate(), this);

        loop.addTimer(HEARTBEAT_INTERVAL, 0, new Heartbeat(),this);
        if (true){
            loop.addTimer(HEARTBEAT_INTERVAL * 2, 0, new PrintStatus(),this);
        }
        
        // Start the main loop
        loop.start();
    }
    // Main method
    public static void main(String[] args) {
        if (args.length < 0 && args.length % 2 != 0 && args.length > 4) {
            System.out.println("Wrong arguments");
            return;
        }
        long token = 42;
        int port = 5580;

        int i;
        for (i = 0; i < args.length; i += 2) {
            if (args[i].equals("-p")) {
                port = Integer.parseInt(args[i + 1]);
            } else if (args[i].equals("-t")) {
                token = Long.parseLong(args[i + 1]);
            } else {
                System.out.println("Wrong arguments");
                return;
            }
        }

        // System.out.println("Self Port: " + port);
        System.out.println("Token: " + token);
        System.out.println("Addr: " + String.format("tcp://*:%d", port));

        Node node = new Node(token, port);
        node.run();
    }
    // -----------------------------------------------
            /*
             * Long token = !request.getProp("token").equals("") ?
             * Long.parseLong(request.getProp("token")) : -1;
             * int port = !request.getProp("port").equals("") ?
             * Integer.parseInt(request.getProp("port")) : -1;
             * int ttl = !request.getProp("ttl").equals("") ?
             * Integer.parseInt(request.getProp("ttl")) : 0;
             * 
             * if ( token == -1 || port == -1) {
             * System.out.printf("E: bad request: invalid token or port\n");
             * return -1;
             * }
             if (node.hashring.containsKey(token)) {
                 System.out.println("E: token already exists");
                 return -1;
                }
            */
}
