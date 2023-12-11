package node;

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

public class Node {
    // -----------------------------------------------
    // Constants
    private final static int MAIN_NODE_PORT = 5556;
    // -----------------------------------------------
    // Predifined Messages
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

    private int port = 5580; // Default port
    private long token = 0; // Default token
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
    // Debugging
    private static boolean token_status = false;
    private static boolean health_status = false;

    // -----------------------------------------------
    // Send heartbeats to the main node
    private static class Heartbeat implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Node node = (Node) arg;

            kvmsg heartbeat = new kvmsg(node.sequence);
            heartbeat.setKey(HEARTBEAT);
            heartbeat.setProp("token", Long.toString(node.token));
            heartbeat.setProp("port", Integer.toString(node.port));
            heartbeat.setProp("ttl", Integer.toString(TTL));
            heartbeat.send(node.pusher);

            if (health_status) {
                System.out.println("Sending heartbeat");
            }
            return 0;
        }
    }

    // Receive updates from the main node
    private static class ReceiveUpdate implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Node node = (Node) arg;
            kvmsg update = kvmsg.recv(node.subscriber);
            if (update == null)
                return -1; // Interrupted

            node.sequence = update.getSequence();

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
    // Database related requests
    private static class DataRequests implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            System.out.println("Handling data request");
            Node node = (Node) arg;
            Socket w_router = item.getSocket();

            byte[] identity = w_router.recv();

            kvmsg request = kvmsg.recv(w_router);
            if (request == null) {
                System.out.println("E: bad request: null request");
                return -1; // Interrupted
            }
            if (request.getKey().equals(READ)) {
                try {
                    String key = request.getProp("db_key");
                    if (key.equals("")) {
                        // raise exception to be caught
                        throw new Exception("E: bad request: empty key");
                    }

                    ShopList value = node.database.containsKey(key) ? (ShopList) node.database.get(key) : null;
                    if (value == null) {
                        value = new ShopList();
                        value.setTimeStamp(Instant.MIN);
                    }
                    kvmsg reply = new kvmsg(0);
                    reply.setKey(READ_REP);
                    reply.setProp("db_key", key);
                    reply.setProp("timestamp", value.getInstant().toString());

                    String body = ShopList.serialize(value);
                    reply.fmtBody("%s", body);

                    reply.setProp("status", OK);
                    w_router.sendMore(identity);
                    reply.send(w_router);

                } catch (Exception e) {
                    kvmsg reply = new kvmsg(0);
                    reply.setKey(READ_REP);
                    reply.setProp("status", FAIL);

                    System.out.println("E: bad read request: key does not exist");
                    return 0;
                }
            } else if (request.getKey().equals(WRITE)) {
                try {
                    String key = request.getProp("db_key");
                    if (key.equals("")) {
                        // raise exception to be caught
                        throw new Exception("E: bad request: empty key");
                    }
                    String body = new String(request.body(), ZMQ.CHARSET);
                    ShopList value = ShopList.deserialize(body); // Items

                    // Update the timestamp
                    String timestamp = request.getProp("timestamp");
                    System.out.println(timestamp);
                    value.setTimeStamp(Instant.parse(timestamp));
                    
                    // Read the current value
                    ShopList currentValue = node.database.containsKey(key) ? (ShopList) node.database.get(key) : null;
                    value = ShopList.merge(currentValue, value);
                    
                    // Update the database
                    node.database.put(key, value);
                    System.out.println("Updated database on key " + key);

                    kvmsg reply = new kvmsg(0);
                    reply.setKey(WRITE_REP);
                    reply.setProp("db_key", key);
                    reply.setProp("timestamp", value.getInstant().toString());
                    reply.setProp("status", OK);
                    w_router.sendMore(identity);
                    reply.send(w_router);

                } catch (Exception e) {
                    System.out.println("E: bad write request: key does not exist");

                    kvmsg reply = new kvmsg(0);
                    reply.setKey(WRITE_REP);
                    reply.setProp("status", FAIL);
                    w_router.sendMore(identity);
                    reply.send(w_router);
                    return 0;
                }
            } else {
                System.out.println("E: bad request: not a data request");
                return 0;

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
            Node node = (Node) args[0];
            ZLoop loop = new ZLoop(node.ctx);

            PollItem sub = new PollItem(node.subscriber, ZMQ.Poller.POLLIN);
            loop.addPoller(sub, new ReceiveUpdate(), node);

            loop.addTimer(HEARTBEAT_INTERVAL, 0, new Heartbeat(), node);
            if (token_status) {
                loop.addTimer(HEARTBEAT_INTERVAL * 2, 0, new PrintStatus(), node);
            }

            loop.start();
        }
    }

    // Thread to handle requests
    private static class Worker extends Thread {
        Object args[];

        Worker(Object args[]) {
            this.args = args;
        }

        @Override
        public void run() {
            Node node = (Node) args[0];
            ZLoop loop = new ZLoop(node.ctx);

            Socket w_router = node.ctx.createSocket(SocketType.ROUTER);
            System.out.println("Binding to port " + node.port);
            w_router.bind("tcp://*:" + node.port);
            PollItem poller = new PollItem(w_router, ZMQ.Poller.POLLIN);
            loop.addPoller(poller, new DataRequests(), node);

            loop.start();
        }

    }

    // -----------------------------------------------
    // Constructor
    public Node(long token, int port) {
        this.token = token;
        this.port = port;
        // Create the context and sockets
        ctx = new ZContext();

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
    private void run() {
        //snapshot();

        // Assert threads to variables
        Central central = new Central(new Object[] { this });
        Worker worker = new Worker(new Object[] { this });

        central.setName("Central");
        worker.setName("Worker");

        // Start the threads
        central.start();
        worker.start();

        // Wait for the threads to finish
        try {
            while (central.isAlive() && worker.isAlive()) {
                // Wait for one of the threads to finish
            }
            // kill the other thread
            if (central.isAlive()) {
                central.interrupt();
            } else if (worker.isAlive()) {
                worker.interrupt();
            }
            ctx.close();
        } catch (Exception e) {
            e.getStackTrace();
        }
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
}
