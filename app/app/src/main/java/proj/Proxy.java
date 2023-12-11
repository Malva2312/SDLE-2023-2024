package proj;

import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

import org.checkerframework.checker.units.qual.h;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

public class Proxy {
    // -----------------------------------------------
    // Constants
    private final static int PROXY_PORT = 5555;
    private final static int MAIN_NODE_PORT = 5556;

    private final static int REPLICATION_FACTOR = 3;
    private final static int READ_QUORUM = 2;
    private final static int WRITE_QUORUM = 2;
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
    private final static int TIMEOUT = 1000 * 3; // msecs
    // -----------------------------------------------
    // Communication Channels
    private ZContext ctx;

    private long sequence = 0;

    private Socket snapshot; // Sends snapshot request
    private Socket subscriber; // Collects hash ring updates

    private Socket router; // Receives requests from clients
    // -----------------------------------------------
    // HashRing
    private ConcurrentHashMap<Long, Integer> hashring;
    // -----------------------------------------------
    // Debugging
    private static boolean token_status = false;
    private static boolean health_status = false;

    // -----------------------------------------------
    // Ring manipulation
    public static long hashStringToLong(String input) {
        // Use the hashCode() method of the String class
        int hashCode = input.hashCode();

        // Convert the hashCode to a positive long value
        long positiveHash = hashCode & 0xffffffffL;

        // Ensure the result is greater than 0
        return positiveHash > 0 ? positiveHash : -positiveHash;
    }

    private Long chooseTargetServer(String key) {

        // Hash funtion string -> long
        Long hash = hashStringToLong(key);

        // Find the index of the first token that is greater than hash
        List<Long> tokens = new ArrayList<Long>(hashring.keySet());
        int index = 0;
        for (Long t : tokens) {
            if (t > hash) {
                break;
            }
            index++;
        }

        // If index is out of bound, then the target server is the first server
        if (index == tokens.size()) {
            index = 0;
        }

        return tokens.get(index);
    }

    private List<Integer> getPorts(long token) {
        List<Integer> ports = new ArrayList<Integer>();
        int replication = REPLICATION_FACTOR;

        List<Long> tokens = new ArrayList<Long>(hashring.keySet());
        int idx = tokens.indexOf(token);
        System.out.println("TOKEN: " + new ArrayList<Integer>(hashring.values()) + "\t" + "Size: " + tokens.size()
                + "\t" + "IDX: " + idx);
        while (replication > 0) {
            if (idx == tokens.size()) {
                idx = 0;
            }
            ports.add(hashring.get(tokens.get(idx)));
            idx++;
            replication--;
        }
        // Remove duplicates
        ports = new ArrayList<Integer>(new HashSet<Integer>(ports));

        return ports;
    }

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
    // Handle requests from clients
    private static class HandleRequest implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            TokenRequest tokenThread = (TokenRequest) arg;
            kvmsg task = kvmsg.recv(item.getSocket());
            if (task == null)
                return 0; // Interrupted
            byte[] identity = ByteToString.decodeFromString(task.getProp("id"));
            
            kvmsg request = new kvmsg(0);
            request.setKey(task.getKey());
            request.setProp("db_key", task.getProp("db_key"));
            if (!task.getProp("timestamp").equals(""))
                request.setProp("timestamp", task.getProp("timestamp"));
            String body = new String(task.body(), ZMQ.CHARSET);
            request.fmtBody("%s", body);

            if (request.getKey().equals(READ)) {
                List<Integer> ports = tokenThread.node.getPorts(tokenThread.token);
                kvmsg[] reply = new kvmsg[ports.size()];
                List<SingleRequest> threads = new ArrayList<SingleRequest>();

                ports.remove(ports.indexOf(tokenThread.node.hashring.get(tokenThread.token)));
                threads.add(new SingleRequest(tokenThread.node.ctx, tokenThread.node.hashring.get(tokenThread.token),
                        request, reply, 0));
                threads.get(0).setName("Thread 0");
                threads.get(0).start();

                for (int i = 0; i < ports.size(); i++) {
                    System.out.println("Sending request to port: " + ports.get(i));
                    threads.add(new SingleRequest(tokenThread.node.ctx, ports.get(i), request, reply, i));
                    int idx = i +1;
                    threads.get(idx).setName("Thread " + Integer.toString(idx));
                    threads.get(idx).start();
                }

                int readQuorum = Math.min(READ_QUORUM, ports.size() + 1);
                System.out.println(threads.size() + "\t" + readQuorum);
                long timeout = System.currentTimeMillis() + TIMEOUT;
                boolean self = false;
                while (readQuorum > 0 && System.currentTimeMillis() < timeout && !self) {
                    if (!threads.get(0).isAlive()) {
                        if (reply[0] != null) {
                            if (reply[0].getKey().equals(READ_REP)) {
                                self = true;
                            }
                        }
                    }
                    for (int idx = 0; idx < threads.size(); idx++) {
                        if (!threads.get(idx).isAlive()) {
                            if (reply[idx] != null) {
                                if (reply[idx].getKey().equals(READ_REP)) {
                                    if (reply[idx].getProp("status").equals(OK)) {
                                        readQuorum--;
                                    }
                                }
                            } else {
                                // Resend request
                                threads.get(idx).start();
                            }
                        }
                    }
                }
                System.out.println("END of READ loop");

                // Solve conflicts
                if (reply[0] != null) {
                    tokenThread.node.router.sendMore(identity);
                    reply[0].send(tokenThread.node.router);
                }
                else {
                    System.out.println("E: no reply from self");

                    kvmsg replyMsg = new kvmsg(0);
                    replyMsg.setKey(READ_REP);
                    replyMsg.setProp("status", FAIL);
                    replyMsg.setProp("timestamp", "");
                    replyMsg.setProp("db_key", request.getProp("db_key"));

                    tokenThread.node.router.sendMore(identity);
                    replyMsg.send(tokenThread.node.router);

                }

            }
            else if (request.getKey().equals(WRITE)){
                List<Integer> ports = tokenThread.node.getPorts(tokenThread.token);
                kvmsg[] reply = new kvmsg[ports.size()];
                List<SingleRequest> threads = new ArrayList<SingleRequest>();

                for (int i = 0; i < ports.size(); i++) {
                    System.out.println("Sending request to port: " + ports.get(i));
                    threads.add(new SingleRequest(tokenThread.node.ctx, ports.get(i), request, reply, i));
                    threads.get(i).setName("Thread " + Integer.toString(i));
                    threads.get(i).start();
                }

                int readQuorum = Math.min(READ_QUORUM, ports.size());
                System.out.println(threads.size() + "\t" + readQuorum);
                long timeout = System.currentTimeMillis() + TIMEOUT;
                
                while (readQuorum > 0 && System.currentTimeMillis() < timeout) {
                    for (int idx = 0; idx < threads.size(); idx++) {
                        if (!threads.get(idx).isAlive()) {
                            if (reply[idx] != null) {
                                if (reply[idx].getKey().equals(READ_REP)) {
                                    if (reply[idx].getProp("status").equals(OK)) {
                                        readQuorum--;
                                    }
                                }
                            } else {
                                // Resend request
                                threads.get(idx).start();
                            }
                        }
                    }
                }
                System.out.println("END of WRIE loop");


                // Solve conflicts
                if (reply[0] != null) {
                    tokenThread.node.router.sendMore(identity);
                    reply[0].send(tokenThread.node.router);
                }
                else {
                    System.out.println("E: no reply from self");

                    kvmsg replyMsg = new kvmsg(0);
                    replyMsg.setKey(WRITE_REP);
                    replyMsg.setProp("status", FAIL);
                    replyMsg.setProp("timestamp", "");
                    replyMsg.setProp("db_key", request.getProp("db_key"));

                    tokenThread.node.router.sendMore(identity);
                    replyMsg.send(tokenThread.node.router);

                }

            }
            else {
                System.out.println("E: bad request: not a read/write request");
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

    // Main Worker
    private static class MainWorker extends Thread {
        Proxy node;
        ConcurrentHashMap<Long, Thread> workers;

        MainWorker(Proxy node) {
            this.node = node;
        }

        @Override
        public void run() {
            node.router = node.ctx.createSocket(SocketType.ROUTER);
            node.router.bind("tcp://*:" + PROXY_PORT);
            workers = new ConcurrentHashMap<Long, Thread>();
            System.out.println("Main worker started on port: " + PROXY_PORT);

            while (!Thread.currentThread().isInterrupted()) {
                byte[] identity = node.router.recv(0);
                if (identity == null)
                    break; // Interrupted
                kvmsg request = kvmsg.recv(node.router);
                if (request == null)
                    break; // Interrupted
                if (node.hashring.isEmpty()) {
                    System.out.println("E: hash ring is empty");
                    kvmsg replyMsg = new kvmsg(0);
                    replyMsg.setKey(request.getKey() + "_REP");
                    replyMsg.setProp("status", FAIL);
                    replyMsg.setProp("db_key", request.getProp("db_key"));

                    node.router.sendMore(identity);
                    replyMsg.send(node.router);
    
                    continue;
                }
                String key = request.getProp("db_key");
                Long token = node.chooseTargetServer(key);
                if (workers.containsKey(token)) {
                    if (!workers.get(token).isAlive()) {
                        workers.remove(token);
                        workers.put(token, new TokenRequest(new Object[] { node, token }));
                        workers.get(token).setName("Token Thread" + token);
                        workers.get(token).start();
                    }
                } else {
                    workers.put(token, new TokenRequest(new Object[] { node, token }));
                    workers.get(token).setName("Token Thread" + token);
                    workers.get(token).start();
                }
                kvmsg task = new kvmsg(0);
                System.out.println("Sending request " + request.getKey() + " to token: " + token);
                task.setKey(request.getKey());
                task.setProp("db_key", new String(request.getProp("db_key")));
                if (!request.getProp("timestamp").equals(""))
                    task.setProp("timestamp", request.getProp("timestamp"));

                task.setProp("id", ByteToString.encodeToString(identity));
                String body = new String(request.body(), ZMQ.CHARSET);
                task.fmtBody("%s", body);

                Socket boss = node.ctx.createSocket(SocketType.PUSH);
                boss.connect("ipc://" + Long.toString(token) + ".ipc");
                task.send(boss);

            }
        }
    }

    // Thread to handle server communication
    private static class TokenRequest extends Thread {
        Object args[];
        Proxy node;
        long token;
        Socket puller;

        TokenRequest(Object args[]) {
            this.args = args;
            this.token = (long) args[1];

            node = (Proxy) args[0];
            this.puller = node.ctx.createSocket(SocketType.PULL);
            try {
                this.puller.bind("ipc://" + Long.toString(token) + ".ipc");
                System.out.println("Token request worker started on token: " + token);
            } catch (Exception e) {
                System.out.println("E: token already exists");
                return;
            }
        }

        @Override
        public void run() {
            Proxy node = (Proxy) args[0];
            ZLoop loop = new ZLoop(node.ctx);
            PollItem pull = new PollItem(this.puller, ZMQ.Poller.POLLIN);
            loop.addPoller(pull, new HandleRequest(), this);
            loop.start();
        }
    }

    // Single request worker
    private static class SingleRequest extends Thread {
        ZContext ctx;
        Socket dealer;
        kvmsg request;
        int port = 0;
        kvmsg[] reply;
        int idx;

        SingleRequest(ZContext ctx, int port, kvmsg request, kvmsg[] reply, int idx) {
            this.ctx = ctx;
            this.port = port;
            this.request = request;
            this.reply = reply;
            this.idx = idx;
        }

        @Override
        public void run() {
            this.dealer = ctx.createSocket(SocketType.DEALER);
            this.dealer.connect("tcp://localhost:" + port);
            System.out.println("THREAD OPENED ON PORT: " + port);

            request.send(dealer);

            // Set up a poller to wait for responses with a timeout
            ZMQ.Poller poller = ctx.createPoller(1);
            poller.register(this.dealer, ZMQ.Poller.POLLIN);

            request.send(dealer);

            // Wait for a response or timeout
            if (poller.poll(1000 /* timeout in milliseconds */) == 0) {
                // No response within the timeout, decide what to do (close the thread, retry,
                // etc.)
                System.out.println("Timeout reached. Closing thread on port: " + port);
            } else {
                // There is a response, proceed with handling it
                this.reply[idx] = kvmsg.recv(this.dealer);
                System.out.println("Received reply from THREAD on port: " + port);
            }

            // Clean up resources
            poller.close();
            dealer.close();
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
        subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);

        // Initialize the hash ring
        hashring = new ConcurrentHashMap<Long, Integer>();
    }

    // Run the proxy
    private void run() {
        snapshot();

        // Start the central thread
        Object args[] = { this };
        Central central = new Central(args);
        MainWorker mainWorker = new MainWorker(this);

        central.setName("Central");
        mainWorker.setName("MainWorker");

        central.start();
        mainWorker.start();

        // Wait for the central thread to finish
        while (true) {
            if (!central.isAlive()) {
                System.out.println("Central thread finished");
                mainWorker.interrupt();
                break;
            }
            if (!mainWorker.isAlive()) {
                System.out.println("Main worker finished");
                central.interrupt();
                break;
            }
        }
    }

    // Main method
    public static void main(String[] args) {
        Proxy proxy = new Proxy();
        proxy.run();
    }
}
