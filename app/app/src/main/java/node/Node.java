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

public class Node {
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

    Long sequence = 0L;

    // ----------------------------------------------
    private ConcurrentHashMap<Long, Integer> tokenAddrsMap;

    // ----------------------------------------------
    // Node interaction for database requests
    private final static String SNDR_CLIENT = "/CLIENT/";
    private final static String SNDR_NODE = "/RING/";

    private final static String READ_REQ = "READ";
    private final static String READ_REP = "READREP";
    private final static String WRITE_REQ = "WRITE";
    private final static String WRITE_REP = "WRITEREP";
    private final static String WRITE_FAIL = "WRITEFAIL";
    private final static String READ_FAIL = "READFAIL";

    private int port;
    private Long token;

    private static int replicationFactor = 3;
    private static int readQuorum = 2;
    private static int writeQuorum = 2;

    private Socket client_router; // Router socket for clients
    private Socket dealer; // Dealer used by clients to send requests to nodes
    private Socket node_router; // Router socket for nodes // Initiated in a separate thread

    private KeyValueDatabase kvdb = new KeyValueDatabase();
    // ----------------------------------------------

    // ----------------------------------------------
    // Main node interaction for node state
    private static class ReceiveUpdate implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {

            Node node = (Node) arg;

            kvmsg kvMsg = kvmsg.recv(node.subscriber);
            if (kvMsg == null)
                return 0; // Interrupted

            if (kvMsg.getSequence() > 0) {
                node.sequence = kvMsg.getSequence();

                if (kvMsg.getKey().startsWith(SUB_NODES + FLUSH_SIGNAL)) {
                    Long old_key = Long.parseLong(kvMsg.getProp("token"));
                    System.out.println("I: flush token " + old_key);
                    node.tokenAddrsMap.remove(old_key);
                } else if (kvMsg.getKey().startsWith(SUB_NODES)) {
                    Long t = Long.parseLong(kvMsg.getProp("token"));
                    int addr = Integer.parseInt(kvMsg.getProp("addr"));

                    if (node.tokenAddrsMap.containsKey(t)) {
                        if (node.tokenAddrsMap.get(t) != addr) {
                            System.out.println("I: update token " + t);
                            node.tokenAddrsMap.replace(t, addr);
                        }
                    } else {
                        System.out.println("I: new token " + t);
                        node.tokenAddrsMap.put(t, addr);
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

    private static class SendHeartBeat implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Node node = (Node) arg;

            kvmsg kvMsg = new kvmsg(0);
            kvMsg.fmtKey("%s", SUB_NODES + node.token.toString());
            kvMsg.fmtBody("%s", SUB_NODES);
            kvMsg.setProp("token", node.getToken().toString());
            kvMsg.setProp("addr", Integer.toString(node.getPort()));
            kvMsg.setProp("ttl", "%d", TTL);
            kvMsg.send(node.publisher);
            kvMsg.destroy();

            return 0;
        }
    }

    private static class PrintStatus implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Node node = (Node) arg;
            System.out.println("\nChecking tokenAddrsMap");
            for (Long token : node.tokenAddrsMap.keySet()) {
                int addr = node.tokenAddrsMap.get(token);
                System.out.printf("Token: %d, Addr: tcp://localhost:%s\n", token, addr);
            }
            return 0;
        }
    }

    private void requestSnapshot() {
        // get state snapshot
        kvmsg kvMsg = new kvmsg(0);
        kvMsg.setKey(REQ_SNAPSHOT);
        kvMsg.setProp("subtree", SUB_NODES);
        kvMsg.setProp("token", Long.toString(this.token));
        kvMsg.setProp("addr", Integer.toString(port));
        kvMsg.setProp("ttl", "%d", TTL);
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
                this.sequence = kvMsg.getSequence();
                kvMsg.destroy();
                break;
            }
        }
    }

    // ----------------------------------------------

    // ----------------------------------------------
    // Node interaction for database requests
    private static class ReceiveNodeRequest implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {

            Node node = (Node) arg;
            Socket socket = item.getSocket(); // Router socket

            byte[] identity = socket.recv();
            kvmsg request = kvmsg.recv(socket);
            if (request == null)
                return 0; // Interrupted
            // Check if the request have the correct needed properties
            if (request.getProp("sender").equals("") || request.getProp("db_key").equals("")) {
                System.out.println("E: bad request in node, aborting:");

                kvmsg response = new kvmsg(0);
                response.setKey(request.getKey() + "REP");
                response.setProp("sender", node.getToken().toString());
                if (request.getKey().equals(READ_REQ)) {
                    response.setProp("status", READ_FAIL);
                } else if (request.getKey().equals(WRITE_REQ)) {
                    response.setProp("status", WRITE_FAIL);
                }
                socket.sendMore(identity);
                response.send(socket);

                return 0;
            }

            if (!request.getProp("sender").equals(SNDR_NODE)) {
                return 0;
            }
            if (request.getKey().startsWith(WRITE_REQ)) {
                // Check needed properties
                if (request.getProp("timestamp").equals("")) {
                    System.out.println("E: bad write request in node, aborting:");

                    kvmsg response = new kvmsg(0);
                    response.setKey(WRITE_REP);
                    response.setProp("sender", node.getToken().toString());
                    response.setProp("status", WRITE_FAIL);

                    socket.sendMore(identity);
                    response.send(socket);
                    return 0;
                }

                ShopList shopList = new ShopList();

                String db_key = request.getProp("db_key");
                String delete = request.getProp("delete");

                if (delete.equals("true") && node.kvdb.containsKey(db_key)) {
                    node.kvdb.remove(delete);
                    kvmsg response = new kvmsg(0);
                    response.setKey(WRITE_REP);
                    response.setProp("db_key", delete);
                    response.setProp("sender", node.getToken().toString());
                    response.setProp("status", "OK");

                    socket.sendMore(identity);
                    response.send(socket);
                    return 0;
                }

                Instant timestamp = Instant.parse(request.getProp("timestamp"));
                shopList.setTimeStamp(timestamp);

                int n_items = Integer.parseInt(request.getProp("items"));
                String[] items = new String(request.body(), ZMQ.CHARSET).split("\n");
                for (int i = 0; i < n_items; i++) {
                    String item_name = items[i * 2];
                    int item_quantity = Integer.parseInt(items[i * 2 + 1]);
                    shopList.addItem(item_name, item_quantity);
                }

                try {
                    node.kvdb.put(db_key, shopList); // Update database // Check for conflicts
                    kvmsg response = new kvmsg(0);
                    response.setKey(WRITE_REP);
                    response.setProp("db_key", db_key);
                    response.setProp("sender", node.getToken().toString());
                    response.setProp("status", "OK");

                    socket.sendMore(identity);
                    response.send(socket);

                } catch (Exception e) {

                    kvmsg response = new kvmsg(0);
                    response.setKey(WRITE_REP);
                    response.setProp("db_key", db_key);
                    response.setProp("sender", node.getToken().toString());
                    response.setProp("status", WRITE_FAIL);

                    socket.sendMore(identity);
                    response.send(socket);

                    System.out.println("E: bad WRITE request in node, aborting:");
                    System.out.println("\tKey: " + db_key);
                    return 0;
                }
            } else if (request.getKey().startsWith(READ_REQ)) {
                String db_key = request.getProp("db_key");
                ShopList shopList = node.kvdb.containsKey(db_key) ? (ShopList) node.kvdb.get(db_key) : new ShopList();
                kvmsg response = new kvmsg(0);
                response.setKey(READ_REP);
                response.setProp("sender", node.getToken().toString());
                response.setProp("db_key", db_key);
                response.setProp("timestamp", "%s", shopList.getInstant().toString());
                response.setProp("items", Integer.toString(shopList.getItems().size()));

                String items = "";
                for (String list_item : shopList.getItems().keySet()) {
                    items += list_item + "\n";
                    items += Integer.toString(shopList.getItems().get(list_item).getQuantity()) + "\n";
                }
                response.fmtBody("%s", items);
                response.setProp("status", "OK");

                socket.sendMore(identity);
                response.send(socket);
            } else {
                System.out.println("E: bad request, aborting");
                System.out.println("\tKey: " + request.getKey());
                return 0;
            }
            System.out.println("Received request from Node " + request.getKey() + " " + request.getProp("status"));
            return 0;
        }
    }

    private static class ReceiveClientRequest implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {

            System.out.println("Received request from Client");

            Node node = (Node) arg;
            Socket socket = item.getSocket(); // Router socket

            byte[] identity = socket.recv();
            kvmsg request = kvmsg.recv(socket);
            if (request == null)
                return 0; // Interrupted
            if (!request.getProp("sender").equals(SNDR_CLIENT))
                return 0; //
      
            if (request.getKey().equals(READ_REQ)) {
                if (request.getProp("sender").equals("") || request.getProp("db_key").equals("")) {
                    System.out.println("E: bad read request from client, aborting:");

                    kvmsg response = new kvmsg(0);
                    response.setKey(READ_REP);
                    response.setProp("sender", node.getToken().toString());
                    response.setProp("status", READ_FAIL);

                    socket.sendMore(identity);
                    response.send(socket);
                    return 0;
                }
                fowardReadRequest(node, socket, identity, request);
            } else if (request.getKey().equals(WRITE_REQ)) {
                if (request.getProp("sender").equals("") || request.getProp("db_key").equals("")
                    || request.getProp("timestamp").equals("") ) {
                    System.out.println("E: bad write request from client, aborting:");

                    kvmsg response = new kvmsg(0);
                    response.setKey(WRITE_REP);
                    response.setProp("sender", node.getToken().toString());
                    response.setProp("status", WRITE_FAIL);

                    socket.sendMore(identity);
                    response.send(socket);
                    return 0;
                }
                fowardWriteRequest(node, socket, identity, request);
            } else {
                System.out.println("E: bad request, aborting");
                System.out.println("\tCould not identify request type " + request.getKey());
                return 0;
            }
            return 0;
        }

        private void fowardReadRequest(Node node, Socket socket, byte[] identity, kvmsg request) {
            System.out.println("Foward read request");
            List<Integer> nextNodesAddr = new ArrayList<>(node.nextNodesAddr());
            int readQuorum = node.getReadQuorum();
            int replicationFactor = node.getReplicationFactor();
            if (nextNodesAddr.isEmpty()) {
                System.out.println("E: no nodes available");
                return;
            } else if (nextNodesAddr.size() < node.getReplicationFactor()) {
                replicationFactor = nextNodesAddr.size();
                readQuorum = (replicationFactor - 1) < 1 ? 1 : replicationFactor - 1;
            }

            kvmsg[] responses = new kvmsg[replicationFactor];

            kvmsg readRequest = new kvmsg(0);
            readRequest.setKey(READ_REQ);
            readRequest.setProp("db_key", request.getProp("db_key"));
            readRequest.setProp("sender", SNDR_NODE);
            readRequest.fmtBody("%s", new String(request.body(), ZMQ.CHARSET));

            System.out.println("Adress to foword the request : " + nextNodesAddr);

            // remove self addr from nextNodesAddr
            if (nextNodesAddr.contains(node.getPort())) {
                nextNodesAddr.remove((Integer) node.getPort());
            }

            List<Thread> threads = new ArrayList<>();

            // Add self to threads
            threads.add(new Thread(() -> {
                responses[0] = node_thread(node.ctx, node.getPort() + 1, readRequest);
            }));

            // Add other nodes to threads
            for (int i = 1; i < replicationFactor; i++) {
                int port = nextNodesAddr.get(i - 1);
                final int index = i;
                Thread thread = new Thread(() -> {
                    responses[index] = node_thread(node.ctx, port + 1, readRequest);
                });
                thread.setName("Node_" + node.token + "_ON_PORT_" + port);
                threads.add(thread);
            }

            // Start all threads
            for (Thread thread : threads) {
                thread.start();
            }

            // wait for the first thread to finish
            List<kvmsg> responsesList = new ArrayList<>();

            try {
                threads.get(0).join();
                responsesList.add(responses[0]);
                readQuorum -= 1;

                while (readQuorum > 0) {
                    for (Thread thread : threads) {
                        if (!thread.isAlive()) {
                            responsesList.add(responses[threads.indexOf(thread)]);
                            readQuorum -= 1;
                        }
                    }
                }

            } catch (InterruptedException e) {
                System.out.println("Thread interrupted error");
                //e.printStackTrace();
            } catch (Exception e) {
                System.out.println("Thread error");
                e.printStackTrace();
            }

            // Kill all remaining threads
            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    thread.interrupt();
                }
            }

            if (responsesList.size() > 0 && responsesList.get(0) != null) {
                // Solve conflicts

                // Count nulls in responsesList
                int nulls = 0;
                for (kvmsg response : responsesList) {
                    if (response == null) {
                        nulls += 1;
                    }
                }
                if (nulls > 0) {
                    System.out.println("Could not assure data consistency: Null responses:" + nulls);
                } else {
                    System.err.println("Solved read conflicts");
                    socket.sendMore(identity);
                    responsesList.get(0).send(socket);
                    return;
                }
            }

            kvmsg response = new kvmsg(0);
            response.setKey(READ_REP);
            response.setProp("sender", node.getToken().toString());
            response.setProp("db_key", request.getProp("db_key"));
            response.setProp("timestamp", "%s", Instant.now().toString());
            response.setProp("items", Integer.toString(0));
            response.setProp("status", READ_FAIL);

            socket.sendMore(identity);
            response.send(socket);

            System.out.println("Sent bad read response");
            return;

        }

        private static void fowardWriteRequest(Node node, Socket socket, byte[] identity, kvmsg request) {
            System.out.println("Foward write request");
            List<Integer> nextNodesAddr = new ArrayList<>(node.nextNodesAddr());
            int writeQuorum = node.getWriteQuorum();
            int replicationFactor = node.getReplicationFactor();
            if (nextNodesAddr.isEmpty()) {
                System.out.println("E: no nodes available");
                return;
            } else if (nextNodesAddr.size() < node.getReplicationFactor()) {
                replicationFactor = nextNodesAddr.size();
                writeQuorum = replicationFactor - 1;
            }

            kvmsg[] responses = new kvmsg[replicationFactor];

            kvmsg writeRequest = new kvmsg(0);
            writeRequest.setKey(WRITE_REQ);
            writeRequest.setProp("db_key", request.getProp("db_key"));
            writeRequest.setProp("sender", SNDR_NODE);
            writeRequest.setProp("timestamp", "%s", request.getProp("timestamp"));
            writeRequest.setProp("items", request.getProp("items"));
            writeRequest.fmtBody("%s", new String(request.body(), ZMQ.CHARSET));

            List<Thread> threads = new ArrayList<>();

            System.out.println("Adress to foword the request : " + nextNodesAddr);
            // Add other nodes to threads
            for (int i = 0; i < replicationFactor; i++) {
                int port = nextNodesAddr.get(i);
                final int index = i;
                Thread thread = new Thread(() -> {
                    responses[index] = node_thread(node.ctx, port + 1, writeRequest);
                });
                thread.setName("Node_" + node.token + "_ON_PORT_" + port);
                threads.add( thread);
            }

            // Start all threads
            for (Thread thread : threads) {
                thread.start();
            }

            // wait for the firsts writeQuorum threads to finish
            List<kvmsg> responsesList = new ArrayList<>();

            try {
                while (writeQuorum > 0) {
                    for (int i = 0; i < threads.size(); i++) {
                        Thread thread = threads.get(i);
                        if (!thread.isAlive()) {
                            responsesList.add(responses[i]);
                            writeQuorum -= 1;
                        }
                    }
                }

                if (responsesList.size() > 0 && responsesList.get(0) != null) {
                    // Solve conflicts

                    // Count nulls in responsesList
                    int nulls = 0;
                    for (kvmsg response : responsesList) {
                        if (response == null) {
                            nulls += 1;
                        }
                    }
                    if (nulls > 0) {
                        System.out.println("Could not assure data consistency: Null responses:" + nulls);
                    } else {
                        System.err.println("Solved write conflicts");
                        socket.sendMore(identity);
                        responsesList.get(0).send(socket);
                        return;
                    }
                    // Solve conflicts
                    socket.sendMore(identity);
                    responsesList.get(0).send(socket);
                    return;
                }
                else {
                    System.out.println("WRITE ERROR");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            kvmsg response = new kvmsg(0);
            response.setKey(WRITE_REP);
            response.setProp("sender", node.getToken().toString());
            response.setProp("db_key", request.getProp("db_key"));
            response.setProp("status", WRITE_FAIL); 

            socket.sendMore(identity);
            response.send(socket);

        }

        private static kvmsg node_thread(ZContext ctx, int port, kvmsg readRequest) {

            System.out.println("THREAD OPENED ON PORT: " + port);

            Socket dealer = ctx.createSocket(SocketType.DEALER);
            dealer.connect("tcp://localhost:" + port);

            readRequest.send(dealer);
            ZMQ.Poller poller = ctx.createPoller(1);
            poller.register(dealer, ZMQ.Poller.POLLIN);

            if (poller.poll(5000) > 0) {
                // Handle the response from the router
                System.out.println("Client received response from node in port" + port);
                return kvmsg.recv(dealer);
            } else {
                System.out.println("Client timed out in port" + port);
                return null;
            }
        }

    };

    // ----------------------------------------------
    private int getReplicationFactor() {
        return replicationFactor;
    }

    public int getWriteQuorum() {
        return writeQuorum;
    }

    public int getReadQuorum() {
        return readQuorum;
    }

    public List<Integer> nextNodesAddr() {
        List<Long> neeededTokens = new ArrayList<>();
        List<Long> ring = new ArrayList<>(tokenAddrsMap.keySet());

        System.out.println("Search for token: " + token);
        int index = ring.indexOf(token);

        for (int i = 0; i < getReplicationFactor(); i++) {
            index = (index + 1) % ring.size();
            neeededTokens.add(ring.get(index));
        }

        // Remove duplicates
        neeededTokens = neeededTokens.stream().distinct().toList();

        return neeededTokens.stream().map(token -> tokenAddrsMap.get(token)).toList();
    }

    public int getPort() {
        return port;
    }

    public Long getToken() {
        return token;
    }
    // ----------------------------------------------

    public Node(int port, Long token) {
        this.port = port;
        this.token = (Long) token;

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
        // ----------------------------------------------

        // Node interaction for database requests ------
        client_router = ctx.createSocket(SocketType.ROUTER);
        client_router.bind(String.format("tcp://*:%d", port));

        node_router = ctx.createSocket(SocketType.ROUTER);
        node_router.bind(String.format("tcp://*:%d", port + 1));

        dealer = ctx.createSocket(SocketType.DEALER);
        dealer.connect("tcp://localhost:" + (port + 1));
        // ----------------------------------------------
    }

    private static class node_worker implements IDetachedRunnable {
        @Override
        public void run(Object[] args) {
            Node node = (Node) args[0];

            ZLoop loop = new ZLoop(node.ctx);
            loop.verbose(false);
            PollItem poller = new PollItem(node.node_router, ZMQ.Poller.POLLIN);
            loop.addPoller(poller, new ReceiveNodeRequest(), node);
            loop.start();
        }
    }

    private static class client_worker implements IDetachedRunnable {
        @Override
        public void run(Object[] args) {
            Node node = (Node) args[0];
            ZLoop loop = new ZLoop(node.ctx);
            loop.verbose(false);

            PollItem poller = new PollItem(node.client_router, ZMQ.Poller.POLLIN);
            loop.addPoller(poller, new ReceiveClientRequest(), node);

            loop.start();
        }
    }

    public void run() {

        ZLoop loop = new ZLoop(ctx);
        loop.verbose(false);

        requestSnapshot();

        PollItem poller = new PollItem(subscriber, ZMQ.Poller.POLLIN);
        loop.addPoller(poller, new ReceiveUpdate(), this);

        loop.addTimer(HEARTBEAT, 0, new SendHeartBeat(), this);
        if (time_stats) {
            loop.addTimer(alarm, 0, new PrintStatus(), this);
        }

        ZThread.start(new node_worker(), this);
        ZThread.start(new client_worker(), this);

        loop.start();

        ctx.close();

    }

    public static void main(String[] args) {
        // args should be: -p <port> -t <token>
        // args could be empty, then default values will be used
        // args could be in any order

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

        Node node = new Node(port, token);
        node.run();
    }
}
