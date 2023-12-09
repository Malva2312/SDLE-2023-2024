package node;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
//import org.zeromq.ZThread.IDetachedRunnable;

import database.Item;
import database.KeyValueDatabase;
import database.ShopList;
import java.time.Instant;

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

    private final static int alarm = 2 * 1000; // msecs
    private final static boolean show_stats = true;
    private final static boolean time_stats = true;

    private ZContext ctx;
    private ZLoop loop;

    Socket snapshot; // Request Snapshot from MainNode
    Socket subscriber; // Subscribe to all updates from MainNode
    Socket publisher; // Publish updates to MainNode

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
    private final static String END_OF_MESSAGE = "ENDOFMESSAGE";

    private int port;
    private Long token;

    private static int replicationFactor = 3;
    private static int readQuorum = 2;
    private static int writeQuorum = 2;

    private Socket client_router; // Router socket for clients
    private Socket dealer; // Dealer used by clients to send requests to nodes
    private Socket node_router; // Router socket for nodes

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
                return -1; // Interrupted

            if (kvMsg.getSequence() > 0) {
                if (kvMsg.getKey().startsWith(SUB_NODES + FLUSH_SIGNAL)) {
                    Long old_key = Long.parseLong(kvMsg.getKey().substring((SUB_NODES + FLUSH_SIGNAL).length()));
                    node.tokenAddrsMap.remove(old_key);
                } else if (kvMsg.getKey().startsWith(SUB_NODES)) {
                    System.out.println("Received update " + kvMsg.getKey());
                    node.storeTokenAddrsMap(kvMsg, SUB_NODES);
                } else {
                    System.out.println("E: bad request, aborting");
                    return -1;
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
            kvMsg.fmtKey("%s%d", SUB_NODES, node.getToken());
            kvMsg.fmtBody("%s", Integer.toString(node.getPort()));
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
        snapshot.sendMore(REQ_SNAPSHOT);
        snapshot.send(SUB_NODES);

        // Set timeout
        Long timeOut = System.currentTimeMillis() + REP_TIMEOUT;

        while (System.currentTimeMillis() > timeOut) { // Wait for snapshot
            kvmsg kvMsg = kvmsg.recv(snapshot);
            if (kvMsg == null) {
                break; // Interrupted
            }

            long sequence = kvMsg.getSequence();
            if (show_stats)
                System.out.printf("I: received snapshot=%d\n", sequence);
            // Check if snapshot is complete
            if (REP_SNAPSHOT.equalsIgnoreCase(kvMsg.getKey())) {
                kvMsg.destroy();
                return;
            }

            storeTokenAddrsMap(kvMsg, SUB_NODES);
        }
        System.out.println("E: failed to receive snapshot from broker, aborting");

        // End of snapshot request
    }

    private void storeTokenAddrsMap(kvmsg kvMsg, String subtree) {
        // Store token and address in tokenAddrsMap
        Long new_token = Long.parseLong(kvMsg.getKey().substring(subtree.length()));
        int new_addr = Integer.parseInt(new String(kvMsg.body(), ZMQ.CHARSET));
        this.tokenAddrsMap.put(new_token, new_addr);
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
                return -1; // Interrupted
            String sender = request.getProp("sender");
            if (!sender.equals(SNDR_NODE))
                return -1; //

            if (request.getKey().equals(READ_REQ)) {
                String db_key = request.getProp("db_key");
                kvmsg response = new kvmsg(0);
                ShopList shopList = node.kvdb.containsKey(db_key) ? (ShopList) node.kvdb.get(db_key) : new ShopList();

                response.setKey(READ_REP);
                response.setProp("sender", node.getToken().toString());
                response.setProp("db_key", db_key);
                response.setProp("timestamp", shopList.getTimeStamp());

                socket.sendMore(identity);
                response.send(socket);

                for (Entry<String, Item> entry : shopList.getItems().entrySet()) {
                    kvmsg r_item = new kvmsg(0);
                    r_item.setKey(READ_REP + db_key);
                    r_item.setProp("sender", node.getToken().toString());
                    r_item.setProp("item", entry.getKey());
                    r_item.setProp("quantity", Integer.toString(entry.getValue().getQuantity()));

                    socket.sendMore(identity);
                    r_item.send(socket);
                }

                kvmsg r_end = new kvmsg(0);
                r_end.setKey(END_OF_MESSAGE + db_key);

                socket.sendMore(identity);
                r_end.send(socket);

                response.destroy();
                r_end.destroy();
            } else if (request.getKey().equals(WRITE_REQ)) {
                String db_key = request.getProp("db_key");
                Instant timestamp = Instant.parse(request.getProp("timestamp"));

                String deleter = request.getProp("delete");
                if (deleter.equals("true")) {
                    node.kvdb.remove(db_key);
                    return 0;
                }
                ShopList shopList = node.kvdb.containsKey(db_key) ? (ShopList) (node.kvdb.get(db_key)) : new ShopList();
                ShopList copy = shopList.copy();

                if (shopList.getInstant().isBefore(timestamp)) {
                    shopList.setTimeStamp(timestamp);

                    while (true) {
                        kvmsg rc_item = kvmsg.recv(socket);
                        if (item == null)
                            return -1; // Interrupted
                        if (rc_item.getKey().equals(END_OF_MESSAGE + db_key)) {
                            node.kvdb.put(db_key, copy);

                            kvmsg response = new kvmsg(0);
                            response.setKey(WRITE_REP);
                            response.setProp("sender", node.getToken().toString());
                            response.setProp("status", "OK");

                            socket.sendMore(identity);
                            response.send(socket);
                            response.destroy();
                            break;
                        } else if (rc_item.getKey().equals(WRITE_REQ + db_key)) {
                            String item_name = rc_item.getProp("item");
                            int item_quantity = Integer.parseInt(rc_item.getProp("quantity"));
                            copy.addItem(item_name, item_quantity);
                        } else {
                            System.out.println("E: bad request, aborting");
                            return -1;
                        }
                    }
                }
            } else {
                System.out.println("E: bad request, aborting");
                return -1;
            }

            return 0;
        }
    }

    private static class ReceiveClientRequest implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {

            Node node = (Node) arg;
            Socket socket = item.getSocket(); // Router socket

            byte[] identity = socket.recv();
            kvmsg request = kvmsg.recv(socket);
            if (request == null)
                return -1; // Interrupted
            if (!request.getKey().startsWith(SNDR_CLIENT))
                return -1; //
            if (request.getKey().equals(READ_REQ)) {
                fowardReadRequest(node, socket, identity, request);
            } else if (request.getKey().equals(WRITE_REQ)) {
                fowardWriteRequest(node, socket, identity, request);
            } else {
                System.out.println("E: bad request, aborting");
                return -1;
            }
            return 0;
        }

        private void fowardReadRequest(Node node, Socket socket, byte[] identity, kvmsg request) {

            request.setProp("sender", SNDR_NODE);
            request.setProp("db_key", request.getProp("db_key"));
            Socket dealer = node.updateDealer();
            request.send(dealer);

            int read_count = 0;
            int read_quorum = node.getReadQuorum();

            Long timeout = System.currentTimeMillis() + REP_TIMEOUT;
            boolean rcv_self = false;
            String db_key = "";

            List<ShopList> shopLists = new ArrayList<>();
            ShopList shopList = null;

            while (System.currentTimeMillis() < timeout && read_count < read_quorum && !rcv_self) {
                kvmsg rcv = kvmsg.recv(node.dealer);
                if (rcv == null)
                    continue; // Interrupted
                if (rcv.getKey().equals(READ_REP)) {
                    if (rcv.getProp("sender").equals(node.getToken().toString())) {
                        rcv_self = true;
                    }
                    db_key = rcv.getProp("db_key");
                    String timestamp = rcv.getProp("timestamp");
                    shopList = new ShopList();
                    shopList.setTimeStamp(Instant.parse(timestamp));

                } else if (rcv.getKey().equals(READ_REP + db_key)) {
                    String item_name = rcv.getProp("item");
                    int item_quantity = Integer.parseInt(rcv.getProp("quantity"));
                    if (shopList != null) {
                        shopList.addItem(item_name, item_quantity);
                    }
                } else if (rcv.getKey().equals(END_OF_MESSAGE + db_key)) {
                    if (shopList != null) {
                        shopLists.add(shopList);
                    }
                    read_count++;
                } else {
                    System.out.println("E: bad request, aborting");
                    continue;
                }
            }

            // Choose the best shop list or combine them
            ShopList bestShopList = null;
            for (ShopList sl : shopLists) {
                if (bestShopList == null) {
                    bestShopList = sl;
                } else {
                    if (sl.getInstant().isAfter(bestShopList.getInstant())) {
                        bestShopList = sl;
                    }
                }
            }

            // Send response to client
            kvmsg response = new kvmsg(0);
            response.setKey(READ_REP);
            response.setProp("sender", node.getToken().toString());
            response.setProp("db_key", db_key);

            socket.sendMore(identity);
            response.send(socket);

            if (bestShopList != null){
                for (Entry<String, Item> entry : bestShopList.getItems().entrySet()) {
                    kvmsg r_item = new kvmsg(0);
                    r_item.setKey(READ_REP + db_key);
                    r_item.setProp("sender", node.getToken().toString());
                    r_item.setProp("item", entry.getKey());
                    r_item.setProp("quantity", Integer.toString(entry.getValue().getQuantity()));
    
                    socket.sendMore(identity);
                    r_item.send(socket);
                }
            }

            kvmsg r_end = new kvmsg(0);
            r_end.setKey(END_OF_MESSAGE + db_key);

            socket.sendMore(identity);
            r_end.send(socket);
        }

        private static void fowardWriteRequest(Node node, Socket socket, byte[] identity, kvmsg request) {
            Socket dealer = node.updateDealer();
            while (request.getKey().startsWith(WRITE_REQ)) {
                request.setProp("sender", SNDR_NODE);
                dealer.sendMore(identity);
                request.send(dealer);

                kvmsg rcv = kvmsg.recv(dealer);
                if (rcv == null)
                    return; // Interrupted
            }
            // End of write request
            if (request.getKey().startsWith(END_OF_MESSAGE)) {
                request.setProp("sender", SNDR_NODE);
                dealer.sendMore(identity);
                request.send(dealer);
            }
            else {
                System.out.println("E: bad request, aborting");
                return;
            }

            // Receive response from nodes
            int write_count = 0;
            int write_quorum = node.getWriteQuorum();
        
            Long timeout = System.currentTimeMillis() + REP_TIMEOUT;

            while (System.currentTimeMillis() < timeout && write_count < write_quorum) {
                kvmsg rcv = kvmsg.recv(dealer);
                if (rcv == null)
                    continue; // Interrupted
                if (rcv.getKey().equals(WRITE_REP)) {
                    if (rcv.getProp("status").equals("OK")) {
                        write_count++;
                    }
                }
            }
            if (write_count == write_quorum) {
                kvmsg response = new kvmsg(0);
                response.setKey(WRITE_REP);
                response.setProp("sender", node.getToken().toString());
                response.setProp("status", "OK");

                socket.sendMore(identity);
                response.send(socket);
            }
        }
    };

    // ----------------------------------------------
    public Socket updateDealer() {
        // Update dealer socket
        dealer.close();
        dealer = ctx.createSocket(SocketType.DEALER);
        for (int port : nextNodeAddr()) {
            dealer.connect("tcp://localhost:" + port + 1);
        }
        return this.dealer;
    }

    public int getWriteQuorum() {
        return writeQuorum;
    }

    public int getReadQuorum() {
        return readQuorum;
    }

    public List<Integer> nextNodeAddr() {
        List<Integer> addrs = new ArrayList<>();
        if (tokenAddrsMap.size() < getReplicationFactor()) {
            addrs.add(port + 1);
        } else {
            List<Long> ringKeys = new ArrayList<>(tokenAddrsMap.keySet());
            int keyIndex = ringKeys.indexOf(token);

            if (keyIndex != -1) {
                // Collect the next N keys in the ring
                for (int i = 1; i <= replicationFactor; i++) {
                    int nextIndex = (keyIndex + i) % ringKeys.size();
                    addrs.add(tokenAddrsMap.get(ringKeys.get(nextIndex)));
                }
            }
        }
        return addrs;
    }

    private int getReplicationFactor() {
        return replicationFactor;
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

    private static class node_worker implements Runnable {
        Socket node_router;
        ZLoop loop;

        public node_worker(Socket node_router, ZLoop loop) {
            this.node_router = node_router;
            this.loop = loop;
        }

        @Override
        public void run() {
            PollItem poller = new PollItem(node_router, ZMQ.Poller.POLLIN);
            loop.addPoller(poller, new ReceiveNodeRequest(), this);
            loop.start();
        }
    }

    public void run() {

        requestSnapshot();

        PollItem poller = new PollItem(subscriber, ZMQ.Poller.POLLIN);
        loop.addPoller(poller, new ReceiveUpdate(), this);

        loop.addTimer(HEARTBEAT, 0, new SendHeartBeat(), this);
        if (time_stats) {
            loop.addTimer(alarm, 0, new PrintStatus(), this);
        }

        poller = new PollItem(client_router, ZMQ.Poller.POLLIN);
        loop.addPoller(poller, new ReceiveClientRequest(), this);

        // Reeceive node requests in a separate thread
        Thread node_thread = new Thread(new node_worker(node_router, loop));
        node_thread.start();
        loop.start();

        ctx.close();

    }

    public static void main(String[] args) {
        // args should be: -p <port> -t <token>
        // args could be empty, then default values will be used
        // args could be in any order

        if (args.length <= 0 && args.length % 2 != 0 && args.length > 4) {
            System.out.println("Wrong arguments");
            return;
        }
        long token = 42;
        int port = 0;

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
