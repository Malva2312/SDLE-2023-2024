package node;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;


import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;

import database.Item;
import database.KeyValueDatabase;
import database.ShopList;
import java.time.Instant;

public class Node {
    private final static int MAIN_NODE_PORT = 5556;
    
    private final static String REQ_SNAPSHOT = "ALLNODES?";
    private final static String REP_SNAPSHOT = "REPNODESSNAP";
    
    private final static String SUB_NODES = "/NODE/";
    private final static String FLUSH_SIGNAL = "/FLUSH/";

    private final static String REP_READ = "/READ/"; // + key
    private final static String REP_READ_ITEM = "/READ_ITEM/"; // + key
    private final static String REP_READ_END = "/READ_END/"; // + key
    
    
    private final static int HEARTBEAT = 5 * 1000; //  msecs
    private final static int TTL = 2 * HEARTBEAT; //  msecs
    
    private final int alarm = 2 * 1000; //  msecs
    private final boolean show_stats = true;
    private final boolean time_stats = true;

    private ZContext ctx;
    private ZLoop loop;

    Socket snapshot;    // Request Snapshot from MainNode
    Socket subscriber;  // Subscribe to all updates from MainNode
    Socket publisher;   // Publish updates to MainNode

    int port;
    Long token;
    String addr;
    private ConcurrentHashMap<Long, String> tokenAddrsMap;

    private KeyValueDatabase kvdb = new KeyValueDatabase();
    private int replicationFactor = 3;

    private final static String REQ_CLIENT = "/CLIENT/";
    private final static String REQ_NODE = "/RING/";    

    private final static String REQ_READ = "/READ/";
    private final static String REQ_WRITE = "/WRITE/";

    // ----------------------------------------------
    // Main node interaction for node state

    private static class ReceiveUpdate implements IZLoopHandler{
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {

            Node node = (Node) arg;

            kvmsg kvMsg = kvmsg.recv(node.subscriber);
            if (kvMsg == null)
                return -1; //  Interrupted
            
            if (kvMsg.getSequence() > 0) {
                if (kvMsg.getKey().startsWith(SUB_NODES + FLUSH_SIGNAL)){
                    Long old_key = Long.parseLong(kvMsg.getKey().substring((SUB_NODES + FLUSH_SIGNAL).length()));
                    node.tokenAddrsMap.remove(old_key);
                } 
                else if (kvMsg.getKey().startsWith(SUB_NODES)) {
                    System.out.println("Received update " + kvMsg.getKey());
                    node.storeTokenAddrsMap(kvMsg, SUB_NODES);
                }
                else {
                    System.out.println("E: bad request, aborting");
                    return -1;
                }
            }
            else {
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
            kvMsg.fmtKey("%s%d", SUB_NODES, node.token);
            kvMsg.fmtBody("%s", node.addr);
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
                String addr = node.tokenAddrsMap.get(token);
                System.out.printf("Token: %d, Addr: %s\n", token, addr);
            }
            return 0;
        }
    }

    private void requestSnapshot() {
        // get state snapshot
        snapshot.sendMore(REQ_SNAPSHOT);
        snapshot.send(SUB_NODES);

        while (true) { // Wait for snapshot
            kvmsg kvMsg = kvmsg.recv(snapshot);
            if (kvMsg == null){
                break; // Interrupted
            }

            long sequence = kvMsg.getSequence();
            if (show_stats)
                System.out.printf("I: received snapshot=%d\n", sequence);
            // Check if snapshot is complete
            if (REP_SNAPSHOT.equalsIgnoreCase(kvMsg.getKey())) {
                kvMsg.destroy();
                break; // done
            }

            storeTokenAddrsMap(kvMsg, SUB_NODES);
        }
        // End of snapshot request
    }

    private void storeTokenAddrsMap(kvmsg kvMsg, String subtree){
        // Store token and address in tokenAddrsMap
        Long new_token = Long.parseLong(kvMsg.getKey().substring(subtree.length()));
        String new_addr = new String(kvMsg.body(), ZMQ.CHARSET);
        this.tokenAddrsMap.put(new_token, new_addr);
    }
    // ----------------------------------------------

    // ----------------------------------------------
    // Node interaction for database requests
    private static class ReceiveRequest implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Node node = (Node) arg;
            Socket socket = item.getSocket();

            byte[] identity = socket.recv();
            if (identity == null)
                return 0; //  Interrupted

            String request = socket.recvStr();

            if (request.equals(REQ_CLIENT)) {
                //handleClientRequest(socket);
            }
            else if (request.equals(REQ_NODE)) {
                //handleNodeRequest(identity, socket, arg);
            }
            else {
                System.out.printf("E: bad request, aborting\n");
                return -1;
            }
            

            // Read operation from client
                // Read from self and from next nodes in the ring (max = replicationFactor)
                // If read from self and from R of the next nodes in the ring, return the value (R = 2)
            
            // Write operation from client
                // Write to self and to next nodes in the ring (max = replicationFactor)
                // If write to self and to W of the next nodes in the ring, return the value (W = 2)         
            return 0;
        }
    }

    private static ShopList sendNodeReadRequest(Socket socket, String key){
        socket.sendMore(REQ_NODE);
        
        ShopList shopList = null;
   
        socket.send(REQ_READ + key);

        while (true) {
            kvmsg kvMsg = kvmsg.recv(socket);
            if (kvMsg == null) {
                break; // Interrupted
            }
            if (kvMsg.getKey().equalsIgnoreCase(REP_READ + key)) {
                shopList = new ShopList();
                Instant timeStamp = Instant.parse(kvMsg.getProp("timeStamp"));
                shopList.setTime(timeStamp);
            } else if (kvMsg.getKey().equalsIgnoreCase(REP_READ_ITEM + key) && shopList != null) {
                String name = kvMsg.getProp("name");
                int quantity = Integer.parseInt(kvMsg.getProp("quantity"));
                shopList.addItem(name, quantity);
            } else if (kvMsg.getKey().equalsIgnoreCase(REP_READ_END + key) && shopList != null) {
                kvMsg.destroy();
                break;
            } else {
                System.out.printf("E: bad request, aborting\n");
                return null;
            }
        }

        return shopList;
    }
    private static int sendNodeWriteRequest(Socket socket, String key, ShopList shopList){
        socket.sendMore(REQ_NODE);
        socket.send(REQ_WRITE + key);

        kvmsg kvMsg = kvmsg.recv(socket);
        if (kvMsg == null) {
            return -1; // Interrupted
        }
        /*if (kvMsg.equalsIgnoreCase(REP_WRITE + key)) {
            kvMsg.destroy();
        } else {
            System.out.printf("E: bad request, aborting\n");
            return -1;
        }*/

        return 0;
    }
    private int handleNodeRequest(byte[] identity, Socket socket, Object arg) {
        Node node = (Node) arg;

        String request = socket.recvStr();
        if (request.startsWith(REQ_READ)) {
            String key = request.substring(REQ_READ.length());
            ShopList shopList = null;
            if (node.kvdb.containsKey(key)) {
                shopList = (ShopList) node.kvdb.get(key);
            }
            else {
                shopList = new ShopList();
            }

            // Send state socket to client
            kvmsg kvMsg = new kvmsg(0);
            kvMsg.setKey(REP_READ + key);
            kvMsg.setProp("timeStamp", shopList.getTimeStamp());
            kvMsg.send(socket);
            kvMsg.destroy();

            for (Entry<String, Item> entry : shopList.getItems().entrySet()){
                kvMsg = new kvmsg(0);
                kvMsg.setKey(REP_READ_ITEM + key);
                kvMsg.setProp("name", entry.getKey());
                kvMsg.setProp("quantity", Integer.toString(entry.getValue().getQuantity()));
                kvMsg.send(socket);
                kvMsg.destroy();
            }
            
            // End of read request
            kvMsg = new kvmsg(0);
            kvMsg.setKey(REP_READ_END + key);
            kvMsg.send(socket);
        }

        return 0;
    }
    // ----------------------------------------------




    public Node(int port, Long token) {
        this.port = port;
        this.token = (Long) token;
        this.addr = new String(String.format("tcp://*:%d", port));


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

        tokenAddrsMap = new ConcurrentHashMap<Long, String>();
        // ----------------------------------------------

        // Node interaction for database requests ------

        // ----------------------------------------------
    }

    public void run() {
        
        requestSnapshot();

        PollItem poller = new PollItem(subscriber, ZMQ.Poller.POLLIN);
        loop.addPoller(poller, new ReceiveUpdate(), this);


        loop.addTimer(HEARTBEAT, 0, new SendHeartBeat(), this);
        if (time_stats){
            loop.addTimer(alarm, 0, new PrintStatus(), this);
        }


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
            }
            else if (args[i].equals("-t")) {
                token = Long.parseLong(args[i + 1]);
            }
            else {
                System.out.println("Wrong arguments");
                return;
            }
        }
        // System.out.println("Self Port: " + port);
        System.out.println("Token: " + token);
        System.out.println("Addr: " + String.format("tcp://*:%d", port));

        Node node = new Node(port,token);
        node.run();
    }
}
