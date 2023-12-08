package node;

import java.util.concurrent.ConcurrentHashMap;


import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;


public class Node {
    private final static int MAIN_NODE_PORT = 5556;
    
    private final static String REQ_SNAPSHOT = "ALLNODES?";
    private final static String REP_SNAPSHOT = "REPNODESSNAP";
    
    private final static String SUB_NODES = "/NODE/";
    private final static String FLUSH_SIGNAL = "/FLUSH/";
    
    private final static int HEARTBEAT = 5 * 1000; //  msecs
    
    private final int alarm = 5 * 1000; //  msecs
    private final boolean show_stats = false;
    private final boolean time_stats = false;

    private ZContext ctx;
    private ZLoop loop;

    Socket snapshot;    // Request Snapshot from MainNode
    Socket subscriber;  // Subscribe to all updates from MainNode
    Socket publisher;   // Publish updates to MainNode

    int port;
    Long token;
    String addr;
    private ConcurrentHashMap<Long, String> tokenAddrsMap;


    private static class receiveUpdate implements IZLoopHandler{
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

    private static class sendHeartBeat implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            Node node = (Node) arg;

            kvmsg kvMsg = new kvmsg(0);
            kvMsg.fmtKey("%s%d", SUB_NODES, node.token);
            kvMsg.fmtBody("%s", node.addr);
            kvMsg.setProp("ttl", "%d", HEARTBEAT * 2);
            kvMsg.send(node.publisher);
            kvMsg.destroy();

            return 0;
        }
    }

    private static class printStatus implements IZLoopHandler {
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

    public Node(int port, Long token, String addr) {
        this.port = port;
        this.token = (Long) token;
        this.addr = (String) addr;


        ctx = new ZContext();
        loop = new ZLoop(ctx);
        loop.verbose(false);


        snapshot = ctx.createSocket(SocketType.DEALER);
        snapshot.connect("tcp://localhost:" + MAIN_NODE_PORT);

        subscriber = ctx.createSocket(SocketType.SUB);
        subscriber.connect("tcp://localhost:" + (MAIN_NODE_PORT + 1));
        subscriber.subscribe(SUB_NODES.getBytes(ZMQ.CHARSET));

        publisher = ctx.createSocket(SocketType.PUSH);
        publisher.connect("tcp://localhost:" + (MAIN_NODE_PORT + 2));

        tokenAddrsMap = new ConcurrentHashMap<Long, String>();
    }

    public void run() {
        
        requestSnapshot();

        PollItem poller = new PollItem(subscriber, ZMQ.Poller.POLLIN);
        loop.addPoller(poller, new receiveUpdate(), this);


        loop.addTimer(HEARTBEAT, 0, new sendHeartBeat(), this);
        if (time_stats){
            loop.addTimer(alarm, 0, new printStatus(), this);
        }


        loop.start();

        ctx.close();

    }

    public static void main(String[] args) {
        // args should be: -p <port> -t <token> -a <address>
        // args could be empty, then default values will be used
        // args could be in any order

        if (args.length <= 0 && args.length % 2 != 0 && args.length > 6) {
            System.out.println("Wrong arguments");
            return;
        }
        long token = 42;
        String addr = "dummy_addr";
        int port = 0;

        int i;
        for (i = 0; i < args.length; i += 2) {
            if (args[i].equals("-p")) {
                port = Integer.parseInt(args[i + 1]);
            }
            else if (args[i].equals("-t")) {
                token = Long.parseLong(args[i + 1]);
            }
            else if (args[i].equals("-a")) {
                addr = args[i + 1];
            }
            else {
                System.out.println("Wrong arguments");
                return;
            }
        }
        // System.out.println("Self Port: " + port);
        System.out.println("Token: " + token);
        System.out.println("Addr: " + addr);

        Node node = new Node(port,token, addr);
        node.run();
    }
}
