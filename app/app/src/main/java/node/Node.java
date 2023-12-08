package node;

import java.util.concurrent.ConcurrentHashMap;


import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;


public class Node {
    private final static int MAIN_NODE_PORT = 5556;

    private final static String REQ_SNAPSHOT = "ALLNODES?";
    private final static String REP_SNAPSHOT = "REPNODESSNAP";

    private final static String SUB_NODES = "/NODE/";
    private final static String FLUSH_SIGNAL = "/FLUSH/";
    
    private final static int HEARTBEAT = 5 * 1000; //  msecs

    private ZContext ctx;
    Socket snapshot;    // Request Snapshot from MainNode
    Socket subscriber;  // Subscribe to all updates from MainNode
    Socket publisher;   // Publish updates to MainNode

    int port;
    Long token;
    String addr;
    private ConcurrentHashMap<Long, String> tokenAddrsMap;

    private final boolean time_stats = false;

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
            // Check if snapshot is complete
            if (REP_SNAPSHOT.equalsIgnoreCase(kvMsg.getKey())) {
                kvMsg.destroy();
                break; // done
            }

            storeTokenAddrsMap(kvMsg, SUB_NODES);
        }
        // End of snapshot request
    }

    private int processUpdate(Poller poller){
        long start_time = System.currentTimeMillis();

        // Process updates from MainNode
            if (poller.pollin(0)) {
                kvmsg kvMsg = kvmsg.recv(subscriber);
                if (kvMsg == null)
                    return -1; //  Interrupted
            
                if (kvMsg.getSequence() > 0) {
                    if (kvMsg.getKey().startsWith(SUB_NODES)) {
                        System.out.println("Received update " + kvMsg.getKey());
                        if (kvMsg.getKey().startsWith(SUB_NODES + FLUSH_SIGNAL)) {
                            System.out.println("FLUSH_SIGNAL");
                            Long old_key = Long.parseLong(kvMsg.getKey().substring((SUB_NODES + FLUSH_SIGNAL).length()));
                            tokenAddrsMap.remove(old_key);
                        }
                        else {
                            storeTokenAddrsMap(kvMsg, SUB_NODES);
                        }
                    }
                } else {
                    kvMsg.destroy();
                }
            }

        if (time_stats) {
            long end_time = System.currentTimeMillis();
            System.out.printf("processUpdate: %d\n", (end_time - start_time) / 1000);
        }
        return 0;
    }
    
    private Long sendUpdate(long alarm){

        if (System.currentTimeMillis() >= alarm) {
            kvmsg kvMsg = new kvmsg(0);
            kvMsg.fmtKey("%s%d", SUB_NODES, token);
            kvMsg.fmtBody("%s", addr);
            kvMsg.setProp("ttl", "%d", HEARTBEAT * 2);
            kvMsg.send(publisher);
            kvMsg.destroy();

            alarm = System.currentTimeMillis() + HEARTBEAT;
        }
        return alarm;
        
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

        Poller poller = ctx.createPoller(1);
        poller.register(subscriber, Poller.POLLIN);

        long alarm = System.currentTimeMillis();
        long check_status = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            
            int rc = poller.poll(
                Math.max(0, alarm - System.currentTimeMillis())
            );
            if (rc == -1)
                break; //  Context has been shut down
            
            if (processUpdate(poller) == -1)
                break; //  Context has been shut down
         

            // Pass the reference of the alarm to sendUpdate
            alarm = sendUpdate(alarm);

            if (System.currentTimeMillis() >= check_status){
                // Check pairs of tokens and addresses
                System.out.println("\nChecking tokenAddrsMap");
                for (Long token : tokenAddrsMap.keySet()) {
                    String addr = tokenAddrsMap.get(token);
                    System.out.printf("Token: %d, Addr: %s\n", token, addr);
                }
                check_status = System.currentTimeMillis() + 5000;
            }
        }
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
