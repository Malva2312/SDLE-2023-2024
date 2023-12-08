package node;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;


import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;

public class MainNode {
    private final static String REQ_SNAPSHOT = "ALLNODES?";
    private final static String REP_SNAPSHOT = "REPNODESSNAP";

    private final static String SUB_NODES = "/NODE/";
    private final static String FLUSH_SIGNAL = "/FLUSH/";

    private ZContext ctx;
    private ZLoop loop;

    private int port;
    private long sequence;

    private Socket snapshot;    // (Router) SnapShot Request
    private Socket publisher;   // (Pub)    Publish Updates
    private Socket collector;   // (PULL)   Collect Updates

    private final static int HEARTBEAT = 1000; //  msecs

    private ConcurrentHashMap<String, kvmsg> kvLog;
    //private ConcurrentHashMap<Long, String> tokenAddrsMap;

    // -----------------------------------------------
    //  (Router) SnapShot Request               // Poller
    private static class SendMemorySnapshot implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            System.out.println("SendMemorySnapshot");

            MainNode node = (MainNode) arg;
            Socket socket = item.getSocket();

            byte[] identity = socket.recv();
            if (identity == null)
                return 0; //  Interrupted
            
            String request = socket.recvStr();
            String subtree = null;

            if (request.equals(REQ_SNAPSHOT)) {
                subtree = socket.recvStr();
            }
            // else if (other type of requests ...)
            else {
                System.out.printf("E: bad request, aborting\n");
                return -1;
            }
            
            System.out.println("request: " + request);
            System.out.println("subtree: " + subtree);
            if (subtree != null) {
                // Send state socket to client
                for (Entry<String, kvmsg> entry : node.kvLog.entrySet()) {
                    if (entry.getValue().getKey().startsWith(subtree)) {
                        socket.send(identity, // Choose recipient
                                ZMQ.SNDMORE);
                        entry.getValue().send(socket);
                    }
                }

                // End of snapshot message
                socket.send(identity, ZMQ.SNDMORE);
                kvmsg kvMsg = new kvmsg(node.sequence);
                kvMsg.setKey(REP_SNAPSHOT);
                kvMsg.setBody(subtree.getBytes(ZMQ.CHARSET));
                kvMsg.send(socket);
                kvMsg.destroy();
            }

            return 0;
        }
    };
    // (Collector) Collect Updates from clients // Poller
    private static class CollectUpdates implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {

            MainNode node = (MainNode) arg;
            Socket socket = item.getSocket();

            kvmsg msg = kvmsg.recv(socket);
            if (msg == null){
                return 0; //  Interrupted
            }
            // Push the update to clients
            msg.setSequence(++node.sequence);
            msg.send(node.publisher);
            int ttl = Integer.parseInt(msg.getProp("ttl"));
            if (ttl > 0) {
                msg.setProp("ttl", Long.toString(System.currentTimeMillis() + ttl));
            }
            msg.store(node.kvLog);


            return 0;
        }
    };

    // (Timer) Flush Expired Entries           // Timer
    private static class FlushTTL implements IZLoopHandler {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            //System.out.println("FlushTTL");

            MainNode node = (MainNode) arg;
            if (node.kvLog != null) {
                for (kvmsg msg : new ArrayList<kvmsg> (node.kvLog.values())) {
        
                    // If message have defined TTL, then check if it is expired
                    long ttl = Long.parseLong(msg.getProp("ttl"));
                    if (ttl > 0 && System.currentTimeMillis() > ttl) {
                            // If expired, then remove it from kvLog
                            
                            kvmsg flush_kvMsg = new kvmsg(++node.sequence);
                            Long old_key = Long.parseLong(msg.getKey().substring((SUB_NODES).length()));
                            flush_kvMsg.setKey( SUB_NODES + FLUSH_SIGNAL + old_key.toString());
                            flush_kvMsg.setBody(ZMQ.MESSAGE_SEPARATOR);
                            flush_kvMsg.send(node.publisher);
                            flush_kvMsg.destroy();
                            node.kvLog.remove(msg.getKey());
                            System.out.printf("I: Expired: %s\n", msg.getKey());
                    }
                }
            }

            return 0;
        }
    };


    // -----------------------------------------------
    public MainNode(int port) {
        this.port = port;
        //this.sequence = 0;

        this.ctx = new ZContext();
        this.loop = new ZLoop(this.ctx);
        this.loop.verbose(false);
        

        this.snapshot = ctx.createSocket(SocketType.ROUTER);
        this.snapshot.bind(String.format("tcp://*:%d", this.port));
        this.publisher = ctx.createSocket(SocketType.PUB);
        this.publisher.bind(String.format("tcp://*:%d", this.port + 1));
        this.collector = ctx.createSocket(SocketType.PULL);
        this.collector.bind(String.format("tcp://*:%d", this.port + 2));

        this.kvLog = new ConcurrentHashMap<String, kvmsg>();
        //this.tokenAddrsMap = new ConcurrentHashMap<Long, String>();
    }

    private void run(){
        // Register our handlers with reactor
        PollItem snapshot_poller = new PollItem(this.snapshot, ZMQ.Poller.POLLIN);
        this.loop.addPoller(snapshot_poller, new SendMemorySnapshot(), this);

        PollItem collector_poller = new PollItem(this.collector, ZMQ.Poller.POLLIN);
        this.loop.addPoller(collector_poller, new CollectUpdates(), this);

        this.loop.addTimer(HEARTBEAT, 0, new FlushTTL(), this);

        System.out.println("MainNode is running...");
        this.loop.start();

        this.ctx.close();
    }
    public static void main(String[] args) {
        MainNode node = new MainNode(5556);
        node.run();
    }
}
