package node;

import java.util.ArrayList;
import java.util.HashMap;
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

    private HashMap<String, kvmsg> kvLog;
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
            
            kvmsg request = kvmsg.recv(socket);
            if (request == null)
                return -1; //  Interrupted
            String subtree = null;
            if (request.getKey().equals(REQ_SNAPSHOT)) {
                subtree = request.getProp("subtree");
            }
            // else if (other type of requests ...)
            else {
                System.out.printf("E: bad request, aborting\n");
                return -1;
            }
            
            if (subtree != null) {
                System.out.println("request: " + request.getKey());
                System.out.println("subtree: " + subtree );

                Long t = Long.parseLong(request.getProp("token"));
                String addr = request.getProp("addr");
                int ttl = Integer.parseInt(request.getProp("ttl"));

                kvmsg store = new kvmsg(++node.sequence);
                store.setKey(subtree + t.toString());
                store.fmtBody("%s", subtree);
                store.setProp("token", t.toString());
                store.setProp("addr", addr);
                store.setProp("ttl", Long.toString(System.currentTimeMillis() + ttl));
                store.send(node.publisher);
                store.store(node.kvLog);


                kvmsg reply = new kvmsg(node.sequence);
                reply.setKey(REP_SNAPSHOT);
                reply.setProp("subtree", subtree);
                reply.setProp("sender", SUB_NODES);

                int count = 0;
                String body = "";
                for (Entry<String, kvmsg> entry : node.kvLog.entrySet()) {
                    String key = entry.getKey();
                    if (key.startsWith(subtree)) {
                        body += entry.getValue().getProp("token"); body += "\n";
                        body += entry.getValue().getProp("addr");  body += "\n";
                        count++;
                    }
                }
                reply.setProp("size", Integer.toString(count));
                reply.fmtBody("%s", body);
                socket.sendMore(identity);
                reply.send(socket);
                reply.destroy();

                System.out.println("Sent snapshot");
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
            System.out.println("FlushTTL");

            MainNode node = (MainNode) arg;
            if (node.kvLog != null) {
                for (kvmsg msg : new ArrayList<kvmsg> (node.kvLog.values())) {
        
                    // If message have defined TTL, then check if it is expired
                    long ttl = Long.parseLong(msg.getProp("ttl"));
                    if (ttl > 0 && System.currentTimeMillis() > ttl) {
                            // If expired, then remove it from kvLog
                            
                            kvmsg flush = new kvmsg(++node.sequence);
                            flush.fmtKey("%s%s",SUB_NODES, FLUSH_SIGNAL);
                            System.out.println("FlushTTL: " + msg.getProp("token"));
                            flush.setProp("token", msg.getProp("token"));
                            node.kvLog.remove(msg.getKey());
                            flush.send(node.publisher);

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

        this.kvLog = new HashMap<String, kvmsg>();
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
