package node;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import org.zeromq.ZLoop;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CentralNode {
    private final ZContext context;
    private final ZLoop loop;

    private final ZMQ.Socket router;
    private final ZMQ.Socket publisher;
    private final ZMQ.Socket pull;

    private final int port;
    private final Map<Long, String> logMap;
    private final Map<Long, String> tokenAddressMap;

    public CentralNode(int port) {
        
        this.context = new ZContext();
        this.loop = new ZLoop(this.context);
        this.loop.verbose(false);

        this.router = context.createSocket(SocketType.ROUTER);
        this.router.bind("tcp://*:" + port);
        
        this.publisher = context.createSocket(SocketType.PUB);
        this.publisher.bind("tcp://*:" + (port + 1));

        this.pull = context.createSocket(SocketType.PULL);
        this.pull.bind("tcp://*:" + (port + 2));
        
        this.port = port;
        this.logMap = new ConcurrentHashMap<>();
        this.tokenAddressMap = new ConcurrentHashMap<>();
    }

    //public void addNode(Long token, String address) {
    //    tokenAddressMap.put(token, address);
    //    publishUpdates();
    //}
    //public void removeNode(Long token) {
    //    tokenAddressMap.remove(token);
    //    publishUpdates();
    //}
    //public void updateNode(Long oldToken, Long newToken, String address) {
    //    tokenAddressMap.remove(oldToken);
    //    tokenAddressMap.put(newToken, address);
    //    publishUpdates();
    //}
    //private void publishUpdates() {
    //    // Publish the updated token-address map
    //    publisher.sendMore("UPDATE");
    //    publisher.send(tokenAddressMap.entrySet().toString());
    //}

    private void run() {
        // open 3 scokets:
        // 1. Publisher: Publishes updates to the token-address map
        // 2. Router: Responds to state requests from the nodes
        // 3. PULL: Receives updates from the nodes
        
        // Using a reactor 

    }

    public static void main(String[] args) {
        CentralNode centralNode = new CentralNode(5555);
        centralNode.run();
    }
}
