package node;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CentralNode {
    private final ZContext context;
    private final ZMQ.Socket publisher;
    private final Map<Long, String> tokenAddressMap;

    public CentralNode() {
        this.context = new ZContext();
        this.publisher = context.createSocket(SocketType.PUB);
        this.publisher.bind("tcp://*:5555"); // Change the address as needed
        //this.publisher.bind("ipc://centralNode");

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.tokenAddressMap = new ConcurrentHashMap<>();
    }

    public void addNode(Long token, String address) {
        tokenAddressMap.put(token, address);
        publishUpdates();
    }

    public void removeNode(Long token) {
        tokenAddressMap.remove(token);
        publishUpdates();
    }

    public void updateNode(Long oldToken, Long newToken, String address) {
        tokenAddressMap.remove(oldToken);
        tokenAddressMap.put(newToken, address);
        publishUpdates();
    }

    private void publishUpdates() {
        // Publish the updated token-address map
        publisher.sendMore("UPDATE");
        publisher.send(tokenAddressMap.entrySet().toString());
    }

    private void run() {
        try {
            while (true) {
                // Perform any node-specific logic here
                // Publish the updated token-address map
                publisher.sendMore("UPDATE");
                publisher.send(tokenAddressMap.entrySet().toString());
                // Print the token-address map
                System.out.println(tokenAddressMap.entrySet().toString());
            }
        } finally {
            this.context.close();
        }
    }

    public static void main(String[] args) {
        CentralNode centralNode = new CentralNode();
        centralNode.run();
    }
}
