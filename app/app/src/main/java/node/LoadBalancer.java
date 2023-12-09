package node;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class LoadBalancer {
    private final static int MAIN_NODE_PORT = 5556;

    private List<String> nodeAddresses;
    private int currentIndex;

    private ZContext ctx;
    private Socket frontend; // Router socket for clients
    private Socket backend; // Dealer socket for nodes

    public LoadBalancer() {
        this.nodeAddresses = new ArrayList<>();
        this.currentIndex = 0;

        // Initialize node addresses (add more if needed)
        nodeAddresses.add("tcp://localhost:5557");
        nodeAddresses.add("tcp://localhost:5558");
        nodeAddresses.add("tcp://localhost:5559");

        this.ctx = new ZContext();
        this.frontend = ctx.createSocket(SocketType.ROUTER);
        this.frontend.bind("tcp://*:5555"); // Clients will connect to this address

        this.backend = ctx.createSocket(SocketType.DEALER);
        for (String address : nodeAddresses) {
            this.backend.connect(address);
        }
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            // Forward client requests to nodes in a round-robin fashion
            forwardRequestToFrontend();
            forwardResponseToBackend();
        }
    }

    private void forwardRequestToFrontend() {
        byte[] identity = frontend.recv();
        byte[] empty = frontend.recv();
        byte[] request = frontend.recv();

        // Choose the next node to send the request to
        String selectedNodeAddress = getNextNodeAddress();
        backend.sendMore(selectedNodeAddress);
        backend.sendMore(empty);
        backend.sendMore(identity);
        backend.sendMore(empty);
        backend.send(request);
    }

    private void forwardResponseToBackend() {
        byte[] address = backend.recv();
        byte[] empty = backend.recv();
        byte[] response = backend.recv();

        // Send the response back to the client
        frontend.sendMore(address);
        frontend.sendMore(empty);
        frontend.send(response);
    }

    private String getNextNodeAddress() {
        // Simple round-robin load balancing
        String address = nodeAddresses.get(currentIndex);
        currentIndex = (currentIndex + 1) % nodeAddresses.size();
        return address;
    }

    public static void main(String[] args) {
        LoadBalancer loadBalancer = new LoadBalancer();
        loadBalancer.run();
    }
}
