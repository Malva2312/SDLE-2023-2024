package app;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Server {
    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            // Create a REP (reply) socket
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.bind("tcp://*:5555");

            System.out.println("Server started...");

            while (!Thread.currentThread().isInterrupted()) {
                // Wait for the next request from the client
                byte[] request = socket.recv(0);
                String receivedMessage = new String(request, ZMQ.CHARSET);
                System.out.println("Received message: " + receivedMessage);

                // Send a reply back to the client
                String replyMessage = "Hello, " + receivedMessage;
                socket.send(replyMessage.getBytes(ZMQ.CHARSET), 0);
            }
        }
    }
}
