package app;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Client {
    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            // Create a REQ (request) socket
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://localhost:5555");

            // Send a request to the server
            String messageToSend = "World";
            System.out.println("Sending message: " + messageToSend);
            socket.send(messageToSend.getBytes(ZMQ.CHARSET), 0);

            // Receive the reply from the server
            byte[] reply = socket.recv(0);
            String receivedMessage = new String(reply, ZMQ.CHARSET);
            System.out.println("Received reply: " + receivedMessage);
        }
    }
}
