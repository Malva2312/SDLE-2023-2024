package app;

import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import database.*;
import node.helper.kvmsg;

import java.time.Instant;

public class Backend {
    private final static String READ = "READ"; // Read
    private final static String READ_REP = "READ_REP"; // Read Reply
    private final static String WRITE = "WRITE"; // Write
    private final static String WRITE_REP = "WRITE_REP"; // Write Reply
    private final static String OK = "OK"; // OK
    private final static String FAIL = "FAIL"; // FAIL

    private static int port = 5555;
    private static String address = "tcp://localhost:" + port;
    private KeyValueDatabase shoppingLists;
    private ZContext ctx;

    public Backend() {
        shoppingLists = new KeyValueDatabase();
        ctx = new ZContext();
    }

    private static class ReadRequest extends Thread {
        Backend backend;
        String key;

        public ReadRequest(Backend backend, String key) {
            this.backend = backend;
            this.key = key;
        }

        @Override
        public void run() {

            // Set up the socket
            Socket socket = backend.ctx.createSocket(SocketType.DEALER);
            socket.connect(Backend.address);

            try {
                // Build the request message
                kvmsg msg = new kvmsg(0);
                msg.setKey(READ);
                msg.setProp("db_key", key);

                // Send the request
                msg.send(socket);

                // Add timeout
                socket.setReceiveTimeOut(3000);

                // Receive the reply
                kvmsg reply = kvmsg.recv(socket);
                if (reply == null) {
                    //System.out.println("No response from server");
                    return;
                } else if (!reply.getProp("status").equals(OK)) {
                    //System.out.println("Error: " + reply.getProp("status"));
                    return;
                } else if (!reply.getKey().equals(READ_REP)) {
                    //System.out.println("Unexpected reply from server: " + reply.getKey());
                    return;
                } else {
                    // Deserialize the shopping list
                    ShopList shopList = ShopList.deserialize(new String(reply.body(), ZMQ.CHARSET));

                    // Get from the local database
                    ShopList localShopList = backend.shoppingLists.hasKey(key)
                            ? (ShopList) backend.shoppingLists.get(key)
                            : null;

                    // Merge the two shopping lists
                    if (localShopList == null && shopList == null) {
                        // Both shopping lists are null
                        return;
                    } else {
                        ShopList merged = ShopList.merge(localShopList, shopList);
                        backend.shoppingLists.put(key, merged);
                    }
                }
            } catch (ZMQException e) {
                //System.out.println("Error: " + e.getMessage());
                if (Thread.interrupted()) {
                    // We were interrupted, so we can exit
                    socket.close();
                    return;
                } else {
                    // Handle other ZMQExceptions
                    //e.printStackTrace();
                }
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }
    }

    private static class WriteRequest extends Thread {
        Backend backend;
        String key;
        ShopList shopList;

        public WriteRequest(Backend backend, String key, ShopList shopList) {
            this.backend = backend;
            this.key = key;
            this.shopList = shopList;
        }

        @Override
        public void run() {

            // Set up the socket
            Socket socket = backend.ctx.createSocket(SocketType.DEALER);
            socket.connect(Backend.address);

            try {

                // Build the request message
                kvmsg msg = new kvmsg(0);
                msg.setKey(WRITE);
                msg.setProp("db_key", key);
                msg.setProp("timestamp", shopList.getInstant().toString());
                msg.setBody(ShopList.serialize(shopList).getBytes(ZMQ.CHARSET));

                // Send the request
                msg.send(socket);

                // Add timeout
                socket.setReceiveTimeOut(3000);

                // Receive the reply
                kvmsg reply = kvmsg.recv(socket);
                if (reply == null) {
                    //System.out.println("No response from server");
                    return;
                } else if (!reply.getProp("status").equals(OK)) {
                    backend.shoppingLists.put(key, shopList);
                    //System.out.println("Error: " + reply.getProp("status"));
                    return;
                } else if (!reply.getKey().equals(WRITE_REP)) {
                    //System.out.println("Unexpected reply from server: " + reply.getKey());
                    return;
                } else {
                    // Get the timestamp from the reply
                    Instant timestamp = Instant.parse(reply.getProp("timestamp"));

                    ShopList localShopList = backend.shoppingLists.hasKey(key)
                            ? (ShopList) backend.shoppingLists.get(key)
                            : null;
                    ShopList serverShopList = ShopList.deserialize(new String(reply.body(), ZMQ.CHARSET));
                    serverShopList.setTimeStamp(timestamp);

                    // Merge the two shopping lists

                    if (localShopList != null || serverShopList != null) {
                        ShopList merged = ShopList.merge(localShopList, serverShopList);
                        backend.shoppingLists.put(key, merged);
                    }
                }
            } catch (ZMQException e) {
                //System.out.println("Error: " + e.getMessage());
                if (Thread.interrupted()) {
                    // We were interrupted, so we can exit
                    socket.close();
                    return;
                } else {
                    // Handle other ZMQExceptions
                    //e.printStackTrace();
                }
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }
    }

    public ShopList getShopList(String key) {
        ReadRequest readRequest = new ReadRequest(this, key);
        readRequest.start();

        long timeout = System.currentTimeMillis() + 3000;
        while (readRequest.isAlive()) {
            if (System.currentTimeMillis() > timeout) {
                readRequest.interrupt();
            }
        }
        return this.shoppingLists.hasKey(key) ? (ShopList) shoppingLists.get(key) : new ShopList();
    }

    public ShopList setShopList(String key, ShopList shopList) {
        WriteRequest writeRequest = new WriteRequest(this, key, shopList);
        writeRequest.start();

        long timeout = System.currentTimeMillis() + 3000;
        while (writeRequest.isAlive()) {
            if (System.currentTimeMillis() > timeout) {
                writeRequest.interrupt();
                this.shoppingLists.put(key, shopList);
            }
        }
        return this.shoppingLists.hasKey(key) ? (ShopList) shoppingLists.get(key) : new ShopList();
    }

    public boolean containsKey(String key) {
        return shoppingLists.hasKey(key);
    }
}
