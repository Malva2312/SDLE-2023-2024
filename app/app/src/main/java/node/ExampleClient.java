package node;

import org.checkerframework.checker.units.qual.s;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import database.KeyValueDatabase;
import database.ShopList;
import java.time.Instant;

public class ExampleClient
{
    // -----------------------------------------------
    // Constants
    private final static int MAIN_NODE_PORT = 5556;

    // -----------------------------------------------
    // Predifined Messages
    private final static String SNAP = "SNAP"; // Snapshot Request
    private final static String SNAP_REP = "SNAP_REP"; // Snapshot Reply
    private final static String FLUSH = "FLUSH"; // Flush
    private final static String UPDATE = "UPDATE"; // Update

    private final static String READ = "READ"; // Read
    private final static String READ_REP = "READ_REP"; // Read Reply
    private final static String WRITE = "WRITE"; // Write
    private final static String WRITE_REP = "WRITE_REP"; // Write Reply
    private final static String OK = "OK"; // OK
    private final static String FAIL = "FAIL"; // FAIL

    private final static String HEARTBEAT = "HEARTBEAT"; // Heartbeat
    private final static int HEARTBEAT_INTERVAL = 1000 * 3; // msecs
    private final static int TTL = HEARTBEAT_INTERVAL * 2; // Heartbeat TTL
    // -----------------------------------------------

    public static void main(String[] args)
    {

        ShopList shopList = new ShopList();
        KeyValueDatabase database = new KeyValueDatabase();
        database.put("unique_id_123", shopList);

        shopList.addItem("apple", 1.99, 2);
        shopList.addItem("banana", 0.99, 3);
        shopList.addItem("orange", 1.49, 1);
        shopList.addItem("pear", 1.99, 2);
        shopList.addItem("grape", 2.99, 1);

        shopList.setTimeStamp(Instant.now());
        System.out.println(shopList.getInstant());

        //  Prepare our context and socket
        try (ZContext ctx = new ZContext()) {
            //  Socket to talk to server
            System.out.println("Connecting to hello world server");

            ZMQ.Socket socket1 = ctx.createSocket(SocketType.DEALER);
            socket1.connect("tcp://localhost:5555");

            // Try to write to the database
            String key = "unique_id_123";
            // BEGIN WRITE
            kvmsg msg = new kvmsg( 0);
            msg.setKey(WRITE);
            msg.setProp("db_key", key);
            msg.setProp("size", Integer.toString(shopList.getItems().size()));
            msg.setProp("timestamp", shopList.getInstant().toString());
            String serialized = ShopList.serialize(shopList);
            msg.fmtBody("%s",serialized);            
            
            msg.send(socket1);
            // Receive the reply
            kvmsg reply = kvmsg.recv(socket1);
            System.out.println("Received write reply");
            if (reply == null) {
                System.out.println("Failed to receive reply");
                return;
            }
            // Check status of the reply
            if (reply.getKey().equals(WRITE_REP)) {
                System.out.println("Status: " + reply.getProp("status"));
            }
            else {
                System.out.println("Status: " + reply.getProp("status"));
                System.out.println("Failed to write to database");
            }
            // END OF WRITE


            // Send a read request
            System.out.println("Sending read request");
            msg = new kvmsg( 0);
            msg.setKey(READ);
            msg.setProp("db_key", key);
            msg.send(socket1);
            // Receive the reply
            reply = kvmsg.recv(socket1);
            System.out.println("Received read reply");
            if (reply == null) {
                System.out.println("Failed to receive reply");
                return;
            }
            // Check status of the reply
            if (reply.getKey().equals(READ_REP)) {
                System.out.println("Status: " + reply.getProp("status"));
            }
            else {
                System.out.println("Status: " + reply.getProp("status"));
                System.out.println("Failed to read from database");
            }

            if (reply.getProp("status").equals(OK)) {
                System.out.println("Received shop list");
            }
            else {
                System.out.println("Failed to read from database");
                return;
            }
            // END OF READ

            // From the reply, get the serialized shop list

            // Deserialize the shop list
            if (reply.body() == null ) {
                System.out.println("Failed to deserialize shop list");
                return;
            }
            String body = new String(reply.body(), ZMQ.CHARSET);
            ShopList shopList2 = ShopList.deserialize(body);

            shopList2.setTimeStamp(Instant.parse(reply.getProp("timestamp")));

            System.out.println(shopList2.getInstant());
            System.out.println(shopList2.getTotalPrice());
            shopList2.displayItems();
        }
        
    }
}