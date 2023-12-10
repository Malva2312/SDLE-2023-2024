package node;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import database.KeyValueDatabase;
import database.ShopList;
import java.time.Instant;

public class ExampleClient
{
    private final static String SNDR_CLIENT = "/CLIENT/";
    private final static String SNDR_NODE = "/RING/";

    private final static String READ_REQ = "READ";
    private final static String READ_REP = "READREP";
    private final static String WRITE_REQ = "WRITE";
    private final static String WRITE_REP = "WRITEREP";
    private final static String END_OF_MESSAGE = "ENDOFMESSAGE";

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
            socket1.connect("tcp://localhost:5581");

            //ZMQ.Socket socket2 = ctx.createSocket(SocketType.DEALER);
            //socket2.connect("tcp://localhost:5571");

            kvmsg msg = new kvmsg(0);
            msg.setKey(WRITE_REQ);
            msg.setProp("db_key", "unique_id_123");
            msg.setProp("sender", SNDR_NODE);
            msg.setProp("timestamp", "%s", shopList.getInstant().toString());

            msg.setProp("items", Integer.toString(shopList.getItems().size()));

            String items = "";
            for (String item : shopList.getItems().keySet()) {
                items += item + "\n";
                items += Integer.toString(shopList.getItems().get(item).getQuantity()) + "\n";
            }
            msg.fmtBody("%s", items);
            msg.send(socket1);


            // Wait for the reply

            System.out.println("Waiting for reply");
            kvmsg reply;
            reply= kvmsg.recv(socket1);
            System.out.println("Received reply " + reply.getKey() + " " + reply.getProp("status"));


            // REQUEST READ
            msg = new kvmsg(0);
            msg.setKey(READ_REQ);
            msg.setProp("db_key", "unique_id_123");
            msg.setProp("sender", SNDR_NODE);

            msg.send(socket1);

            // Wait for the reply
            System.out.println("Waiting for reply");
            reply = kvmsg.recv(socket1);
            ShopList rcv = new ShopList();
            rcv.setTimeStamp(Instant.parse(reply.getProp("timestamp")));

            String db_key = reply.getProp("db_key");
            int items_count = Integer.parseInt(reply.getProp("items"));
            String[] list_items = new String(reply.body(), ZMQ.CHARSET).split("\n");

            for (int i = 0; i < items_count; i++) {
                String item_name = list_items[i*2];
                int item_quantity = Integer.parseInt(list_items[i*2 + 1]);
                rcv.addItem(item_name, 0.0, item_quantity);
            }

            System.out.println("Received reply " + reply.getKey() + " " + reply.getProp("status"));
            rcv.displayItems();


        }
    }
}