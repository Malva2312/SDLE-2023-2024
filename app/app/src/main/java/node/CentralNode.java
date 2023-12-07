package node;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import org.zeromq.ZLoop;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CentralNode {
    // ---------------------------------------------------------------------
    // Binary Star finite state machine
    enum State {
        STATE_PRIMARY,          // Primary, waiting for peer to connect
        STATE_BACKUP,           // Backup, waiting for peer to connect
        STATE_ACTIVE,           // Active - accepting connections
        STATE_PASSIVE           // Passive - not accepting connections
    };
    enum Event {
        PEER_PRIMARY,
        PEER_BACKUP,
        PEER_ACTIVE,
        PEER_PASSIVE,
        CLIENT_REQUEST
    }

    // Binary Star status information
    private ZContext            bstar_context;         // Context wrapper for binary star
    private ZLoop               loop;               // Reactor loop

    private State               state;              // Current state
    private Event               event;              // Current event
    private long                peerExpiry;         // When peer is considered 'dead'
    // Handlers & Object
    
    // Dedicated socket to receive state from peer
    private Socket              statepub;           // State publisher
    private Socket              statesub;           // State subscriber
    
    private final static int   HEARTBEAT = 1000;   // (msecs) // If peer doesn't respond in 2 heartbeats -> 'dead'
    // ---------------------------------------------------------------------

    private ZContext            context;            // Context wrapper
    private Socket              router;             // Router socket
    private Socket              publisher;          // Publisher socket
    private Socket              pull;               // Pull socket

    private long                sequence;           // Last kvmsg processed
    private int                 port;               // Port number
    private int                 peer;               // Peer number

    private List<kvmsg>         pending;            // Pending updates from clients
    private boolean             primary;            // TRUE if we're primary server
    private boolean             active;             // TRUE if we're active 
    private boolean             backup;             // TRUE if we're backup server

    private Map<Long, String>   logMap;             // Key-value store
    private Map<Long, String>   tokenAddressMap;    // Data to be shared to the client nodes
    
    // ---------------------------------------------------------------------
    //  Handlers for ZeroMQ reactor

    //  Publish state to peer
    private IZLoopHandler SendStateToPeer = new IZLoopHandler() {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            CentralNode node = (CentralNode) arg;
            node.statepub.send(String.format("%d", node.state.ordinal()));
            return 0;
        }
    };
    // Receive state from peer
    private static IZLoopHandler RecvStateFromPeer = new IZLoopHandler() {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            CentralNode self = (CentralNode) arg;
            String state = item.getSocket().recvStr();
            if (state != null) {
                self.event = Event.values()[Integer.parseInt(state)];
                self.peerExpiry = System.currentTimeMillis() + 2 * HEARTBEAT;
            }
            return self.bstar_state_machine() ? 0 : -1;
        }
    };

    // ---------------------------------------------------------------------

    private boolean bstar_state_machine(){
        boolean rc = true;

        if (this.state == State.STATE_PRIMARY){
            if (this.event == Event.CLIENT_REQUEST){}
            else if (this.event == Event.PEER_BACKUP){}
            else if (this.event == Event.PEER_ACTIVE){}
            else {
                //error
            }
        }
        else if (this.state == State.STATE_ACTIVE){
            if (this.event == Event.CLIENT_REQUEST){}
            else if (this.event == Event.PEER_ACTIVE){}
            else {
                //error
            }
        }
        else if (this.state == State.STATE_PASSIVE){
            if (this.event == Event.PEER_BACKUP){}
            else if (this.event == Event.PEER_PRIMARY){}
            else if (this.event == Event.PEER_PASSIVE){}
            else {
                //error
            }
        }
        else if (this.state == State.STATE_BACKUP){
            if (this.event == Event.PEER_ACTIVE){}
            else {
                //error
            }
        }
        return rc;
    }


    public CentralNode(int port, int peer, boolean primary) {
        
        // Set dedicated port for the binary star
        this.bstar_context = new ZContext();
        this.loop = new ZLoop(bstar_context);
        this.loop.verbose(true);

        this.state = primary ? State.STATE_PRIMARY : State.STATE_BACKUP;

        this.statepub = this.bstar_context.createSocket(SocketType.PUB);

        this.statesub = this.bstar_context.createSocket(SocketType.SUB);
        this.statesub.subscribe(ZMQ.SUBSCRIPTION_ALL);

        if (primary) {
            statepub.bind("tcp://*:5003");          // Eg. "tcp://*:5003"
            statesub.connect("tcp://localhost:5004");  // Eg. "tcp://localhost:5004"
        }
        else {
            statepub.bind("tcp://*:5004");          // Eg. "tcp://*:5004"
            statesub.connect("tcp://localhost:5003");  // Eg. "tcp://localhost:5003"
        }
        
        this.loop.addTimer(HEARTBEAT, 0, this.SendStateToPeer , this);
        PollItem poller = new PollItem(this.statesub, ZMQ.Poller.POLLIN);
        this.loop.addPoller(poller, RecvStateFromPeer, this);
        // End of binary star setup

        this.port = port;
        this.peer = peer;
        this.primary = primary;
        

        this.context = new ZContext();
        this.loop = new ZLoop(this.context);
        this.loop.verbose(true);

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


    private void run() {

    }

    public static void main(String[] args) {
        //  Arguments can be either of:
        //      -p  primary server, at tcp://localhost:5001
        //      -b  backup server,  at tcp://localhost:5002

        int port = 5556;
        int peer = 5566;

        CentralNode centralNode = null;

        if (args.length == 1 && "-p".equals(args[0])) {
            centralNode = new CentralNode(port, peer, true);
        }
        else if (args.length == 1 && "-b".equals(args[0])) {
            centralNode = new CentralNode(peer, port, false);
        }
        else {
            System.out.printf("Usage: clonesrv4 { -p | -b }\n");
            System.exit(0);
        }
        
        if (centralNode != null) {
            centralNode.run();
        }
    }
}
