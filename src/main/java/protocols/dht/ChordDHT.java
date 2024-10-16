package protocols.dht;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.AutomatedApp;
import protocols.dht.chord.ChordInterval;
import protocols.dht.chord.ChordNode;
import protocols.dht.chord.ChordNodeSelf;
import protocols.dht.chord.FingerEntry;
import protocols.dht.messages.*;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.Lookup;
import protocols.dht.timers.FixFingersTimer;
import protocols.dht.timers.StabilizeTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HybridMap;
import utils.IdentifierUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

public class ChordDHT extends GenericProtocol {

    public static final short PROTOCOL_ID = 500;
    public static final String PROTOCOL_NAME = "chord-dht";
    public static final int M_BITS = 256;
    private static final Logger LOGGER = LogManager.getLogger(ChordDHT.class);
    private final ChordNodeSelf self;
    private final Random random;
    private final int channelId;
    private final Map<Host, Boolean> connections;
    private final Map<Host, Queue<ProtoMessage>> messages;
    private ChordNode successor;
    private ChordNode predecessor;
    private HybridMap<BigInteger, FingerEntry> fingers;
    private int stabilizeTime;
    private int fixFingersTime;

    public ChordDHT(Properties props, Host selfHost) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        final String nodeIDHex = props.getProperty(AutomatedApp.PROPERTY_NODE_ID);
        this.self = new ChordNodeSelf(new BigInteger(nodeIDHex, 16).toByteArray(), selfHost, nodeIDHex);
        this.successor = null;
        this.predecessor = null;
        this.fingers = new HybridMap<>();
        this.stabilizeTime = Integer.parseInt(props.getProperty("stabilize_time", "10000")); //10 seconds
        this.fixFingersTime = Integer.parseInt(props.getProperty("fix_fingers_time", "10000")); //10 seconds
        this.random = new Random();
        this.connections = new HashMap<>();
        this.messages = new HashMap<>();

        final String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds
        final Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        registerRequestHandler(Lookup.REQUEST_ID, this::uponLookup);

        registerMessageSerializer(channelId, FindPredecessorRequest.MSG_ID, FindPredecessorRequest.serializer);
        registerMessageSerializer(channelId, JoinLookup.MSG_ID, JoinLookup.serializer);
        registerMessageSerializer(channelId, JoinLookupReply.MSG_ID, JoinLookupReply.serializer);
        registerMessageSerializer(channelId, JoinFindPredecessorRequest.MSG_ID, JoinFindPredecessorRequest.serializer);
        registerMessageSerializer(channelId, StabilizeGetPredecessorRequest.MSG_ID, StabilizeGetPredecessorRequest.serializer);
        registerMessageSerializer(channelId, StabilizeGetPredecessorReply.MSG_ID, StabilizeGetPredecessorReply.serializer);
        registerMessageSerializer(channelId, NotifyRequest.MSG_ID, NotifyRequest.serializer);
        registerMessageSerializer(channelId, FixFingersFindPredecessorRequest.MSG_ID, FixFingersFindPredecessorRequest.serializer);
        registerMessageSerializer(channelId, FixFingersLookupReply.MSG_ID, FixFingersLookupReply.serializer);
        registerMessageSerializer(channelId, InitialContactRequest.MSG_ID, InitialContactRequest.serializer);
        registerMessageSerializer(channelId, InitialContactReply.MSG_ID, InitialContactReply.serializer);

        registerMessageHandler(channelId, FindPredecessorRequest.MSG_ID, this::uponFindPredecessorRequest, this::uponFindPredecessorRequestFail);
        registerMessageHandler(channelId, JoinLookup.MSG_ID, this::uponJoinLookup, this::uponJoinLookupFail);
        registerMessageHandler(channelId, JoinLookupReply.MSG_ID, this::uponJoinLookupReply, this::uponJoinLookupReplyFail);
        registerMessageHandler(channelId, JoinFindPredecessorRequest.MSG_ID, this::uponJoinFindPredecessorRequest, this::uponJoinFindPredecessorRequestFail);
        registerMessageHandler(channelId, StabilizeGetPredecessorRequest.MSG_ID, this::uponStabilizeGetPredecessorRequest, this::uponStabilizeGetPredecessorRequestFail);
        registerMessageHandler(channelId, StabilizeGetPredecessorReply.MSG_ID, this::uponStabilizeGetPredecessorReply, this::uponStabilizeGetPredecessorReplyFail);
        registerMessageHandler(channelId, NotifyRequest.MSG_ID, this::uponNotifyRequest, this::uponNotifyRequestFail);
        registerMessageHandler(channelId, FixFingersFindPredecessorRequest.MSG_ID, this::uponFixFingersFindPredecessorRequest, this::uponFixFingersFindPredecessorRequestFail);
        registerMessageHandler(channelId, FixFingersLookupReply.MSG_ID, this::uponFixFingersLookupReply, this::uponFixFingersLookupReplyFail);
        registerMessageHandler(channelId, InitialContactRequest.MSG_ID, this::uponInitialContactRequest, this::uponInitialContactRequestFail);
        registerMessageHandler(channelId, InitialContactReply.MSG_ID, this::uponInitialContactReply, this::uponInitialContactReplyFail);

        registerTimerHandler(StabilizeTimer.TIMER_ID, this::uponStabilize);
        registerTimerHandler(FixFingersTimer.TIMER_ID, this::uponFixFingers);

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) {
        if (props.containsKey("contact")) {
            try {
                final String contact = props.getProperty("contact");
                final String[] split = contact.split(":");
                final Host contactHost = new Host(InetAddress.getByName(split[0]), Short.parseShort(split[1]));
                sendMessageToHost(new InitialContactRequest(), contactHost);
            } catch (Exception e) {
                LOGGER.error("Invalid contact on configuration: '" + props.getProperty("contact"));
                System.exit(-1);
            }
        } else { // first node
            this.predecessor = new ChordNode(self.getNodeID(), self.getHost());
            this.successor = new ChordNode(self.getNodeID(), self.getHost());
            this.successor.addPeer(self.getNodeIDHex(), self.getHost());
            final byte[] startID = FingerEntry.calculateStart(self.getNodeID(), 1);
            this.fingers.put(BigInteger.ONE, new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                    IdentifierUtils.toNumerical(FingerEntry.calculateStart(self.getNodeID(), 2)), false, false),
                    new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers())));
        }
        setupPeriodicTimer(new StabilizeTimer(), stabilizeTime, stabilizeTime);
        setupPeriodicTimer(new FixFingersTimer(), fixFingersTime, fixFingersTime);
    }

    private void uponLookup(Lookup request, short protoID) {
        LOGGER.info("Received Lookup Request of the form {} from {}", request, request.getPeerID());
        findPredecessor(request.getPeerID(), protoID);
    }

    private void findPredecessor(byte[] peerID, short destinationProtocol) {
        if (ChordInterval.isInInterval(peerID, self.getNodeID(), successor.getNodeID(), true, true)) {
            final LookupReply lookupReply = new LookupReply(peerID);
            lookupReply.addAllPeers(successor.getPeers().get(IdentifierUtils.toHex(peerID)));
            sendReply(lookupReply, destinationProtocol);
        } else {
            final ChordNode closestNode = closestPrecedingNode(peerID);

            LOGGER.info(closestNode.getHost());
            if (closestNode.getHost().equals(self.getHost())) { //LAST

                final FingerEntry firstFinger = fingers.get(BigInteger.ONE);
                if (firstFinger != null && IdentifierUtils.toNumerical(firstFinger.getNode().getNodeID())
                .compareTo(IdentifierUtils.toNumerical(self.getNodeID())) != 0) {
                    LOGGER.info("Finger[1] is smaller than self, returning finger[1]");

                    byte[] pid =firstFinger.getNode().getNodeID();
                    final LookupReply lookupReply = new LookupReply(pid);
                    lookupReply.addAllPeers(successor.getPeers().get(IdentifierUtils.toHex(pid)));
                    sendReply(lookupReply, destinationProtocol);
                }
                
                //This should only happen when there is ONLY ONE Node in the system.
                //RETURN SUCESSOR! --- if NOT SOLO
                //SOLO MEANS THAT FIRST FINGER IS SAME AS THIS.
                return;
            }   

            sendMessageToHost(new FindPredecessorRequest(peerID, destinationProtocol), closestNode.getHost());
                //the logic will probably go along the lines of the already done Join. 
                
        }
    }

    private ChordNode closestPrecedingNode(byte[] targetID) {
        LOGGER.info(fingers.toString());
        for (final BigInteger index : fingers.descendingKeySet()) {
            final FingerEntry entry = fingers.get(index);
            final ChordNode entrySuc = entry.getNode();
            if (entrySuc.isInInterval(self.getNodeID(), targetID, true, false)) {
                return entrySuc;
            }
        }
   
        return new ChordNode(self.getNodeID(), self.getHost());
    }

    private void uponFindPredecessorRequest(FindPredecessorRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        if (ChordInterval.isInInterval(msg.getPeerID(), self.getNodeID(), successor.getNodeID(), true, true)) {
            final LookupReply lookupReply = new LookupReply(msg.getPeerID());
            lookupReply.addAllPeers(successor.getPeers().get(IdentifierUtils.toHex(msg.getPeerID())));
            sendReply(lookupReply, msg.getDestinationProtocol());
        } else {
            final ChordNode closestNode = closestPrecedingNode(msg.getPeerID());
            if (closestNode.getHost().equals(self.getHost())) {
                LOGGER.info("HERE2 PROBLEM ALREADY MENTIONED : {}", msg.getPeerID().toString());
                final LookupReply lookupReply = new LookupReply(msg.getPeerID());
                lookupReply.addAllPeers(successor.getPeers().get(IdentifierUtils.toHex(msg.getPeerID())));
                sendReply(lookupReply, msg.getDestinationProtocol());

                return;
            }
            sendMessageToHost(new FindPredecessorRequest(msg.getPeerID(), msg.getDestinationProtocol()), closestNode.getHost());
        }
    }

    private void uponFindPredecessorRequestFail(ProtoMessage msg, Host host, short destProto,
                                                Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void join(ChordNode existingNode) {
        LOGGER.info("JOINING...");
        sendMessageToHost(new JoinLookup(existingNode.getNodeID(), self.getHost(), self.getNodeID(), self.getPeers()), existingNode.getHost());
    }

    private void uponJoinLookup(JoinLookup msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        joinFindPredecessor(msg.getTargetID(), msg.getOrigin(), msg.getOriginID(), msg.getOriginSuccessors());
    }

    private void uponJoinLookupFail(ProtoMessage msg, Host host, short destProto,
                                    Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void joinFindPredecessor(byte[] targetID, Host origin, byte[] originID, Map<String, Set<Host>> originSuccessors) {
        if (ChordInterval.isInInterval(targetID, self.getNodeID(), successor.getNodeID(), true, true)) {
            sendMessageToHost(new JoinLookupReply(successor.getNodeID(), successor.getHost(), successor.getPeers()), origin);
        } else {
            final ChordNode closestNode = closestPrecedingNode(targetID);
            sendMessageToHost(new JoinFindPredecessorRequest(targetID, origin, originID, originSuccessors), closestNode.getHost());
        }
    }

    private void uponJoinFindPredecessorRequest(JoinFindPredecessorRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        if (ChordInterval.isInInterval(msg.getTargetID(), self.getNodeID(), successor.getNodeID(), true, true)) {
            sendMessageToHost(new JoinLookupReply(successor.getNodeID(), successor.getHost(), successor.getPeers()), msg.getOrigin());
        } else {
            final ChordNode closestNode = closestPrecedingNode(msg.getTargetID());

            if (closestNode.getHost().equals(self.getHost())) {                
                successor = new ChordNode(msg.getOriginID(), msg.getOrigin(), msg.getOriginSuccessors());
                this.successor.addPeer(IdentifierUtils.toHex(msg.getOriginID()), msg.getOrigin());
                
                final byte[] startID = FingerEntry.calculateStart(self.getNodeID(), 1);
                fingers.put(BigInteger.ONE, new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                IdentifierUtils.toNumerical(FingerEntry.calculateStart(self.getNodeID(), 2)), false, false),
                 new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers()))); 

                sendMessageToHost(new JoinLookupReply(self.getNodeID(), self.getHost(), self.getPeers()), msg.getOrigin());
                return;
            }
            sendMessageToHost(new JoinFindPredecessorRequest(msg.getTargetID(), msg.getOrigin(), msg.getOriginID(), msg.getOriginSuccessors()), closestNode.getHost());
        }
    }

    private void uponJoinFindPredecessorRequestFail(ProtoMessage msg, Host host, short destProto,
                                                    Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponJoinLookupReply(JoinLookupReply msg, Host from, short sourceProto, int channelId) {
        //-------TODO
        //THIS IS NOT DONE YET, PROBABLY, WE HAVE TO CHANGE THE MESSAGE, TO ALSO SEND SUCESSOR
        //THIS ONLY WORKS BECAUSE I DID FOR 2 NODES IN THE SYSTEM
        //AND IMPLEMENTED P2P BETWEEN BOTH FULLY
        //HOWEVER, ANY OTHER NODE IN THE SYSTEM, NEEDS BOTH THE PRE ALREADY IN MESSAGE AND IT'S PREVIOUS SUCESSOR
        //WHICH WILL BE THIS ONE
        //THIS PREDECESSOR = MSG, THIS SUCESSOR = MSGSUCESSOR, --> AND ... 
        LOGGER.debug("Received {} from {}", msg, from);
        predecessor = new ChordNode(msg.getSuccessorID(), msg.getSuccessorHost(), msg.getSuccessorPeers());
        successor = new ChordNode(msg.getSuccessorID(), msg.getSuccessorHost(), msg.getSuccessorPeers());
        successor.addPeer(IdentifierUtils.toHex(successor.getNodeID()), successor.getHost());
        LOGGER.info("JOIN LOOKUP REPLY. SUCCESSOR: " + successor);
        

        final byte[] startID = FingerEntry.calculateStart(self.getNodeID(), 1);
        fingers.put(BigInteger.ONE, new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                IdentifierUtils.toNumerical(FingerEntry.calculateStart(self.getNodeID(), 2)), false, false),
                 new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers()))); 
    }

    private void uponJoinLookupReplyFail(ProtoMessage msg, Host host, short destProto,
                                         Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponStabilize(StabilizeTimer timer, long timerID) {
        LOGGER.debug("Stabilizing...");
        sendMessageToHost(new StabilizeGetPredecessorRequest(), successor.getHost());
    }

    private void uponStabilizeGetPredecessorRequest(StabilizeGetPredecessorRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        sendMessageToHost(new StabilizeGetPredecessorReply(predecessor.getNodeID(), predecessor.getHost(), predecessor.getPeers()), from);
    }

    private void uponStabilizeGetPredecessorRequestFail(ProtoMessage msg, Host host, short destProto,
                                                        Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponStabilizeGetPredecessorReply(StabilizeGetPredecessorReply msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        if (ChordInterval.isInInterval(msg.getPredecessorID(), self.getNodeID(), successor.getNodeID(), true, false)) {
            successor = new ChordNode(msg.getPredecessorID(), msg.getPredecessorHost(), msg.getPredecessorPeers());
            final byte[] startID = FingerEntry.calculateStart(self.getNodeID(), 1);
            fingers.put(BigInteger.ONE, new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                    IdentifierUtils.toNumerical(FingerEntry.calculateStart(self.getNodeID(), 2)), false, false),
                    new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers())));
        }
        sendMessageToHost(new NotifyRequest(self.getNodeID()), successor.getHost());
    }

    private void uponStabilizeGetPredecessorReplyFail(ProtoMessage msg, Host host, short destProto,
                                                      Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponNotifyRequest(NotifyRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        if (predecessor == null || ChordInterval.isInInterval(msg.getTargetID(), predecessor.getNodeID(), self.getNodeID(), true, false)) {
            predecessor = new ChordNode(msg.getTargetID(), from);
        }
    }

    private void uponNotifyRequestFail(ProtoMessage msg, Host host, short destProto,
                                       Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponFixFingers(FixFingersTimer timer, long timerID) {
        LOGGER.debug("Fixing fingers...");
        final BigInteger i = randomIndexExcludingFirst();
        final byte[] startID = FingerEntry.calculateStart(self.getNodeID(), i.intValue());
        fixFingersFindPredecessor(startID, i);
    }

    private BigInteger randomIndexExcludingFirst() {
        return BigInteger.valueOf(random.nextInt(M_BITS - 2) + 2L);
    }

    private void fixFingersFindPredecessor(byte[] targetID, BigInteger index) {
        if (ChordInterval.isInInterval(targetID, self.getNodeID(), successor.getNodeID(), true, true)) {
            fingers.put(index, new FingerEntry(targetID, new ChordInterval(IdentifierUtils.toNumerical(targetID),
                    IdentifierUtils.toNumerical(FingerEntry.calculateStart(self.getNodeID(), index.add(BigInteger.valueOf(1)).intValue())),
                    false, false), new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers())));
        } else {
            final ChordNode closestNode = closestPrecedingNode(targetID);
            sendMessageToHost(new FixFingersFindPredecessorRequest(targetID, self.getHost(), index), closestNode.getHost());
        }
    }

    private void uponFixFingersFindPredecessorRequest(FixFingersFindPredecessorRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        if (ChordInterval.isInInterval(msg.getTargetID(), self.getNodeID(), successor.getNodeID(), true, true)) {
            sendMessageToHost(new FixFingersLookupReply(successor.getNodeID(), successor.getHost(), successor.getPeers(), msg.getIndex()), msg.getOrigin());
        } else {
            final ChordNode closestNode = closestPrecedingNode(msg.getTargetID());
            if (closestNode.getHost().equals(self.getHost())) {
                sendMessageToHost(new FixFingersLookupReply(successor.getNodeID(), successor.getHost(), successor.getPeers(), msg.getIndex()), msg.getOrigin());
                return;
            }
            sendMessageToHost(new FixFingersFindPredecessorRequest(msg.getTargetID(), msg.getOrigin(), msg.getIndex()), closestNode.getHost());
        }
    }

    private void uponFixFingersFindPredecessorRequestFail(ProtoMessage msg, Host host, short destProto,
                                                          Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponFixFingersLookupReply(FixFingersLookupReply msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        final byte[] startID = FingerEntry.calculateStart(self.getNodeID(), msg.getIndex().intValue());
        fingers.put(msg.getIndex(), new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                IdentifierUtils.toNumerical(FingerEntry.calculateStart(self.getNodeID(), msg.getIndex().add(BigInteger.valueOf(1)).intValue())),
                false, false), new ChordNode(msg.getSuccessorID(), msg.getSuccessorHost(), msg.getSuccessorPeers())));
    }

    private void uponFixFingersLookupReplyFail(ProtoMessage msg, Host host, short destProto,
                                               Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponInitialContactRequest(InitialContactRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        sendMessageToHost(new InitialContactReply(self.getNodeID(), self.getHost()), from);
    }

    private void uponInitialContactRequestFail(ProtoMessage msg, Host host, short destProto,
                                               Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponInitialContactReply(InitialContactReply msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        join(new ChordNode(msg.getNodeID(), msg.getHost()));
    }

    private void uponInitialContactReplyFail(ProtoMessage msg, Host host, short destProto,
                                             Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    private void sendMessageToHost(ProtoMessage message, Host destination) {
        if (connections.getOrDefault(destination, false)) {
            sendMessage(message, destination);
        } else {
            messages.computeIfAbsent(destination, k -> new LinkedList<>()).add(message);
            if (!connections.containsKey(destination)) {
                openConnection(destination);
                connections.put(destination, false); // pending connection
            }
        }
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        final Host peer = event.getNode();
        LOGGER.debug("Connection to {} is up", peer);
        connections.put(peer, true);
        final Queue<ProtoMessage> messageQueue = messages.getOrDefault(peer, new LinkedList<>());
        while (!messageQueue.isEmpty()) {
            System.out.println("QUEUE " + messageQueue);
            sendMessage(messageQueue.poll(), peer);
        }
    }

    //If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
    //protocol. Alternatively, we could do smarter things like retrying the connection X times.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        final Host peer = event.getNode();
        LOGGER.debug("Connection to {} is down cause {}", peer, event.getCause());
        connections.put(peer, false);
    }

    //If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
    //pending set. Note that this event is only triggered while attempting a connection, not after connection.
    //Thus, the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        final Host peer = event.getNode();
        LOGGER.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        connections.remove(peer);
        messages.remove(peer);
    }

    //If someone established a connection to me, this event is triggered. In this protocol we do nothing with this event.
    //If we want to add the peer to the membership, we will establish our own outgoing connection.
    // (not the smartest protocol, but its simple)
    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        LOGGER.trace("Connection from {} is up", event.getNode());
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        LOGGER.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* --------------------------------- Metrics ---------------------------- */

    //If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
    //periodically by the channel. This is NOT a protocol timer, but a channel event.
    //Again, we are just showing some of the information you can get from the channel, and use how you see fit.
    //"getInConnections" and "getOutConnections" returns the currently established connection to/from me.
    //"getOldInConnections" and "getOldOutConnections" returns connections that have already been closed.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        LOGGER.info(sb);
    }
}