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
import java.util.Properties;
import java.util.Random;

import javax.crypto.spec.DESedeKeySpec;

public class ChordDHT extends GenericProtocol {

    public static final short PROTOCOL_ID = 500;
    public static final String PROTOCOL_NAME = "chord-dht";
    private static final int M_BITS = 256;
    private static final Logger LOGGER = LogManager.getLogger(ChordDHT.class);
    private final ChordNodeSelf self;
    private final int channelId;
    private final Random random;
    private ChordNode successor;
    private ChordNode predecessor;
    private HybridMap<BigInteger, FingerEntry> fingers;
    private int stabilizeTime;
    private int fixFingersTime;

    private String contact;

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

        final String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds
        final Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

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
    public void init(Properties props) throws HandlerRegistrationException {
        
        if (props.containsKey("contact")) {
            contact = props.getProperty("contact");
            this.registerRequestHandler(Lookup.REQUEST_ID, this::uponContact);
        } else { // first node
            this.registerRequestHandler(Lookup.REQUEST_ID, this::uponLookup);

            this.successor = new ChordNode(self.getNodeID(), self.getHost());
            this.successor.addPeer(self.getNodeIDHex(), self.getHost());
            final byte[] startID = FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), 1);
            this.fingers.put(BigInteger.ONE, new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                    IdentifierUtils.toNumerical(FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), 2)), false, false),
                    new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers())));
        }
        setupPeriodicTimer(new StabilizeTimer(), stabilizeTime, stabilizeTime);
        setupPeriodicTimer(new FixFingersTimer(), fixFingersTime, fixFingersTime);
    }

    private void uponContact(Lookup request, short protoID) {
        LOGGER.info("Received CONTACT Lookup Request of the form {} from {}", request, request.getPeerID());
        try {
            final String[] split = contact.split(":");
            final Host contactHost = new Host(InetAddress.getByName(split[0]), Short.parseShort(split[1]));
            //openConnection(contactHost); // TODO - asynch and test if this is needed
            sendMessage(new InitialContactRequest(), contactHost);
        } catch (Exception e) {
            LOGGER.error("Invalid contact on configuration: '" + contact);
            System.exit(-1);
        }
        
        //findPredecessor(request.getPeerID(), protoID);
    }

    private void uponLookup(Lookup request, short protoID) {
        LOGGER.info("Received Lookup Request of the form {} from {}", request, request.getPeerID());
        findPredecessor(request.getPeerID(), protoID);
    }

    private void findPredecessor(byte[] peerID, short destinationProtocol) {
        BigInteger pid =  IdentifierUtils.toNumerical(peerID);
        BigInteger pidStart =  IdentifierUtils.toNumerical(self.getNodeID());
        BigInteger pidEnd = IdentifierUtils.toNumerical(successor.getNodeID());

        if (pidStart.compareTo(BigInteger.ZERO) >= 0 && pidEnd.compareTo(BigInteger.ZERO) >= 0) {
            if (ChordInterval.isInInterval(pid, pidStart, pidEnd, true, true)) {
                final LookupReply lookupReply = new LookupReply(peerID);
                lookupReply.addAllPeers(successor.getPeers().get(IdentifierUtils.toHex(peerID)));
                sendReply(lookupReply, destinationProtocol);
            } 
        }
        else {
            LOGGER.info("here");
            final ChordNode closestNode = closestPrecedingNode(peerID);
            sendMessage(new FindPredecessorRequest(peerID, destinationProtocol), closestNode.getHost());
        }
    }

    private ChordNode closestPrecedingNode(byte[] targetID) {
        LOGGER.info("mmm" + fingers.descendingKeySet().toString());
        for (final BigInteger index : fingers.descendingKeySet()) {
            final FingerEntry entry = fingers.get(index);
            final ChordNode entrySuc = entry.getNode();

            LOGGER.info("mmm1" + index);

            LOGGER.info("mmm2" + entry.toString());

            LOGGER.info("mmm3" + entrySuc.toString());
            BigInteger pidStart =  IdentifierUtils.toNumerical(self.getNodeID());
            BigInteger pidEnd = IdentifierUtils.toNumerical(targetID);

            if (pidStart.compareTo(BigInteger.ZERO) >= 0 && pidEnd.compareTo(BigInteger.ZERO) >= 0) {
                if (entrySuc.isInInterval(pidStart, pidEnd, true, false)) {
                    return entrySuc;
                }
            }
        }
        return new ChordNode(self.getNodeID(), self.getHost());
    }

    private void uponFindPredecessorRequest(FindPredecessorRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        BigInteger pid =  IdentifierUtils.toNumerical(msg.getPeerID());
        BigInteger pidStart =  IdentifierUtils.toNumerical(self.getNodeID());
        BigInteger pidEnd = IdentifierUtils.toNumerical(successor.getNodeID());

        LOGGER.info("here");

        if (pidStart.compareTo(BigInteger.ZERO) >= 0 && pidEnd.compareTo(BigInteger.ZERO) >= 0) {
            if (ChordInterval.isInInterval(pid, pidStart, pidEnd, true, true)) {
                final LookupReply lookupReply = new LookupReply(msg.getPeerID());
                lookupReply.addAllPeers(successor.getPeers().get(IdentifierUtils.toHex(msg.getPeerID())));
                sendReply(lookupReply, msg.getDestinationProtocol());
            }
        } else {
            LOGGER.info("here");
            final ChordNode closestNode = closestPrecedingNode(msg.getPeerID());
            sendMessage(new FindPredecessorRequest(msg.getPeerID(), msg.getDestinationProtocol()), closestNode.getHost());
        }
    }

    private void uponFindPredecessorRequestFail(ProtoMessage msg, Host host, short destProto,
                                                Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void join(ChordNode existingNode) {
        predecessor = null;
        sendMessage(new JoinLookup(self.getNodeID(), self.getHost()), existingNode.getHost());
    }

    private void uponJoinLookup(JoinLookup msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        joinFindPredecessor(msg.getTargetID(), msg.getOrigin());
    }

    private void uponJoinLookupFail(ProtoMessage msg, Host host, short destProto,
                                    Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void joinFindPredecessor(byte[] targetID, Host origin) {
        BigInteger pid =  IdentifierUtils.toNumerical(targetID);
        BigInteger pidStart =  IdentifierUtils.toNumerical(self.getNodeID());
        BigInteger pidEnd = IdentifierUtils.toNumerical(successor.getNodeID());
        if (ChordInterval.isInInterval(pid, pidStart, pidEnd, true, true)) {
            sendMessage(new JoinLookupReply(successor.getNodeID(), successor.getHost(), successor.getPeers()), origin);
        } else {
            final ChordNode closestNode = closestPrecedingNode(targetID);
            sendMessage(new JoinFindPredecessorRequest(targetID, origin), closestNode.getHost());
        }
    }

    private void uponJoinFindPredecessorRequest(JoinFindPredecessorRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        BigInteger pid =  IdentifierUtils.toNumerical(msg.getTargetID());
        BigInteger pidStart =  IdentifierUtils.toNumerical(self.getNodeID());
        BigInteger pidEnd = IdentifierUtils.toNumerical(successor.getNodeID());
        if (ChordInterval.isInInterval(pid, pidStart, pidEnd, true, true)) {
            sendMessage(new JoinLookupReply(successor.getNodeID(), successor.getHost(), successor.getPeers()), msg.getOrigin());
        } else {
            final ChordNode closestNode = closestPrecedingNode(msg.getTargetID());
            sendMessage(new JoinFindPredecessorRequest(msg.getTargetID(), msg.getOrigin()), closestNode.getHost());
        }
    }

    private void uponJoinFindPredecessorRequestFail(ProtoMessage msg, Host host, short destProto,
                                                    Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponJoinLookupReply(JoinLookupReply msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        successor = new ChordNode(msg.getSuccessorID(), msg.getSuccessorHost(), msg.getSuccessorPeers());
        successor.addPeer(self.getNodeIDHex(), self.getHost());
        final byte[] startID = FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), 1);
        fingers.put(BigInteger.ONE, new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                IdentifierUtils.toNumerical(FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), 2)), false, false),
                new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers())));
    }

    private void uponJoinLookupReplyFail(ProtoMessage msg, Host host, short destProto,
                                         Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponStabilize(StabilizeTimer timer, long timerID) {
        LOGGER.debug("Stabilizing...");
        sendMessage(new StabilizeGetPredecessorRequest(), successor.getHost());
    }

    private void uponStabilizeGetPredecessorRequest(StabilizeGetPredecessorRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        sendMessage(new StabilizeGetPredecessorReply(predecessor.getNodeID(), predecessor.getHost(), predecessor.getPeers()), from);
    }

    private void uponStabilizeGetPredecessorRequestFail(ProtoMessage msg, Host host, short destProto,
                                                        Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponStabilizeGetPredecessorReply(StabilizeGetPredecessorReply msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        BigInteger pid =  IdentifierUtils.toNumerical(msg.getPredecessorID());
        BigInteger pidStart =  IdentifierUtils.toNumerical(self.getNodeID());
        BigInteger pidEnd = IdentifierUtils.toNumerical(successor.getNodeID());

        if (ChordInterval.isInInterval(pid, pidStart, pidEnd, true, false)) {
            successor = new ChordNode(msg.getPredecessorID(), msg.getPredecessorHost(), msg.getPredecessorPeers());
            final byte[] startID = FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), 1);
            fingers.put(BigInteger.ONE, new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                    IdentifierUtils.toNumerical(FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), 2)), false, false),
                    new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers())));
        }
        sendMessage(new NotifyRequest(self.getNodeID()), successor.getHost());
    }

    private void uponStabilizeGetPredecessorReplyFail(ProtoMessage msg, Host host, short destProto,
                                                      Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponNotifyRequest(NotifyRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        if (predecessor == null || ChordInterval.isInInterval(IdentifierUtils.toNumerical(msg.getTargetID()), IdentifierUtils.toNumerical(predecessor.getNodeID()), IdentifierUtils.toNumerical(self.getNodeID()), true, false)) {
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
        final byte[] startID = FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), i.intValue());
        fixFingersFindPredecessor(startID, i);
    }

    private BigInteger randomIndexExcludingFirst() {
        return BigInteger.valueOf(random.nextInt(M_BITS - 2) + 2L);
    }

    private void fixFingersFindPredecessor(byte[] targetID, BigInteger index) {
        if (ChordInterval.isInInterval(IdentifierUtils.toNumerical(targetID), IdentifierUtils.toNumerical(self.getNodeID()), IdentifierUtils.toNumerical(successor.getNodeID()), true, true)) {
            fingers.put(index, new FingerEntry(targetID, new ChordInterval(IdentifierUtils.toNumerical(targetID),
                    IdentifierUtils.toNumerical(FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), index.add(BigInteger.valueOf(1)).intValue())),
                    false, false), new ChordNode(successor.getNodeID(), successor.getHost(), successor.getPeers())));
        } else {
            final ChordNode closestNode = closestPrecedingNode(targetID);
            sendMessage(new FixFingersFindPredecessorRequest(targetID, self.getHost(), index), closestNode.getHost());
        }
    }

    private void uponFixFingersFindPredecessorRequest(FixFingersFindPredecessorRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        if (ChordInterval.isInInterval(IdentifierUtils.toNumerical(msg.getTargetID()), IdentifierUtils.toNumerical(self.getNodeID()), IdentifierUtils.toNumerical(successor.getNodeID()), true, true)) {
            sendMessage(new FixFingersLookupReply(successor.getNodeID(), successor.getHost(), successor.getPeers(), msg.getIndex()), msg.getOrigin());
        } else {
            final ChordNode closestNode = closestPrecedingNode(msg.getTargetID());
            sendMessage(new FixFingersFindPredecessorRequest(msg.getTargetID(), msg.getOrigin(), msg.getIndex()), closestNode.getHost());
        }
    }

    private void uponFixFingersFindPredecessorRequestFail(ProtoMessage msg, Host host, short destProto,
                                                          Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponFixFingersLookupReply(FixFingersLookupReply msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        final byte[] startID = FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), msg.getIndex().intValue());
        fingers.put(msg.getIndex(), new FingerEntry(startID, new ChordInterval(IdentifierUtils.toNumerical(startID),
                IdentifierUtils.toNumerical(FingerEntry.calculateStart(IdentifierUtils.toNumerical(self.getNodeID()), msg.getIndex().add(BigInteger.valueOf(1)).intValue())),
                false, false), new ChordNode(msg.getSuccessorID(), msg.getSuccessorHost(), msg.getSuccessorPeers())));
    }

    private void uponFixFingersLookupReplyFail(ProtoMessage msg, Host host, short destProto,
                                               Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponInitialContactRequest(InitialContactRequest msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        sendMessage(new InitialContactReply(self.getNodeID()), from);
    }

    private void uponInitialContactRequestFail(ProtoMessage msg, Host host, short destProto,
                                               Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponInitialContactReply(InitialContactReply msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        join(new ChordNode(msg.getNodeID(), from));
    }

    private void uponInitialContactReplyFail(ProtoMessage msg, Host host, short destProto,
                                             Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //If a connection is successfully established, this event is triggered. In this protocol, we want to add the
    //respective peer to the membership, and inform the Dissemination protocol via a notification.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        LOGGER.debug("Connection to {} is up", peer);
    }

    //If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
    //protocol. Alternatively, we could do smarter things like retrying the connection X times.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        LOGGER.debug("Connection to {} is down cause {}", peer, event.getCause());

    }

    //If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
    //pending set. Note that this event is only triggered while attempting a connection, not after connection.
    //Thus, the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        LOGGER.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
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