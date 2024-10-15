package protocols.point2point;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.Lookup;
import protocols.point2point.messages.PayloadMessage;
import protocols.point2point.notifications.Deliver;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class Point2PointCommunication extends GenericProtocol {
    public static final short PROTOCOL_ID = 400;
    public static final String PROTOCOL_NAME = "p2p-communication";
    private static final Logger LOGGER = LogManager.getLogger(Point2PointCommunication.class);
    private final Host myself;
    private final short dhtProtocolID;
    private final int channelId;
    private final Map<Host, Boolean> connections;
    private final Map<Host, Queue<ProtoMessage>> messages;
    private final Queue<Send> sendQueue;

    public Point2PointCommunication(Properties props, Host commHost, short dhtProtocolID) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = commHost;
        this.dhtProtocolID = dhtProtocolID;
        this.connections = new HashMap<>();
        this.messages = new HashMap<>();
        this.sendQueue = new LinkedList<>();

        final String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds
        final Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, String.valueOf(Integer.parseInt(props.getProperty("port")) + 1)); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        registerRequestHandler(Send.REQUEST_ID, this::uponSend);
        registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);

        registerMessageSerializer(channelId, PayloadMessage.MSG_ID, PayloadMessage.serializer);
        registerMessageHandler(channelId, PayloadMessage.MSG_ID, this::uponPayloadMessage, this::uponPayloadMessageFail);

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) {
        // empty
    }

    private void uponSend(Send request, short protoID) {
        LOGGER.info("Received Send Request: " + request.toString());
        final Lookup lookup = new Lookup(request.getDestinationPeerID());
        sendQueue.add(request);
        sendRequest(lookup, dhtProtocolID);
    }

    private void uponLookupReply(LookupReply reply, short protoID) {
        LOGGER.info("Received Lookup Reply: " + reply.toString());
        System.out.println("RECEIVED in P2P");
        final Host dest = reply.getPeersIterator().next();

        final Send send = sendQueue.poll();
        if(send != null) {
            final PayloadMessage payloadMessage = new PayloadMessage(send.getSenderPeerID(), send.getMessageID(), send.getMessagePayload());
            Host destTest = new Host(dest.getAddress(), dest.getPort() + 1);
            sendMessageToHost(payloadMessage, destTest);
        }
    }

    private void uponPayloadMessage(PayloadMessage msg, Host from, short sourceProto, int channelId) {
        LOGGER.debug("Received {} from {}", msg, from);
        System.out.println("SENT NOTIFICATION in P2P");
        triggerNotification(new Deliver(msg.getSenderID(), msg.getMessageID(), msg.getMessagePayload()));
    }

    private void uponPayloadMessageFail(ProtoMessage msg, Host host, short destProto,
                                        Throwable throwable, int channelId) {
        LOGGER.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

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
            System.out.println("SENT to host" + peer);
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