package protocols.point2point;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.Lookup;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.Properties;

public class Point2PointCommunication extends GenericProtocol {

    public static final String PROTOCOL_NAME = "p2p-communication";
    public static final short PROTOCOL_ID = 400;
    private static final Logger LOGGER = LogManager.getLogger(Point2PointCommunication.class);
    private final Host myself;
    private final short dhtProtocolID;
    String s;

    public Point2PointCommunication(Host commHost, short dhtProtocolID) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = commHost;
        this.dhtProtocolID = dhtProtocolID;
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        //TODO: Must create tcp channel
        s = props.getProperty("node_opaque_id");

        registerRequestHandler(Send.REQUEST_ID, this::uponSendRequest);
        registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);
    }

    private void uponSendRequest(Send request, short protoID) {
        LOGGER.info("Received Send Request: " + request.toString());

        Lookup lookup = new Lookup(request.getDestinationPeerID());

        sendRequest(lookup, dhtProtocolID);
    }

    private void uponLookupReply(LookupReply reply, short protoID) {
        LOGGER.info("Received Lookup Reply: " + reply.toString());
    }

}