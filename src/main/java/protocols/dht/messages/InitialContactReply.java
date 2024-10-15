package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.IdentifierUtils;

import java.io.IOException;

public class InitialContactReply extends ProtoMessage {

    public static final short MSG_ID = 513;
    public static ISerializer<InitialContactReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(InitialContactReply initialContactReply, ByteBuf out) throws IOException {
            out.writeInt(initialContactReply.nodeID.length);
            out.writeBytes(initialContactReply.nodeID);
            Host.serializer.serialize(initialContactReply.host, out);
        }

        @Override
        public InitialContactReply deserialize(ByteBuf in) throws IOException {
            final int length = in.readInt();
            final byte[] bytes = new byte[length];
            in.readBytes(bytes);
            final Host h = Host.serializer.deserialize(in);
            return new InitialContactReply(bytes, h);
        }
    };
    private final byte[] nodeID;
    private final Host host;

    public InitialContactReply(byte[] nodeID, Host host) {
        super(MSG_ID);
        this.nodeID = nodeID;
        this.host = host;
    }

    public byte[] getNodeID() {
        return nodeID;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "InitialContactReply{" +
               "nodeID=" + IdentifierUtils.toHex(nodeID) +
               ", host=" + host +
               '}';
    }
}