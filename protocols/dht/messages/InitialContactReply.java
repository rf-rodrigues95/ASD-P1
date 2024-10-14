package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.Arrays;

public class InitialContactReply extends ProtoMessage {

    public static final short MSG_ID = 513;
    public static ISerializer<InitialContactReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(InitialContactReply initialContactReply, ByteBuf out) {
            out.writeInt(initialContactReply.nodeID.length);
            out.writeBytes(initialContactReply.nodeID);
        }

        @Override
        public InitialContactReply deserialize(ByteBuf in) {
            final int length = in.readInt();
            final byte[] bytes = new byte[length];
            in.readBytes(bytes);
            return new InitialContactReply(bytes);
        }
    };
    private final byte[] nodeID;

    public InitialContactReply(byte[] nodeID) {
        super(MSG_ID);
        this.nodeID = nodeID;
    }

    public byte[] getNodeID() {
        return nodeID;
    }

    @Override
    public String toString() {
        return "InitialContactReply{" +
               "nodeID=" + Arrays.toString(nodeID) +
               '}';
    }
}