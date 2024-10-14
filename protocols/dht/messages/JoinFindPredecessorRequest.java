package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.IdentifierUtils;

import java.io.IOException;

public class JoinFindPredecessorRequest extends ProtoMessage {

    public static final short MSG_ID = 506;
    public static ISerializer<JoinFindPredecessorRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinFindPredecessorRequest joinFindPredecessorRequest, ByteBuf out) throws IOException {
            out.writeInt(joinFindPredecessorRequest.targetID.length);
            out.writeBytes(joinFindPredecessorRequest.targetID);
            Host.serializer.serialize(joinFindPredecessorRequest.origin, out);
        }

        @Override
        public JoinFindPredecessorRequest deserialize(ByteBuf in) throws IOException {
            final int length = in.readInt();
            final byte[] bytes = new byte[length];
            in.readBytes(bytes);
            final Host h = Host.serializer.deserialize(in);
            return new JoinFindPredecessorRequest(bytes, h);
        }
    };
    private final byte[] targetID;
    private final Host origin;

    public JoinFindPredecessorRequest(byte[] targetID, Host origin) {
        super(MSG_ID);
        this.targetID = targetID;
        this.origin = origin;
    }

    public byte[] getTargetID() {
        return targetID;
    }

    public Host getOrigin() {
        return origin;
    }

    @Override
    public String toString() {
        return "JoinFindPredecessorRequest{" +
               "targetID=" + IdentifierUtils.toHex(targetID) +
               ", origin=" + origin +
               '}';
    }
}