package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.IdentifierUtils;

public class NotifyRequest extends ProtoMessage {

    public static final short MSG_ID = 509;
    public static ISerializer<NotifyRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(NotifyRequest notifyRequest, ByteBuf out) {
            out.writeInt(notifyRequest.targetID.length);
            out.writeBytes(notifyRequest.targetID);
        }

        @Override
        public NotifyRequest deserialize(ByteBuf in) {
            final int length = in.readInt();
            final byte[] bytes = new byte[length];
            in.readBytes(bytes);
            return new NotifyRequest(bytes);
        }
    };
    private final byte[] targetID;

    public NotifyRequest(byte[] targetID) {
        super(MSG_ID);
        this.targetID = targetID;
    }

    public byte[] getTargetID() {
        return targetID;
    }

    @Override
    public String toString() {
        return "NotifyRequest{" +
               "targetID='" + IdentifierUtils.toHex(targetID) + '\'' +
               '}';
    }
}