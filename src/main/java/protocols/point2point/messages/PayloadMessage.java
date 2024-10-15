package protocols.point2point.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.IdentifierUtils;

import java.util.UUID;

public class PayloadMessage extends ProtoMessage {

    public static final short MSG_ID = 403;
    public static ISerializer<PayloadMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PayloadMessage payloadMessage, ByteBuf out) {
            out.writeInt(payloadMessage.senderID.length);
            out.writeBytes(payloadMessage.senderID);
            out.writeLong(payloadMessage.messageID.getMostSignificantBits());
            out.writeLong(payloadMessage.messageID.getLeastSignificantBits());
            out.writeInt(payloadMessage.messagePayload.length);
            out.writeBytes(payloadMessage.messagePayload);
        }

        @Override
        public PayloadMessage deserialize(ByteBuf in) {
            final int senderIDLength = in.readInt();
            final byte[] sid = new byte[senderIDLength];
            in.readBytes(sid);
            final long mostSigBits = in.readLong();
            final long leastSigBits = in.readLong();
            final UUID mid = new UUID(mostSigBits, leastSigBits);
            final int payloadLength = in.readInt();
            final byte[] mp = new byte[payloadLength];
            in.readBytes(mp);
            return new PayloadMessage(sid, mid, mp);
        }
    };
    private final byte[] senderID;
    private final UUID messageID;
    private final byte[] messagePayload;

    public PayloadMessage(byte[] senderID, UUID messageID, byte[] messagePayload) {
        super(MSG_ID);
        this.senderID = senderID;
        this.messageID = messageID;
        this.messagePayload = messagePayload;
    }

    public byte[] getSenderID() {
        return senderID;
    }

    public UUID getMessageID() {
        return messageID;
    }

    public byte[] getMessagePayload() {
        return messagePayload;
    }

    @Override
    public String toString() {
        return "PayloadMessage{" +
               "senderID=" + IdentifierUtils.toHex(senderID) +
               ", messageID=" + messageID +
               ", messagePayload=" + IdentifierUtils.toHex(messagePayload) +
               '}';
    }
}