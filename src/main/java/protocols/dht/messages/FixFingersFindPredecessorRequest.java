package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.IdentifierUtils;

import java.io.IOException;
import java.math.BigInteger;

public class FixFingersFindPredecessorRequest extends ProtoMessage {

    public static final short MSG_ID = 504;
    public static ISerializer<FixFingersFindPredecessorRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(FixFingersFindPredecessorRequest fixFingersFindPredecessorRequest, ByteBuf out) throws IOException {
            out.writeInt(fixFingersFindPredecessorRequest.targetID.length);
            out.writeBytes(fixFingersFindPredecessorRequest.targetID);
            final byte[] bytesIndex = fixFingersFindPredecessorRequest.index.toByteArray();
            out.writeInt(bytesIndex.length);
            out.writeBytes(bytesIndex);
            Host.serializer.serialize(fixFingersFindPredecessorRequest.origin, out);
        }

        @Override
        public FixFingersFindPredecessorRequest deserialize(ByteBuf in) throws IOException {
            final int lengthTargetID = in.readInt();
            final byte[] bytesTargetID = new byte[lengthTargetID];
            in.readBytes(bytesTargetID);
            final int lengthIndex = in.readInt();
            final byte[] bytesIndex = new byte[lengthIndex];
            in.readBytes(bytesIndex);
            final Host h = Host.serializer.deserialize(in);
            return new FixFingersFindPredecessorRequest(bytesTargetID, h, new BigInteger(bytesIndex));
        }
    };
    private final byte[] targetID;
    private final Host origin;
    private final BigInteger index;

    public FixFingersFindPredecessorRequest(byte[] targetID, Host origin, BigInteger index) {
        super(MSG_ID);
        this.targetID = targetID;
        this.origin = origin;
        this.index = index;
    }

    public byte[] getTargetID() {
        return targetID;
    }

    public Host getOrigin() {
        return origin;
    }

    public BigInteger getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "FixFingersPredecessorRequest{" +
               "targetID=" + IdentifierUtils.toHex(targetID) +
               ", origin=" + origin +
               ", index=" + index +
               '}';
    }
}