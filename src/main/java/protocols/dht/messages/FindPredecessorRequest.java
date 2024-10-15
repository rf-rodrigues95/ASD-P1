package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.IdentifierUtils;

public class FindPredecessorRequest extends ProtoMessage {

    public static final short MSG_ID = 503;
    public static ISerializer<FindPredecessorRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindPredecessorRequest findPredecessorRequest, ByteBuf out) {
            out.writeInt(findPredecessorRequest.peerID.length);
            out.writeBytes(findPredecessorRequest.peerID);
            out.writeShort(findPredecessorRequest.destinationProtocol);
        }

        @Override
        public FindPredecessorRequest deserialize(ByteBuf in) {
            final int length = in.readInt();
            final byte[] bytes = new byte[length];
            in.readBytes(bytes);
            final short destProto = in.readShort();
            return new FindPredecessorRequest(bytes, destProto);
        }
    };
    private final byte[] peerID;
    private final short destinationProtocol;

    public FindPredecessorRequest(byte[] peerID, short destinationProtocol) {
        super(MSG_ID);
        this.peerID = peerID;
        this.destinationProtocol = destinationProtocol;
    }

    public byte[] getPeerID() {
        return peerID;
    }

    public short getDestinationProtocol() {
        return destinationProtocol;
    }

    @Override
    public String toString() {
        return "FindPredecessorRequest{" +
               "peerID=" + IdentifierUtils.toHex(peerID) +
               ", destinationProtocol=" + destinationProtocol +
               '}';
    }
}