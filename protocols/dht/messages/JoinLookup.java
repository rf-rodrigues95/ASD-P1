package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.IdentifierUtils;

import java.io.IOException;

public class JoinLookup extends ProtoMessage {

    public static final short MSG_ID = 507;
    public static ISerializer<JoinLookup> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinLookup joinLookup, ByteBuf out) throws IOException {
            out.writeInt(joinLookup.targetID.length);
            out.writeBytes(joinLookup.targetID);
            Host.serializer.serialize(joinLookup.origin, out);
        }

        @Override
        public JoinLookup deserialize(ByteBuf in) throws IOException {
            final int length = in.readInt();
            final byte[] bytes = new byte[length];
            in.readBytes(bytes);
            final Host h = Host.serializer.deserialize(in);
            return new JoinLookup(bytes, h);
        }
    };
    private final byte[] targetID;
    private final Host origin;

    public JoinLookup(byte[] targetID, Host origin) {
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
        return "JoinLookup{" +
               "targetID=" + IdentifierUtils.toHex(targetID) +
               ", origin=" + origin +
               '}';
    }
}