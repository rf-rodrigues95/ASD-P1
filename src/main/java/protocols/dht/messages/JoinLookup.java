package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.IdentifierUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JoinLookup extends ProtoMessage {

    public static final short MSG_ID = 507;
    public static ISerializer<JoinLookup> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinLookup joinLookup, ByteBuf out) throws IOException {
            out.writeInt(joinLookup.targetID.length);
            out.writeBytes(joinLookup.targetID);

            Host.serializer.serialize(joinLookup.origin, out);

            // Serialize originID
            out.writeInt(joinLookup.originID.length);
            out.writeBytes(joinLookup.originID);

            out.writeInt(joinLookup.originSuccessors.size());
            for (Map.Entry<String, Set<Host>> entry : joinLookup.originSuccessors.entrySet()) {
                out.writeInt(entry.getKey().length());
                out.writeBytes(entry.getKey().getBytes());
                out.writeInt(entry.getValue().size());
                for (Host h : entry.getValue()) {
                    Host.serializer.serialize(h, out);
                }
            }
        }

        @Override
        public JoinLookup deserialize(ByteBuf in) throws IOException {
            final int targetLength = in.readInt();
            final byte[] targetBytes = new byte[targetLength];
            in.readBytes(targetBytes);

            final Host originHost = Host.serializer.deserialize(in);

            final int originLength = in.readInt();
            final byte[] originBytes = new byte[originLength];
            in.readBytes(originBytes);

            final int successorsSize = in.readInt();
            final Map<String, Set<Host>> originSuccessorsMap = new java.util.HashMap<>();
            for (int i = 0; i < successorsSize; i++) {
                final int keyLength = in.readInt();
                final byte[] keyBytes = new byte[keyLength];
                in.readBytes(keyBytes);
                final String key = new String(keyBytes);
                final int sizeHosts = in.readInt();
                final Set<Host> hosts = new HashSet<>();
                for (int j = 0; j < sizeHosts; j++) {
                    hosts.add(Host.serializer.deserialize(in));
                }
                originSuccessorsMap.put(key, hosts);
            }

            return new JoinLookup(targetBytes, originHost, originBytes, originSuccessorsMap);
        }
    };

    private final byte[] targetID;
    private final Host origin;
    private final byte[] originID;
    private final Map<String, Set<Host>> originSuccessors;

    public JoinLookup(byte[] targetID, Host origin, byte[] originID, Map<String, Set<Host>> originSuccessors) {
        super(MSG_ID);
        this.targetID = targetID;
        this.origin = origin;
        this.originID = originID;
        this.originSuccessors = originSuccessors;
    }

    public byte[] getOriginID() {
        return originID;
    }

    public Map<String, Set<Host>> getOriginSuccessors() {
        return originSuccessors;
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
               ", originID=" + IdentifierUtils.toHex(originID) +
               ", originSuccessors=" + originSuccessors +
               '}';
    }
}