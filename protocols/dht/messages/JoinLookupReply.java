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

public class JoinLookupReply extends ProtoMessage {

    public static final short MSG_ID = 508;
    public static ISerializer<JoinLookupReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinLookupReply joinLookupReply, ByteBuf out) throws IOException {
            out.writeInt(joinLookupReply.successorID.length);
            out.writeBytes(joinLookupReply.successorID);
            Host.serializer.serialize(joinLookupReply.successorHost, out);
            out.writeInt(joinLookupReply.successorPeers.size());
            for (Map.Entry<String, Set<Host>> entry : joinLookupReply.successorPeers.entrySet()) {
                out.writeInt(entry.getKey().length());
                out.writeBytes(entry.getKey().getBytes());
                out.writeInt(entry.getValue().size());
                for (Host h : entry.getValue()) {
                    Host.serializer.serialize(h, out);
                }
            }
        }

        @Override
        public JoinLookupReply deserialize(ByteBuf in) throws IOException {
            final int length = in.readInt();
            final byte[] bytes = new byte[length];
            in.readBytes(bytes);
            final Host h = Host.serializer.deserialize(in);
            final int sizePeers = in.readInt();
            final Map<String, Set<Host>> peers = new java.util.HashMap<>();
            for (int i = 0; i < sizePeers; i++) {
                final int keyLength = in.readInt();
                final byte[] keyBytes = new byte[keyLength];
                in.readBytes(keyBytes);
                final String key = new String(keyBytes);
                final int sizeHosts = in.readInt();
                final Set<Host> hosts = new HashSet<>();
                for (int j = 0; j < sizeHosts; j++) {
                    hosts.add(Host.serializer.deserialize(in));
                }
                peers.put(key, hosts);
            }
            return new JoinLookupReply(bytes, h, peers);
        }
    };
    private final byte[] successorID;
    private final Host successorHost;
    private final Map<String, Set<Host>> successorPeers;

    public JoinLookupReply(byte[] successorID, Host successorHost, Map<String, Set<Host>> successorPeers) {
        super(MSG_ID);
        this.successorID = successorID;
        this.successorHost = successorHost;
        this.successorPeers = successorPeers;
    }

    public byte[] getSuccessorID() {
        return successorID;
    }

    public Host getSuccessorHost() {
        return successorHost;
    }

    public Map<String, Set<Host>> getSuccessorPeers() {
        return successorPeers;
    }

    @Override
    public String toString() {
        return "JoinLookupReply{" +
               "successorID=" + IdentifierUtils.toHex(successorID) +
               ", successorHost=" + successorHost +
               ", successorPeers=" + successorPeers +
               '}';
    }
}