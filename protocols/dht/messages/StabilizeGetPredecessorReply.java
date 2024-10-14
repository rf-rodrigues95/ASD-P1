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

public class StabilizeGetPredecessorReply extends ProtoMessage {

    public static final short MSG_ID = 510;
    public static ISerializer<StabilizeGetPredecessorReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(StabilizeGetPredecessorReply stabilizeGetPredecessorReply, ByteBuf out) throws IOException {
            out.writeInt(stabilizeGetPredecessorReply.predecessorID.length);
            out.writeBytes(stabilizeGetPredecessorReply.predecessorID);
            Host.serializer.serialize(stabilizeGetPredecessorReply.predecessorHost, out);
            out.writeInt(stabilizeGetPredecessorReply.predecessorPeers.size());
            for (Map.Entry<String, Set<Host>> entry : stabilizeGetPredecessorReply.predecessorPeers.entrySet()) {
                out.writeInt(entry.getKey().length());
                out.writeBytes(entry.getKey().getBytes());
                out.writeInt(entry.getValue().size());
                for (Host h : entry.getValue()) {
                    Host.serializer.serialize(h, out);
                }
            }
        }

        @Override
        public StabilizeGetPredecessorReply deserialize(ByteBuf in) throws IOException {
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
            return new StabilizeGetPredecessorReply(bytes, h, peers);
        }
    };
    private final byte[] predecessorID;
    private final Host predecessorHost;
    private final Map<String, Set<Host>> predecessorPeers;

    public StabilizeGetPredecessorReply(byte[] predecessorID, Host predecessorHost, Map<String, Set<Host>> predecessorPeers) {
        super(MSG_ID);
        this.predecessorID = predecessorID;
        this.predecessorHost = predecessorHost;
        this.predecessorPeers = predecessorPeers;
    }

    public byte[] getPredecessorID() {
        return predecessorID;
    }

    public Host getPredecessorHost() {
        return predecessorHost;
    }

    public Map<String, Set<Host>> getPredecessorPeers() {
        return predecessorPeers;
    }

    @Override
    public String toString() {
        return "StabilizeGetPredecessorReply{" +
               "predecessorID=" + IdentifierUtils.toHex(predecessorID) +
               ", predecessorHost=" + predecessorHost +
               ", predecessorPeers=" + predecessorPeers +
               '}';
    }
}