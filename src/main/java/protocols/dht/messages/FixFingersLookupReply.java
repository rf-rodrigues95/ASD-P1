package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.IdentifierUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FixFingersLookupReply extends ProtoMessage {

    public static final short MSG_ID = 505;
    public static ISerializer<FixFingersLookupReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(FixFingersLookupReply fixFingersLookupReply, ByteBuf out) throws IOException {
            out.writeInt(fixFingersLookupReply.successorID.length);
            out.writeBytes(fixFingersLookupReply.successorID);
            final byte[] bytesIndex = fixFingersLookupReply.index.toByteArray();
            out.writeInt(bytesIndex.length);
            out.writeBytes(bytesIndex);
            Host.serializer.serialize(fixFingersLookupReply.successorHost, out);
            out.writeInt(fixFingersLookupReply.successorPeers.size());
            for (Map.Entry<String, Set<Host>> entry : fixFingersLookupReply.successorPeers.entrySet()) {
                out.writeInt(entry.getKey().length());
                out.writeBytes(entry.getKey().getBytes());
                out.writeInt(entry.getValue().size());
                for (Host h : entry.getValue()) {
                    Host.serializer.serialize(h, out);
                }
            }
        }

        @Override
        public FixFingersLookupReply deserialize(ByteBuf in) throws IOException {
            final int lengthSuccessorID = in.readInt();
            final byte[] bytesSuccessorID = new byte[lengthSuccessorID];
            in.readBytes(bytesSuccessorID);
            final int lengthIndex = in.readInt();
            final byte[] bytesIndex = new byte[lengthIndex];
            in.readBytes(bytesIndex);
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
            return new FixFingersLookupReply(bytesSuccessorID, h, peers, new BigInteger(bytesIndex));
        }
    };
    private final byte[] successorID;
    private final Host successorHost;
    private final Map<String, Set<Host>> successorPeers;
    private final BigInteger index;

    public FixFingersLookupReply(byte[] successorID, Host successorHost, Map<String, Set<Host>> successorPeers, BigInteger index) {
        super(MSG_ID);
        this.successorID = successorID;
        this.successorHost = successorHost;
        this.successorPeers = successorPeers;
        this.index = index;
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

    public BigInteger getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "FixFingersLookupReply{" +
               "successorID=" + IdentifierUtils.toHex(successorID) +
               ", successorHost=" + successorHost +
               ", successorPeers=" + successorPeers +
               ", index=" + index +
               '}';
    }
}