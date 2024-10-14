package protocols.dht.replies;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.IdentifierUtils;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class LookupReply extends ProtoReply {

    public static final short REPLY_ID = 502;
    private final byte[] peerID;
    private final Set<Host> peers;

    public LookupReply(byte[] peerID) {
        super(REPLY_ID);
        this.peerID = peerID.clone();
        this.peers = new HashSet<>();
    }

    public byte[] getPeerID() {
        return this.peerID.clone();
    }

    public BigInteger getPeerIDNumerical() {
        return IdentifierUtils.toNumerical(peerID);
    }

    public String getPeerIDHex() {
        return IdentifierUtils.toHex(peerID);
    }

    public Iterator<Host> getPeersIterator() {
        return this.peers.iterator();
    }

    public void addPeer(Host h) {
        this.peers.add(h);
    }

    public void addAllPeers(Set<Host> peers) {
        this.peers.addAll(peers);
    }

    public String toString() {
        final StringBuilder reply = new StringBuilder("LookupReply for " + this.getPeerIDHex() + " containing set (" + this.peers.size() + " elements):\n");
        for (final Host h : this.peers) {
            reply.append("\t").append(h).append("\n");
        }
        return reply.toString();
    }
}