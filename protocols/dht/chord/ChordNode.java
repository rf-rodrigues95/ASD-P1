package protocols.dht.chord;

import pt.unl.fct.di.novasys.network.data.Host;
import utils.IdentifierUtils;

import java.math.BigInteger;
import java.util.*;

public class ChordNode {

    private byte[] nodeID;
    private Host host;
    private Map<String, Set<Host>> peers;

    public ChordNode(byte[] nodeID, Host host) {
        this.nodeID = nodeID;
        this.host = host;
        this.peers = new HashMap<>();
    }

    public ChordNode(byte[] nodeID, Host host, Map<String, Set<Host>> peers) {
        this.nodeID = nodeID;
        this.host = host;
        this.peers = peers;
    }

    public byte[] getNodeID() {
        return nodeID;
    }

    public void setNodeID(byte[] nodeID) {
        this.nodeID = nodeID;
    }

    public Host getHost() {
        return host;
    }

    public void setHost(Host host) {
        this.host = host;
    }

    public Map<String, Set<Host>> getPeers() {
        return peers;
    }

    public void addPeer(String peerIDHex, Host peerHost) {
        peers.putIfAbsent(peerIDHex, new HashSet<>());
        peers.get(peerIDHex).add(peerHost);
    }

    public boolean isInInterval(BigInteger start, BigInteger end, boolean isStartOpen, boolean isEndClosed) {
        return ChordInterval.isInInterval( IdentifierUtils.toNumerical(nodeID), start, end, isStartOpen, isEndClosed);
    }

    @Override
    public String toString() {
        return "ChordNode{" +
               "nodeID=" + Arrays.toString(nodeID) +
               ", host=" + host +
               '}';
    }
}