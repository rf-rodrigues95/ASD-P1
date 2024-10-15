package protocols.dht.chord;

import pt.unl.fct.di.novasys.network.data.Host;

public class ChordNodeSelf extends ChordNode {

    private final String nodeIDHex;

    public ChordNodeSelf(byte[] nodeID, Host host, String nodeIDHex) {
        super(nodeID, host);
        this.nodeIDHex = nodeIDHex;
    }

    public String getNodeIDHex() {
        return nodeIDHex;
    }

    @Override
    public String toString() {
        return "ChordNodeSelf{" +
               "nodeIDHex='" + nodeIDHex + '\'' +
               '}';
    }
}