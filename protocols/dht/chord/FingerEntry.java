package protocols.dht.chord;

import utils.IdentifierUtils;

import java.math.BigInteger;

public class FingerEntry {

    private static final int M_BITS = 256;
    private byte[] start;
    private ChordInterval interval;
    private ChordNode node; // successor

    public FingerEntry(byte[] start, ChordInterval interval, ChordNode node) {
        this.start = start;
        this.interval = interval;
        this.node = node;
    }

    public static byte[] calculateStart(BigInteger id, int i) {
        return id.add(BigInteger.valueOf(2).pow(i)).mod(BigInteger.valueOf(2).pow(M_BITS)).toByteArray();
    }

    public byte[] getStart() {
        return start;
    }

    public ChordInterval getInterval() {
        return interval;
    }

    public ChordNode getNode() {
        return node;
    }

    public void setNode(ChordNode node) {
        this.node = node;
    }

    @Override
    public String toString() {
        return "FingerEntry{" +
               "start=" + IdentifierUtils.toHex(start) +
               ", interval=" + interval +
               ", node=" + node +
               '}';
    }
}