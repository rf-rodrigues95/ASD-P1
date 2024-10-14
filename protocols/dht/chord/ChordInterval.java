package protocols.dht.chord;

import utils.IdentifierUtils;

import java.math.BigInteger;

public class ChordInterval {
    private final BigInteger start;
    private final BigInteger end;
    private final boolean isStartOpen;
    private final boolean isEndClosed;

    public ChordInterval(BigInteger start, BigInteger end, boolean isStartOpen, boolean isEndClosed) {
        if (start.compareTo(BigInteger.ZERO) < 0 || end.compareTo(BigInteger.ZERO) < 0) {
            throw new IllegalArgumentException("Start and end values must be non-negative.");
        }
        this.start = start;
        this.end = end;
        this.isStartOpen = isStartOpen;
        this.isEndClosed = isEndClosed;
    }

    public static boolean isInInterval(BigInteger target, BigInteger start, BigInteger end, boolean isStartOpen, boolean isEndClosed) {
        final ChordInterval interval = new ChordInterval(start, end, isStartOpen, isEndClosed);
        return interval.contains(target);
    }

    public boolean contains(BigInteger id) {
        if (start.equals(end)) {
            return false;
        } else if (start.compareTo(end) < 0) {
            return (isStartOpen ? id.compareTo(start) > 0 : id.compareTo(start) >= 0) &&
                   (isEndClosed ? id.compareTo(end) <= 0 : id.compareTo(end) < 0);
        } else {
            return (isStartOpen ? id.compareTo(start) > 0 : id.compareTo(start) >= 0) ||
                   (id.compareTo(BigInteger.ZERO) >= 0 && (isEndClosed ? id.compareTo(end) <= 0 : id.compareTo(end) < 0));
        }
    }

    @Override
    public String toString() {
        return (isStartOpen ? "(" : "[") + start.toString() + ", " + end.toString() + (isEndClosed ? "]" : ")");
    }
}
