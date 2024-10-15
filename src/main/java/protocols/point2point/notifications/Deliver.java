package protocols.point2point.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import utils.HashProducer;

import java.math.BigInteger;
import java.util.UUID;

public class Deliver extends ProtoNotification {

    public final static short NOTIFICATION_ID = 402;

    private final byte[] senderID;
    private final UUID messageID;
    private final byte[] messagePayload;

    public Deliver(byte[] senderID, UUID mid, byte[] mPayload) {
        super(NOTIFICATION_ID);
        this.senderID = senderID.clone();
        this.messageID = mid;
        this.messagePayload = mPayload.clone();
    }


    public byte[] getSenderPeerID() {
        return this.senderID.clone();
    }

    public BigInteger getSenderPeerIDNumerical() {
        return HashProducer.toNumberFormat(senderID);
    }

    public String getSenderPeerIDHex() {
        return HashProducer.toNumberFormat(senderID).toString(16);
    }

    public byte[] getMessagePayload() {
        return this.messagePayload.clone();
    }

    public UUID getMessageID() {
        return this.messageID;
    }

    public String toString() {
        return "DeliverNotification from " + this.getSenderPeerIDHex() + " with message ID " + this.messageID + " payload of " + this.messagePayload.length + " bytes";
    }

    public String toStringLong() {
        String representation = this.toString();
        representation += "\nPayload:\n" + new String(this.messagePayload);
        return representation;
    }
}
