package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class InitialContactRequest extends ProtoMessage {

    public static final short MSG_ID = 512;
    public static ISerializer<InitialContactRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(InitialContactRequest initialContactRequest, ByteBuf out) {
        }

        @Override
        public InitialContactRequest deserialize(ByteBuf in) {
            return new InitialContactRequest();
        }
    };

    public InitialContactRequest() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "InitialContactRequest{}";
    }
}