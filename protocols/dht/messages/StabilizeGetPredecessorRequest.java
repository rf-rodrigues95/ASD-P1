package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class StabilizeGetPredecessorRequest extends ProtoMessage {

    public static final short MSG_ID = 511;
    public static ISerializer<StabilizeGetPredecessorRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(StabilizeGetPredecessorRequest stabilizeGetPredecessorRequest, ByteBuf out) {
        }

        @Override
        public StabilizeGetPredecessorRequest deserialize(ByteBuf in) {
            return new StabilizeGetPredecessorRequest();
        }
    };

    public StabilizeGetPredecessorRequest() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "StabilizeGetPredecessorRequest{}";
    }
}