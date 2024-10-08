package protocols.dht;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.apps.AutomatedApp;
import protocols.dht.chord.ChordNode;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.Lookup;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.ChannelEvent;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashProducer;

public class ChordDHT extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(ChordDHT.class);
	
	private static final int MAX = 1000;
	public final static short PROTOCOL_ID = 500;
	public final static String PROTOCOL_NAME = "chordDHT";
	
	private final Host myself;
	private ChordNode[] chordRing;
	private ChordNode currentNode;
	private int count;
	
	public ChordDHT(Host dhtHost) {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = dhtHost;
		chordRing = new ChordNode[MAX];
		count = 0;
	}
	
	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		
		//TODO: Must create tcp channel
		
		//int channelId = createChannel("TCP", props);
		//registerChannelEventHandler(channelId, (short) 1, this::uponChannelEvent);
		
		this.registerRequestHandler(Lookup.REQUEST_ID, this::uponLookup);
		
		String myPeerIDHex = props.getProperty(AutomatedApp.PROPERTY_NODE_ID);
		byte[] myPeerID = new BigInteger(myPeerIDHex, 16).toByteArray();
		
		this.currentNode = new ChordNode(myPeerID, this.myself, MAX);
		
		 if(count != 0)
			 //currentNode.setSuccessor(chordRing[0].findSuccessor(myPeerID)); 
		 	 //currentNode.join();
		 	  chordRing[0].join(currentNode);
		 else {
			 //currentNode.setPredecessor(currentNode);
			 currentNode.setSuccessor(currentNode);
		 }
		 chordRing[count++] = currentNode;
	
		 logger.info("COUNT: " + count);
	}
	 
	private void uponLookup(Lookup request, short protoID) {
		logger.info("Received LookupRequest: " + request.toString());
		
		byte[] requestedID = request.getPeerID();
		
		ChordNode newNode = new ChordNode(requestedID, this.myself, MAX);
		if(count == 1) {
			BigInteger orgNum = HashProducer.toNumberFormat(chordRing[0].getPeerID());
	        BigInteger reqNum= HashProducer.toNumberFormat(requestedID);
	        
	        if (orgNum.compareTo(reqNum) < 0) {
	        	chordRing[0].setSuccessor(newNode);
	        	newNode.setPredecessor(chordRing[0]);
	        } else {
	        	chordRing[0].setPredecessor(newNode);
	        	newNode.setSuccessor(chordRing[0]);
	        }
	    } else {
	    	newNode.join(chordRing[count-1]);
	    }
		chordRing[count++] = newNode;
		logger.info("COUNT: " + count);
		
		//currentNode.findSuccessor(requestedID);
		//ChordNode successor = newNode.findSuccessor(requestedID);
		
		BigInteger node = HashProducer.toNumberFormat(newNode.getPeerID());
		logger.info("NODE: " + node + "::" + newNode.getPeerIDHex());
		
		BigInteger successor = HashProducer.toNumberFormat(newNode.getSuccessor().getPeerID());
		logger.info("SUCC: " + successor + "::" +newNode.getSuccessor().getPeerIDHex());
		
		int c = node.compareTo(successor);
		if ( c > 0) logger.info("node is bigger");
		else logger.info("node is smaller");
		
		
		LookupReply lr = new LookupReply(request.getPeerID());
		lr.addElementToPeers(newNode.getSuccessor().getPeerID(), newNode.getSuccessor().getHost());
		
		sendReply(lr, protoID);
	}

}
