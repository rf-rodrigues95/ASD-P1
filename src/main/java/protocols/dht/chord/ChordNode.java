package protocols.dht.chord;

import java.math.BigInteger;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashProducer;

public class ChordNode {

	private final byte[] peerID;
	private final Host host;
	private ChordNode successor;
	private ChordNode predecessor;
	private final ChordNode[] fingers;
	private final int max;
	
	public ChordNode(byte[] pid, Host h, int max) {
		this.peerID = pid;
		this.host = h;
		this.successor = null;
		this.predecessor = null;
		this.fingers = new ChordNode[max];
		this.max = max;
	}
	
	public byte[] getPeerID() {
		return this.peerID.clone();
	}
	
	public String getPeerIDHex() {
        return HashProducer.toNumberFormat(peerID).toString(16);
    }

    public Host getHost() {
        return host;
    }
    
    public ChordNode getSuccessor() {
    	return successor;
    }
	
    public void setSuccessor(ChordNode suc) {
    	this.successor = suc;
    }
    
    public ChordNode getPredecessor() {
    	return predecessor;
    }
    
    public void setPredecessor(ChordNode pre) {
    	this.predecessor = pre;
    }
	
    public ChordNode[] getFingerTable() {
    	return fingers;
    }
    
    private boolean inRange(byte[] pid, byte[] suc) {
    	BigInteger pidNum = HashProducer.toNumberFormat(pid);
        BigInteger nodeNum= HashProducer.toNumberFormat(this.peerID);
        BigInteger succNum = HashProducer.toNumberFormat(suc);
                
        return (pidNum.compareTo(nodeNum) > 0 && pidNum.compareTo(succNum) <= 0);
    
    }
    
    //ask node n(self) to find the successor of id
    public ChordNode findSuccessor(byte[] pid) {
    	if (successor != null && inRange(pid, successor.getPeerID())) 
    		return successor;
    	else {
    		ChordNode n = closestPreNode(pid); //closest preceding node
    		if (n == this)
    			return this;
    		
    		return n.findSuccessor(pid);
    	}
    				
    }
    
    private ChordNode closestPreNode(byte[] pid) {
    	for(int i = max-1; i >= 0; i --) {
    		if (fingers[i] != null && inRange(fingers[i].getPeerID(), pid))
    			return fingers[i];
    	}
    	
    	//self
    	return this;
    }

    //FIRST NODE THAT SUCCEDES...
    //finger[k] = node + 2^(k-1)
    private void createFingers() {
    	if (successor == null)
    		return;
    	
    	fingers[0] = this;
    	for(int k = 1; k < max; k++) {
    		
    		byte[] target= HashProducer.toNumberFormat(peerID)
    				.add(BigInteger.valueOf(2).shiftLeft(k-1))
    	    		.mod(BigInteger.valueOf(2).shiftLeft(max))
    	    		.toByteArray();
    		
    		fingers[k] = findSuccessor(target);
    	}
    }
    
    /*
    
    public void join(ChordNode newNode) {
        successor = findSuccessor(newNode.getPeerID());
        successor.setPredecessor(newNode);
        predecessor = successor.getPredecessor();

        if (predecessor != null) {
            predecessor.setSuccessor(successor);
        }

        createFingers();
    }
    */
    
    public void join(ChordNode existingNode) {
        successor = existingNode.findSuccessor(peerID);
        
        if(successor.getPeerID() == peerID)
        	successor.setPredecessor(successor.getPredecessor());
        else
        	successor.setPredecessor(this);
        
        //ChordNode succPre = successor.getPredecessor();
        //successor.setPredecessor(this);
        
        ChordNode succPre = successor.getPredecessor();
        if (succPre != null && succPre != this) {
            predecessor = succPre;
            predecessor.setSuccessor(this);
        }

        createFingers();
    }
    
}
