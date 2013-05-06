package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.EnumSet;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Collection;
import java.util.Map;

public class AutomaticBalancer {

	private static final Log LOG = LogFactory.getLog(AutomaticBalancer.class.getName());
	private DatanodeDescriptor sourceNode;
	private DatanodeDescriptor targetNode;

	//private static long BLOCK_SIZE_TO_FETCH = 64*1024*1024; //64MB

	private NetworkTopology cluster = new NetworkTopology();
	private BlockTokenSecretManager blockTokenSecretManager;
	private NavigableMap<String, DatanodeDescriptor> nodeMap;
	//private Collection<Block> recentInvalidateSets;
	private Map<String, List<Block>> movedBlockList;


	public AutomaticBalancer(DatanodeDescriptor source, DatanodeDescriptor target, NetworkTopology cluster, 
					NavigableMap<String, DatanodeDescriptor> map, Map<String, List<Block>> movedBlocks) {
		this.sourceNode = source;
		this.targetNode = target;
		this.cluster = cluster;
		this.nodeMap = map;
		this.movedBlockList = movedBlocks; 
	}

/*	private void getSourceBlocks() throws IOException{
		srcBlockList =  new ArrayList<BlockInfo>();
		BlockWithLocations[] blocks = namenode.getBlocks(sourceNode, BLOCK_SIZE_TO_FETCH).getBlocks();
		for(int i =0;i<blocks.length;i++) {
			BlockInfo info = new BlockInfo();
			info.setBlock(blocks[i].getBlock());
			for(String name : blocks[i].getDatanodes()) {
				DatanodeDescriptor node = (DatanodeDescriptor)cluster.getNode(name);
				if(node != null) {
					info.addLocation(node);
				}
			}

			if(isGoodBlockCandidate(info)) {
				srcBlockList.add(info);
			}
		}
	}
*/
	private boolean isGoodBlockCandidate(BlockInfo block) {
		if(movedBlockList.get(sourceNode.getStorageID())!=null && movedBlockList.get(sourceNode.getStorageID()).contains(block)) {
System.out.println("Not good becuase in invalidate set");
			return false;
		}
		//Check if block is already in target
		boolean goodBlock = true;
		if(block.inLocation(targetNode)) {
System.out.println("Not good because already in target node");
			goodBlock = false;	
		}
		
		if(goodBlock) {
			block.setProxySource(sourceNode);
			for(DatanodeDescriptor node : block.getLocations()) {
			//Check if any of the other replicas are located on the same rack. set proxy if there is 
				if(cluster.isOnSameRack(targetNode,node)) {
System.out.println("Found proxy node!!! " + node.getName());
					block.setProxySource(node);
					break;
				}
			}
		}

		/*boolean goodBlock = false;
		if(cluster.isOnSameRack(sourceNode, targetNode)) {
			goodBlock = true;
		}
		else {
			for(DatanodeDescriptor node : block.getLocations()) {
				//Check if any of the other replicas are located on the same rack. set proxy if there is 
				if(cluster.isOnSameRack(targetNode,node)) {
					block.setProxySource(node);
					goodBlock = true;
				}
				//else good block if source is on the same rack as any of the other replicas
				else if(cluster.isOnSameRack(sourceNode,node)) {
					block.setProxySource(sourceNode);
					goodBlock = true;
				}
			}
		}*/

		return goodBlock;
	}

	public void dispatchBlocks(BlocksWithLocations blocks, BlockTokenSecretManager manager) {
		this.blockTokenSecretManager = manager;
System.out.println("Roshan - Inside dispatchBlocks");
		for(BlockWithLocations block : blocks.getBlocks()) {
System.out.println("Roshan - Moving block - " + block.getBlock());
			BlockInfo info = new BlockInfo();
			info.setBlock(block.getBlock());
System.out.print("Replica locations - ");
			for(String name : block.getDatanodes()) {
System.out.print(name + " , ");
				try {
				//DatanodeDescriptor node = (DatanodeDescriptor)cluster.getNode(name);
				DatanodeDescriptor node = nodeMap.get(name);
				if(node != null) {
					info.addLocation(node);
				}}catch(Exception e) { 
e.printStackTrace(System.out);
System.out.println("Roshan - Exception!!!");
}
			}
System.out.println("");
			if(isGoodBlockCandidate(info)) {
				dispatch(info);
				if(movedBlockList.get(sourceNode.getStorageID())==null){
					movedBlockList.put(sourceNode.getStorageID(),new ArrayList<Block>());
				}
				movedBlockList.get(sourceNode.getStorageID()).add(info.getBlock());
			}
		}
                System.out.println("Roshan dispatched blocks");
	}

	private void dispatch(BlockInfo info) {

System.out.println("Moving " + info.getBlock().getBlockId() + "Source = " + sourceNode.getName() + " Target = " + targetNode.getName() + " Proxy = " + info.getProxySource().getName());

		Socket sock = new Socket();
		DataOutputStream out = null;
		DataInputStream in = null;
		try {
			sock.connect(NetUtils.createSocketAddr(
						targetNode.getName()), HdfsConstants.READ_TIMEOUT);
			sock.setKeepAlive(true);
			out = new DataOutputStream( new BufferedOutputStream(
						sock.getOutputStream(), FSConstants.BUFFER_SIZE));
			sendRequest(out, info);
			in = new DataInputStream( new BufferedInputStream(
						sock.getInputStream(), FSConstants.BUFFER_SIZE));
			receiveResponse(in);
			LOG.info( "Moving block " + info.getBlock().getBlockId() +
					" from "+ sourceNode.getName() + " to " +
					targetNode.getName() + " through " +
					info.getProxySource().getName() +
					" is succeeded." );
		} catch (IOException e) {
			LOG.warn("Error moving block "+info.getBlock().getBlockId()+
					" from " + sourceNode.getName() + " to " +
					targetNode.getName() + " through " +
					info.getProxySource().getName() +
					": "+e.getMessage());
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
			IOUtils.closeSocket(sock);

		}

	}

	/* Send a block replace request to the output stream*/
	private void sendRequest(DataOutputStream out, BlockInfo info) throws IOException {
		out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
		out.writeByte(DataTransferProtocol.OP_REPLACE_BLOCK);
		out.writeLong(info.getBlock().getBlockId());
		out.writeLong(info.getBlock().getGenerationStamp());
		Text.writeString(out, sourceNode.getStorageID()); //Avoid deleting block
		//Text.writeString(out, "");
		info.getProxySource().write(out);
		
		Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
		if (blockTokenSecretManager != null) {
			accessToken = blockTokenSecretManager.generateToken(null, info.getBlock(),
		  	EnumSet.of(BlockTokenSecretManager.AccessMode.REPLACE,
			BlockTokenSecretManager.AccessMode.COPY));
		}
		accessToken.write(out);
		out.flush();
	}

	/* Receive a block copy response from the input stream */
	private void receiveResponse(DataInputStream in) throws IOException {
		short status = in.readShort();
		if (status != DataTransferProtocol.OP_STATUS_SUCCESS) {
			if (status == DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN)
				throw new IOException("block move failed due to access token error");
			throw new IOException("block move is failed");
		}
	}

	private class BlockInfo {
		private Block block;
		private DatanodeDescriptor proxySource;	
		private List<DatanodeDescriptor> locations;

		public BlockInfo() { 
			locations = new ArrayList<DatanodeDescriptor>(3);
		}

		public void setBlock(Block block) {
			this.block = block;
		}

		public Block getBlock() {
			return block;
		}

		public void setProxySource(DatanodeDescriptor proxy) {
			this.proxySource = proxy;
		}

		public DatanodeDescriptor getProxySource() {
			return proxySource;
		}

		public void addLocation(DatanodeDescriptor loc) {
			if(!locations.contains(loc)) {
				locations.add(loc);
			}
		}

		public List<DatanodeDescriptor> getLocations() {
			return locations;
		}
		
		public boolean inLocation(DatanodeDescriptor node) {
			return locations.contains(node);
		}
	}
}

