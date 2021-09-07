package com.github.mdc.common;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

/**
 * 
 * @author Arun
 * Zookeeper operations
 */
public class ZookeeperOperations{
	private static Logger log = Logger.getLogger(ZookeeperOperations.class);

	/**
	 * Persistent Node create operation. 
	 */
	public static Zk<CuratorFramework,String,String,String> persistentCreate = (final CuratorFramework cf,final String path,final String child,final String data)->{
		try {
			cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path+MDCConstants.BACKWARD_SLASH+child, data.getBytes());
			return path+child+MDCConstants.ZKOPERATION_CREATED;
		} catch (Exception ex) {
			log.error("ZookeeperOperations create persistent znode error, See Cause below: \n",ex);
			return null;
		}
	};
	
	
	
	/**
	 * Ephemeral sequential node create operation.
	 */
	public static Zk<CuratorFramework,String,String,String> ephemeralSequentialCreate = (final CuratorFramework cf,final String path,final String child,final String data)->{
		try {
			cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path+MDCConstants.BACKWARD_SLASH+child, data.getBytes());
			return path+child+MDCConstants.ZKOPERATION_CREATED;
		} catch (Exception ex) {
			log.error("ZookeeperOperations create ephemeral sequential znode error, See Cause below: \n",ex);
			return null;
		}
	};
	
	/**
	 * Operation to obtain child nodes information of a node. 
	 */
	public static Zk<CuratorFramework,String,String,String> childNodes = (final CuratorFramework cf,final String path,final String child,final String data)->{
		try {
			if(child!=null)return cf.getChildren().forPath(path+MDCConstants.BACKWARD_SLASH+child);
			else return cf.getChildren().forPath(path);
		} catch (Exception ex) {
			log.error("ZookeeperOperations retrieve child nodes error, See Cause below: \n",ex);
			return new ArrayList<>();
		}
	};
	/**
	 * Operation to obtain child nodes with path.
	 */
	public static Zk<CuratorFramework,String,String,String> childNodesWithPath = (final CuratorFramework cf,final String path,final String child,final String data)->{
		try {
			var childnodes = cf.getChildren().forPath(path);
			return childnodes.stream().sorted().map(childnode->path+MDCConstants.BACKWARD_SLASH+childnode).collect(Collectors.toList());
		} catch (Exception ex) {
			log.error("ZookeeperOperations retrieve child nodes with path error, See Cause below: \n",ex);
			return null;
		}
	};
	
	/**
	 * Operation to obtain node data.
	 */
	public static Zk<CuratorFramework,String,String,String> nodedata = (final CuratorFramework cf,final String path,final String child,final String data)->{
		try {
			if(child!=null)
			return new String(cf.getData().forPath(path+MDCConstants.BACKWARD_SLASH+child));
			return new String(cf.getData().forPath(path));
		} catch (Exception ex) {
			log.error("ZookeeperOperations retrieve node data error, See Cause below: \n",ex);
			return null;
		}
	};
	
	/**
	 * Operation to delete node.
	 */
	public static Zk<CuratorFramework,String,String,String> deletenode = (final CuratorFramework cf,final String path,final String child,final String data)->{
		try {
			if(child!=null)
			cf.delete().deletingChildrenIfNeeded().forPath(path+MDCConstants.BACKWARD_SLASH+child);
			cf.delete().deletingChildrenIfNeeded().forPath(path);
			return null;
		} catch (Exception ex) {
			log.error("ZookeeperOperations delete node error, See Cause below: \n",ex);
			return null;
		}
	};
	
	/**
	 * Operation to obtain nodes data 
	 */
	@SuppressWarnings("unchecked")
	public static Zk<CuratorFramework,String,String,String> nodesdata = (final CuratorFramework cf,final String path,final String child,final String data)->{
		try {
			var childnodes=(List<String>) ZookeeperOperations.childNodes.invoke(cf, path, null, null);
			return childnodes.stream().map(childnode->ZookeeperOperations.nodedata.invoke(cf, path,childnode,null)).collect(Collectors.toList());
		} catch (Exception ex) {
			log.error("ZookeeperOperations retrieve nodes data error, See Cause below: \n",ex);
			return null;
		}
	};
	
	/**
	 * Operation to write data to node.
	 */
	@SuppressWarnings("finally")
	public static Zk<CuratorFramework,String,String,String> writedata = (final CuratorFramework cf,final String path,final String child,final String data)->{
		try {
			cf.setData().forPath(path, data.getBytes());
		} catch (Exception ex) {
			log.error("ZookeeperOperations set node data error, See Cause below: \n",ex);
		}
		finally {
			return null;
		}
	};
	
	/**
	 * Operation to obtain information on whether node exists or not. 
	 */
	@SuppressWarnings("finally")
	public static Zk<CuratorFramework,String,String,String> checkexists = (final CuratorFramework cf,final String path,final String child,final String data)->{
		var isexists = false;
		try {
			if(child!=null)
				isexists =  cf.checkExists().forPath(path+MDCConstants.BACKWARD_SLASH+child)!=null;
			else isexists = cf.checkExists().forPath(path)!=null;
		} catch (Exception ex) {
			log.error("ZookeeperOperations check node exists error, See Cause below: \n",ex);
		}
		finally {
			return isexists;
		}
	};
	
	
	
	public static ZkConnectivity<CuratorFramework,ConnectionStateListener> addconnectionstate = (final CuratorFramework cf,ConnectionStateListener connectionstatelistener)->{
		cf.getConnectionStateListenable().addListener(connectionstatelistener);
	};
	
	
	
	
}