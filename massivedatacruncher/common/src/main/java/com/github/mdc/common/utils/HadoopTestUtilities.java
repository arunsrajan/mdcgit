package com.github.mdc.common.utils;

import org.apache.hadoop.conf.Configuration;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

public class HadoopTestUtilities {

	private HadoopTestUtilities() {}
	
	public static HdfsLocalCluster initHdfsCluster(int port, 
			int httpport, int numnodes) throws Exception {
		HdfsLocalCluster hdfsLocalCluster = new HdfsLocalCluster.Builder()
			    .setHdfsNamenodePort(port)
			    .setHdfsNamenodeHttpPort(httpport)
			    .setHdfsTempDir("embedded_hdfs")
			    .setHdfsNumDatanodes(numnodes)
			    .setHdfsEnablePermissions(false)
			    .setHdfsFormat(true)
			    .setHdfsEnableRunningUserAsProxyUser(true)
			    .setHdfsConfig(new Configuration())
			    .build();
			                
			hdfsLocalCluster.start();
			return hdfsLocalCluster;
	}
	
}
