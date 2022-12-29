package com.github.mdc.common;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import com.github.mdc.common.utils.HadoopTestUtilities;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

public class HadoopTestUtilitiesTest {
	
	@Test
	public void startHDFSCluster() throws Exception {
		HdfsLocalCluster cluster = HadoopTestUtilities.initHdfsCluster(9100, 9870, 2);
		assertEquals(Integer.valueOf(2), cluster.getHdfsNumDatanodes());
		cluster.stop(true);
	}

}
