package com.github.mdc.common;

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * Jetty server to obtain files from scheduler daemon by executors
 * to download and parse the properties  
 * @author Administrator
 *
 */
public class MesosThirdPartyLibraryDistributor {
	private String dir;
	private Server server;
	public MesosThirdPartyLibraryDistributor(String dir) {
		this.dir = dir;
	}
	/**
	 * Start the server daemon
	 * @return
	 * @throws Throwable
	 */
	public int start() throws Throwable {
		server = new Server(new InetSocketAddress(MDCConstants.THISHOST,0));

		server.getConnectors()[0].getConnectionFactory(HttpConnectionFactory.class);
		server.setHandler(new HttpRequestHandler(dir));

		server.start();

		Connector[] port = server.getConnectors();
		return ((ServerConnector) port[0]).getLocalPort();
	}
	/**
	 * Stop the server
	 * @throws Throwable
	 */
	public void stop() throws Throwable {
		if(server!=null) {
			server.stop();
			server.destroy();
		}
	}
}
