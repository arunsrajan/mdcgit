package com.github.mdc.common;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * 
 * @author Arun
 * Utility server for viewing, downloading the output from the data replicator.
 */
public class ServerUtils implements ServerUtilsMBean {


	Server server;

	/**
	 * Initialize the server.
	 */
	@Override
	public void init(Object... config) throws Exception {
		if (config == null || config.length % 2 == 0 || config.length == 1) {
			throw new Exception("Server requires Port and atleast one servlet and url to access");
		}
		else if (!(config[0] instanceof Integer)) {
			throw new Exception("Configuration port must be integer");
		}
		var port = (Integer) config[0];
		//Create the server object.
		server = new Server(port);
		var context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath(MDCConstants.BACKWARD_SLASH);
		server.setHandler(context);
		for (var conf = 1; conf < config.length; conf += 2) {
			if (!(config[conf] instanceof HttpServlet)) {
				throw new Exception(config[conf] + " which is of type " + config[conf].getClass().getName() + " must be instance of servlet javax.servlet.http.HttpServlet");
			}
			else if (!(config[conf + 1] instanceof String)) {
				throw new Exception("Path must be Url path of servlet " + config[conf].getClass().getName());
			}
			//Configure the server to receive the request.
			context.addServlet(new ServletHolder((Servlet) config[conf]), (String) config[conf + 1]);
		}
	}

	/**
	 * Start the server.
	 */
	@Override
	public void start() throws Exception {
		if (server != null) {
			server.start();
		}

	}

	/**
	 * Stop the server.
	 */
	@Override
	public void stop() throws Exception {
		if (server != null) {
			server.stop();
		}
	}

	/**
	 * Destroy the server.
	 */
	@Override
	public void destroy() throws Exception {
		if (server != null) {
			server.destroy();
		}

	}

}
