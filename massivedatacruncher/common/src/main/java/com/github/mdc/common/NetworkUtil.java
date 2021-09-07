package com.github.mdc.common;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;

import org.apache.log4j.Logger;

/**
 * Class to obtain the network address
 * @author arunsrajan
 *
 */
public class NetworkUtil {
	private static Logger log = Logger.getLogger(NetworkUtil.class);

	/**
	 * This functions returns listenable host address passing the host information. 
	 * @param host
	 * @return host address.
	 */
	public static String getNetworkAddress(String host) {
		log.debug("Entered NetworkUtil.getNetworkAddress()");
		try {
			var netinfs = NetworkInterface.getNetworkInterfaces();
			for(var netinf:Collections.list(netinfs)) {
				var inetAddresses = netinf.getInetAddresses();
		        for (var inetAddress : Collections.list(inetAddresses)) {
		            if(inetAddress.getHostAddress().equals(host)) {
		            	return host;
		            }
		        }
			}
			var hostia = InetAddress.getByName(host);
			if(hostia==null) {
				log.debug("Host Address Is Null: "+host);
				hostia = InetAddress.getLocalHost();
			}
			log.debug("Exiting NetworkUtil getNetworkAddress() method with host: "+hostia.getHostAddress());
	        return (hostia.getHostAddress()).trim(); 
		} catch (Exception ex) {
			log.error("Exception in NetworkUtil getNetworkAddress() method", ex);
		}
		log.debug("Exiting NetworkUtil.getNetworkAddress()");
		return MDCConstants.LOCALHOST;
	}
}
