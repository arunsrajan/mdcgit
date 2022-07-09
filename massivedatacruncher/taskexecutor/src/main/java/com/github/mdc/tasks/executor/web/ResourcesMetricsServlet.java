package com.github.mdc.tasks.executor.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.github.mdc.common.MDCConstants;

/**
 * 
 * @author Arun Resource Metrics servlet to display all the available chunks.
 */
public class ResourcesMetricsServlet extends HttpServlet {

	private static final long serialVersionUID = 8713220540678338208L;
	private static Logger log = Logger.getLogger(ResourcesMetricsServlet.class);

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		response.setStatus(HttpServletResponse.SC_OK);
		response.setContentType("application/json");
		try (PrintWriter writer = response.getWriter();) {
			MemoryMXBean membean = ManagementFactory.getMemoryMXBean() ;
			double systemloadaverage = getProcessCpuLoad();
			MemoryUsage heap = membean.getHeapMemoryUsage();
			double heapusage = heap.getUsed() / (double) heap.getMax() * 100.0;
			MemoryUsage nonheap = membean.getNonHeapMemoryUsage();
			double nonheapusage = nonheap.getUsed() / (double) nonheap.getMax() * 100.0;
			String[] cpuheap  = {systemloadaverage + MDCConstants.EMPTY, heapusage + MDCConstants.EMPTY, nonheapusage + MDCConstants.EMPTY};

			writer.write(new ObjectMapper().writeValueAsString(cpuheap));
			writer.flush();
		} catch (Exception ex) {
			log.debug("TaskScheduler Web servlet error, See cause below \n", ex);
		}
	}

	public static double getProcessCpuLoad() throws Exception {

		MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
		ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
		AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});
		if (list.isEmpty()) {
			return Double.NaN;
		}
		Attribute att = (Attribute) list.get(0);
		Double value  = (Double) att.getValue();
		if (value == -1.0) {
			return Double.NaN;
		}
		return (int) (value * 1000) / 10.0;
	}
}
