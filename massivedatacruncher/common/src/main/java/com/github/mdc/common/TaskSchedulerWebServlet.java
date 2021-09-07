package com.github.mdc.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

/**
 * 
 * @author Arun Data master servlet to display all the available chunks.
 */
public class TaskSchedulerWebServlet extends HttpServlet {

	private static final long serialVersionUID = 8713220540678338208L;
	private static Logger log = Logger.getLogger(TaskSchedulerWebServlet.class);

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType(MDCConstants.TEXTHTML);
		response.setStatus(HttpServletResponse.SC_OK);
		var writer = response.getWriter();
		String contextpath = request.getScheme() + "://" + request.getServerName() + MDCConstants.COLON
				+ request.getLocalPort();
		try {
			var lists = MDCNodesResources.get();
			StringBuilder builder = new StringBuilder();
			builder.append(String.format(
					"""
					<html>
					<head>
					<link rel="stylesheet" href="%s/resources/jquery-ui.css">
					<script src="%s/resources/jquery-1.11.1.min.js"></script>
					<script src="%s/resources/jquery.canvasjs.min.js"></script>
					<script src="%s/resources/jquery-ui.js"></script>
					</head>
					<body>""",contextpath,contextpath,contextpath,contextpath));
			
			if (!Objects.isNull(lists)&&lists.keySet().size()>0) {
				builder.append(
					"""
						<table style=\"color:#000000;border-collapse:collapse;width:800px;height:30px\" align=\"center\" border=\"1.0\">
				<thead><th>Node</th><th>FreeMemory</th><th>TotalProcessors</th><th>Physicalmemorysize</th><th>Totaldisksize</th><th>Totalmemory</th><th>Usabledisksize</th></thead>
				<tbody>""");
				int i=0;
				for (var node : lists.keySet()) {
					Resources resources = lists.get(node);
					String[] nodeport = node.split(MDCConstants.UNDERSCORE);
					builder.append("<tr bgcolor=\"" + getColor(i++) + "\">");
					builder.append("<td>");
					builder.append(resources.getNodeport());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getFreememory());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getNumberofprocessors());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getPhysicalmemorysize());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getTotaldisksize());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getTotalmemory());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(resources.getUsabledisksize());
					builder.append("</td>");
					builder.append("<td>");
					builder.append("<a href=\"http://" + nodeport[0] + MDCConstants.COLON
							+ (Integer.parseInt(nodeport[1]) + 50) + "\">");
					builder.append(node);
					builder.append("</a>");
					builder.append("</td>");
					builder.append("</tr>");
				}
				builder.append("</tbody></table>");
			}
			
			if(MDCJobMetrics.get().keySet().size()>0) {
				int i=0;
				builder.append("<br/>");
				builder.append(
						"""
						<table style=\"color:#000000;border-collapse:collapse;width:800px;height:30px\" align=\"center\" border=\"1.0\">
				<thead>
				<th>Job Id</th>
				<th>Files Used</th>
				<th>Job Mode</th>
				<th>Total Files Size (MB)</th>
				<th>Total Files Blocks</th>
				<th>Stages</th>
				<th>Containers Allocated</th>
				<th>Nodes</th>
				<th>Job Start Time</th>
				<th>Job Completion Time</th>
				<th>Total Time Taken (Sec)</th>
				</thead>
				<tbody>""");
				var jms = MDCJobMetrics.get();
				for (var jid : jms.keySet()) {
					var jm = jms.get(jid);
					builder.append("<tr bgcolor=\"" + getColor(i++) + "\">");
					builder.append("<td>");
					builder.append(jm.jobid);
					builder.append("</td>");
					builder.append("<td>");
					builder.append(toHtml(jm.files));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.mode);
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.totalfilesize);
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.totalblocks);
					builder.append("</td>");
					builder.append("<td>");
					builder.append(toHtml(jm.stages));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(toHtml(jm.containersallocated));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(!Objects.isNull(jm.nodes)?toHtml(new ArrayList<>(jm.nodes)):"");
					builder.append("</td>");
					builder.append("<td>");
					builder.append(new Date(jm.jobstarttime));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.jobcompletiontime==0?"":new Date(jm.jobcompletiontime));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.totaltimetaken==0?"":jm.totaltimetaken);
					builder.append("</td>");
					builder.append("</tr>");
				}
				builder.append("</tbody></table>");
				
			}
			builder.append("</body></html>");
			writer.write(builder.toString());
		} catch (Exception ex) {
			log.debug("TaskScheduler Web servlet error, See cause below \n", ex);
		}
	}

	public String getColor(int i) {
		{
			if (i % 2 == 0) {
				return "#45aaaa";
			} else {
				return "#ddddddd";
			}
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public String toHtml(Object data) {
		StringBuilder builder = new StringBuilder();
		builder.append("<p>");
		if (!Objects.isNull(data)) {
			if (data instanceof List files) {
				for (String file : (List<String>) files) {
					builder.append(file);
					builder.append("<br/>");
				}
			}else if(data instanceof Map map) {
				map.keySet().stream().forEach(key->{
					builder.append(key);
					builder.append(MDCConstants.COLON);
					builder.append("Percentage Completed (");
					builder.append(map.get(key));
					builder.append("%)");
					builder.append("<br/>");
				});
			}
		}
		builder.append("</p>");
		return builder.toString();
	}
	
}