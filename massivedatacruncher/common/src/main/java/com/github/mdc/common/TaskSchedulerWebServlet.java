/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.mdc.common;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.util.CollectionUtils;

import static java.util.Objects.nonNull;

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
					<body>""", contextpath, contextpath, contextpath, contextpath));

			if (!Objects.isNull(lists) && lists.keySet().size() > 0) {
				builder.append(
						"""
						<table style=\"color:#000000;border-collapse:collapse;width:800px;height:30px\" align=\"center\" border=\"1.0\">
				<thead><th>Node</th><th>FreeMemory</th><th>TotalProcessors</th><th>Physicalmemorysize</th><th>Totaldisksize</th><th>Totalmemory</th><th>Usabledisksize</th></thead>
				<tbody>""");
				int i = 0;
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

			if (MDCJobMetrics.get().keySet().size() > 0) {
				int i = 0;
				builder.append("<br/>");
				builder.append(
						"""
						<table style=\"color:#000000;border-collapse:collapse;width:800px;height:30px\" align=\"center\" border=\"1.0\">
				<thead>
				<th>Job<Br/>Id</th>
				<th>Job<Br/>Name</th>
				<th>Files<Br/>Used</th>
				<th>Job<Br/>Mode</th>
				<th>Total<Br/>Files<Br/>Size (MB)</th>
				<th>Total<Br/>Files<Br/>Blocks</th>
				<th>Container<Br/>Resources</th>
				<th>Job<Br/>Status</th>
				<th>Nodes</th>
				<th>Job<Br/>Start<Br/>Time</th>
				<th>Job<Br/>Completion<Br/>Time</th>
				<th>Total<Br/>Time<Br/>Taken (Sec)</th>
				<th>Summary</th>
				</thead>
				<tbody>""");
				var jms = MDCJobMetrics.get();
				var jobmetrics = jms.keySet().stream().map(key -> jms.get(key)).sorted((jm1, jm2) -> {
					return (int) (jm2.jobstarttime - jm1.jobstarttime);
				}).collect(Collectors.toList());
				for (var jm : jobmetrics) {
					builder.append("<tr bgcolor=\"" + getColor(i++) + "\">");
					builder.append("<td>");
					builder.append(jm.getJobid());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(Objects.isNull(jm.getJobname()) ? MDCConstants.EMPTY : jm.getJobname());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(toHtml(jm.getFiles()));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.getMode());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.getTotalfilesize());
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.totalblocks);
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.containerresources);
					builder.append("</td>");
					builder.append("<td>");
					builder.append(toHtml(jm.containersallocated));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(!Objects.isNull(jm.nodes) ? toHtml(new ArrayList<>(jm.nodes)) : "");
					builder.append("</td>");
					builder.append("<td>");
					builder.append(new Date(jm.jobstarttime));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.jobcompletiontime == 0 ? "" : new Date(jm.jobcompletiontime));
					builder.append("</td>");
					builder.append("<td>");
					builder.append(jm.totaltimetaken == 0 ? "" : jm.totaltimetaken);
					builder.append("</td>");
					builder.append("<td>");
					builder.append(summary(jm));
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

	private String summary(JobMetrics jm){
		SimpleDateFormat formatstartenddate = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
		StringBuilder tasksummary = new StringBuilder();
		tasksummary.append("<p>");
		if(!CollectionUtils.isEmpty(jm.taskexcutortasks)){
			jm.taskexcutortasks.entrySet().stream().forEachOrdered(entry->{
				tasksummary.append(entry.getKey());
				tasksummary.append(":<BR/>");
				double totaltimetakenexecutor = 0d;
				for(Task task : entry.getValue()){
					tasksummary.append(task.taskid);
					tasksummary.append("<BR/>");
					tasksummary.append(formatstartenddate.format(new Date(task.taskexecutionstartime)));
					tasksummary.append("<BR/>");
					tasksummary.append(formatstartenddate.format(new Date(task.taskexecutionendtime)));
					tasksummary.append("<BR/>");
					tasksummary.append(task.timetakenseconds);
					tasksummary.append("<BR/>");
					tasksummary.append("<BR/>");
					totaltimetakenexecutor += task.timetakenseconds;
				}
				tasksummary.append(totaltimetakenexecutor/entry.getValue().size());
				tasksummary.append("<BR/>");
				tasksummary.append("<BR/>");
			});
		}
		tasksummary.append("</p>");
		return tasksummary.toString();
	}

	/**
	 * Color for primary and alternate
	 * @param i
	 * @return
	 */
	private String getColor(int i) {
		{
			if (i % 2 == 0) {
				return MDCProperties.get().getProperty(MDCConstants.COLOR_PICKER_PRIMARY, MDCConstants.COLOR_PICKER_PRIMARY_DEFAULT);
			} else {
				return MDCProperties.get().getProperty(MDCConstants.COLOR_PICKER_ALTERNATE, MDCConstants.COLOR_PICKER_ALTERNATE_DEFAULT);
			}
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private String toHtml(Object data) {
		StringBuilder builder = new StringBuilder();
		builder.append("<p>");
		if (!Objects.isNull(data)) {
			if (data instanceof List files) {
				for (String file : (List<String>) files) {
					builder.append(file);
					builder.append("<br/>");
				}
			} else if (data instanceof Map map) {
				map.keySet().stream().forEach(key -> {
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
