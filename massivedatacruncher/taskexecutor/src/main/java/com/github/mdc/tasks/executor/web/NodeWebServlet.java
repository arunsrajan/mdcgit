package com.github.mdc.tasks.executor.web;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;

/**
 * 
 * @author Arun Data master servlet to display all the available chunks.
 */
public class NodeWebServlet extends HttpServlet {
	Map<String, Map<String, Process>> containersipport;

	public NodeWebServlet(Map<String, Map<String, Process>> containersipport) {
		this.containersipport = containersipport;
	}

	private static final long serialVersionUID = 8713220540678338208L;
	private static Logger log = Logger.getLogger(NodeWebServlet.class);

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType(MDCConstants.TEXTHTML);
		response.setStatus(HttpServletResponse.SC_OK);
		var writer = response.getWriter();
		String contextpath = request.getScheme() + "://" + request.getServerName() + MDCConstants.COLON
				+ request.getLocalPort();
		try {
			writer.println(String.format("""
					<!DOCTYPE HTML>
					<html>
					<head>
					<link rel="stylesheet" href="%s/resources/jquery-ui.css">
					<script src="%s/resources/jquery-1.11.1.min.js"></script>
					<script src="%s/resources/jquery.canvasjs.min.js"></script>
					<script src="%s/resources/jquery-ui.js"></script>
					<script>
					$(function() {

					var dataPointsCpu = [];
					var dataPointsMemory = [];


					var optionsCpu = {
						theme: "light2",
						title: {
							text: "Cpu Usage"
						},
						data: [{
							type: "line",
							dataPoints: dataPointsCpu
						}]
					};
					$("#chartContainerCpu").CanvasJSChart(optionsCpu);


					var optionsMemory = {
						theme: "light2",
						title: {
							text: "Memory Usage"
						},
						data: [{
							type: "line",
							dataPoints: dataPointsMemory
						}]
					};
					$("#chartContainerMemory").CanvasJSChart(optionsMemory);


					updateData();

					// Initial Values
					var xValue = 0;
					var yValue = 100;
					var newDataCount = 6;

					function addData(data) {
						{
							//dataPoints.shift();
							dataPointsCpu.push({x: xValue, y: parseInt(data[0])});
							dataPointsMemory.push({x: xValue, y: parseInt(data[1])});
							xValue++;
						}

						newDataCount = 1;

						$("#chartContainerCpu").CanvasJSChart().render()
						$("#chartContainerMemory").CanvasJSChart().render()
						setTimeout(updateData, 1500);
					}

					function updateData() {
						$.getJSON("%s/data", addData);
					}


					$( "#tabs" ).tabs();
					});
					</script>
					</head>
					<body>
					<H1>%s</H1>
					<div>
					<p>--------------------------------------------------------------------------</p>
					</div>
					<div id="tabs">
					  <ul>
					    <li><a href="#tabs-1">Cpu Usage</a></li>
					    <li><a href="#tabs-2">Memory Usage</a></li>
					  </ul>
					  <div id="tabs-1">
					    <div id="chartContainerCpu" style="height: 500px; width: 600px;"></div>
					  </div>
					  <div id="tabs-2">
					    <div id="chartContainerMemory" style="height: 500px; width: 600px;"></div>
					  </div>
					</div>""" + getIframe() + """
					</body>
					</html>
										""", contextpath, contextpath, contextpath, contextpath, contextpath, request.getServerName() + MDCConstants.COLON
					+ request.getLocalPort()));
		} catch (Exception ex) {
			log.debug("TaskScheduler Web servlet error, See cause below \n", ex);
		}
	}

	public String getIframe() {
		StringBuilder containersiframe = new StringBuilder();
		containersiframe.append("""
				<H1>Containers</H1>
				<div>
					<p>--------------------------------------------------------------------------</p>
					</div>
				""");
		containersipport.keySet().stream().flatMap(container -> containersipport.get(container).keySet().stream())
				.map(port -> (MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)
						+ MDCConstants.UNDERSCORE + (Integer.parseInt(port) + 50)) + "<BR/><iframe src=\"http://" + MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)
						+ MDCConstants.COLON + (Integer.parseInt(port) + 50)
						+ "\" width=\"900px\" height=\"800px\" style=\"border:1px solid black;\">\r\n" + "</iframe>")
				.forEach(iframe -> {
					containersiframe.append(iframe);
					containersiframe.append("<BR/>");
				});
		return containersiframe.toString();
	}
}
