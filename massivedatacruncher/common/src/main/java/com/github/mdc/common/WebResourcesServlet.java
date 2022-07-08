package com.github.mdc.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

/**
 * 
 * @author Arun Data master servlet to display all the available chunks.
 */
public class WebResourcesServlet extends HttpServlet {

	private static final long serialVersionUID = 8713220540678338208L;
	private static Logger log = Logger.getLogger(WebResourcesServlet.class);

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		response.setStatus(HttpServletResponse.SC_OK);
		String filename = FilenameUtils.getName(request.getRequestURL().toString());
		if (filename.endsWith(MDCConstants.JAVASCRIPT)) {
			response.setContentType(MDCConstants.TEXTJAVASCRIPT);
		} else if (filename.endsWith(MDCConstants.CSS)) {
			response.setContentType(MDCConstants.TEXTCSS);
		}
		File file = new File(MDCConstants.PREV_FOLDER + MDCConstants.BACKWARD_SLASH + MDCConstants.WEB_FOLDER
				+ MDCConstants.BACKWARD_SLASH + filename);
		try (FileInputStream fis = new FileInputStream(file); ServletOutputStream sos = response.getOutputStream();) {

			byte[] buffer = new byte[4096];
			int numread;
			while ((numread = fis.read(buffer, 0, 4096)) != -1) {
				sos.write(buffer, 0, numread);
				sos.flush();
			}
		} catch (Exception ex) {
			log.debug("TaskScheduler Web servlet error, See cause below \n", ex);
		}
	}
}
