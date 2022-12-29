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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.LinkedHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

/**
 * 
 * @author Arun
 * Utility to get the intermediate 
 * and the final stage output from the HDFS and local file system
 */
public class RemoteDataFetcher {
	private RemoteDataFetcher() {
	}
	private static Logger log = Logger.getLogger(RemoteDataFetcher.class);

	/**
	 * Write the intermediate and final stage output to HDFS.
	 * @param serobj
	 * @param jobid
	 * @param filename
	 * @throws Throwable
	 */
	@SuppressWarnings("rawtypes")
	public static void writerIntermediatePhaseOutputToDFS(Context serobj,
			String jobid, String filename) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.writerIntermediatePhaseOutputToDFS");
		var configuration = new Configuration();
		configuration.set(MDCConstants.HDFS_DEFAULTFS, MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT));
		configuration.set(MDCConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		configuration.set(MDCConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
		var jobpath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT) + MDCConstants.FORWARD_SLASH
				+ FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + jobid;
		var filepath = jobpath + MDCConstants.FORWARD_SLASH + filename;
		var jobpathurl = new Path(jobpath);
		// Create folders if not already created.
		var filepathurl = new Path(filepath);
		try (var hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT)),
				configuration);) {
			if (!hdfs.exists(jobpathurl)) {
				hdfs.mkdirs(jobpathurl);
			}
			if (hdfs.exists(filepathurl)) {
				hdfs.delete(filepathurl, false);
			}
			createFileMR(hdfs, filepathurl, serobj);

		} catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ioe);
		}
		log.debug("Exiting RemoteDataFetcher.writerIntermediatePhaseOutputToDFS");
	}

	/**
	 * This method creates file given the path in HDFS for the MRJob API
	 * @param hdfs
	 * @param filepathurl
	 * @param serobj
	 * @throws RemoteDataFetcherException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	protected static void createFileMR(FileSystem hdfs, Path filepathurl, Context serobj) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.createFileMR");

		try (var fsdos = hdfs.create(filepathurl); 
				var output = new FSTObjectOutput(fsdos, Utils.getConfigForSerialization());) {

			output.writeObject(new LinkedHashSet<>(serobj.keys()));
			output.writeObject(serobj);
			output.flush();
		} catch (Exception ex) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex);
		}
		log.debug("Exiting RemoteDataFetcher.createFileMR");
	}


	/**
	 * This method creates file given the path in HDFS for the pipeline API
	 * @param hdfs
	 * @param filepathurl
	 * @param serobj
	 * @throws RemoteDataFetcherException
	 */
	protected static void createFile(FileSystem hdfs, Path filepathurl, Object serobj) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.createFile");
		try (var fsdos = hdfs.create(filepathurl); 
				var output = new FSTObjectOutput(fsdos, Utils.getConfigForSerialization());) {
			output.writeObject(serobj);
			output.flush();
		} catch (Exception ex) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex);
		}
		log.debug("Exiting RemoteDataFetcher.createFile");
	}

	/**
	 * Write intermediate data like graph, jobstage maps and topological order
	 * information 
	 * in Task scheduler to HDFS
	 * @param serobj
	 * @param dirtowrite
	 * @param filename
	 * @throws Throwable
	 */
	public static void writerYarnAppmasterServiceDataToDFS(Object serobj,
			String dirtowrite, String filename) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS");
		var configuration = new Configuration();
		configuration.set(MDCConstants.HDFS_DEFAULTFS, MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT));
		configuration.set(MDCConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		configuration.set(MDCConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());

		var jobpath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT) + MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + dirtowrite;
		var filepath = jobpath + MDCConstants.FORWARD_SLASH + filename;
		var jobpathurl = new Path(jobpath);
		var filepathurl = new Path(filepath);
		try (var hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT)), configuration);) {
			if (!hdfs.exists(jobpathurl)) {
				hdfs.mkdirs(jobpathurl);
			}
			createFile(hdfs, filepathurl, serobj);

		} catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ioe);
		}
		log.debug("Exiting RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS");
	}

	/**
	 * Reads intermediate data like graph, jobstage maps and topological order
	 * information 
	 * in Yarn App master from HDFS
	 * @param dirtoread
	 * @param filename
	 * @return
	 * @throws Throwable
	 */
	public static Object readYarnAppmasterServiceDataFromDFS(String namenodeurl,
			String dirtoread, String filename) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS");
		var configuration = new Configuration();
		var path = namenodeurl + MDCConstants.FORWARD_SLASH
				+ FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + dirtoread + MDCConstants.FORWARD_SLASH
				+ filename;
		try (var hdfs = FileSystem.newInstance(new URI(System.getProperty(MDCConstants.HDFSNAMENODEURL)),
				configuration);
				var fs = (DistributedFileSystem) hdfs;
				var dis = fs.getClient().open(new Path(path).toUri().getPath());
				var in = new FSTObjectInput(dis, Utils.getConfigForSerialization())) {
			log.debug("Exiting RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS");
			return in.readObject();
		} catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
		}

	}

	/**
	 * Read intermediate and final stage output from HDFS.
	 * @param jobid
	 * @param filename
	 * @return
	 * @throws Throwable
	 */
	public static Object readIntermediatePhaseOutputFromDFS(
			String jobid, String filename, boolean keys) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
		var configuration = new Configuration();
		var path = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS
				+ MDCConstants.FORWARD_SLASH + jobid + MDCConstants.FORWARD_SLASH + filename;
		try (var hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT)),
				configuration);
				var dis = hdfs.open(new Path(path));
				var in = new FSTObjectInput(dis, Utils.getConfigForSerialization())) {
			var keysobj = in.readObject();
			if (keys) {
				return keysobj;
			}
			log.debug("Exiting RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
			return in.readObject();
		} catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
		}

	}

	/**
	 * Gets the HDFS inputstream of a file using jobid and filename 
	 * @param jobid
	 * @param filename
	 * @param hdfs
	 * @return
	 * @throws Throwable
	 */
	public static InputStream readIntermediatePhaseOutputFromDFS(
			String jobid, String filename, FileSystem hdfs) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
		try {
			var path = MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + jobid + MDCConstants.FORWARD_SLASH + filename;
			log.debug("Exiting RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
			return hdfs.open(new Path(path));
		}
		catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
		}
	}


	/**
	 * Gets the local file using jobid and filename 
	 * @param jobid
	 * @param filename
	 * @param hdfs
	 * @return
	 * @throws Throwable
	 */
	public static InputStream readIntermediatePhaseOutputFromFS(
			String jobid, String filename) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
		try {
			var path = MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + jobid + MDCConstants.FORWARD_SLASH + filename;			
			File file = new File(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path);
			log.debug("Exiting RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
			if (file.isFile() && file.exists()) {
				return new BufferedInputStream(new FileInputStream(file));
			}
			return null;
		}
		catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
		}
	}


	/**
	 * Delete all the stages outputs of a job
	 * @param jobid
	 * @throws Throwable
	 */
	public static void deleteIntermediatePhaseOutputFromDFS(
			String jobid) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.deleteIntermediatePhaseOutputFromDFS");
		var configuration = new Configuration();
		try (var hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT)), configuration)) {
			hdfs.delete(new Path(MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + jobid), true);
		}
		catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEDELETEERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEDELETEERROR, ioe);
		}
		log.debug("Exiting RemoteDataFetcher.deleteIntermediatePhaseOutputFromDFS");
	}

	/**
	 * This method returns the intermediate data remote by passing the
	 * RemoteDataFetch object.
	 * @param rdf
	 * @throws Exception
	 */
	public static void remoteInMemoryDataFetch(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered RemoteDataFetcher.remoteInMemoryDataFetch");
		log.info("Remote data recover with hp: " + rdf.getHp());
		var rdfwithdata = (RemoteDataFetch) Utils.getResultObjectByInput(rdf.getHp(), rdf);
		rdf.setData(rdfwithdata.getData());
		log.debug("Exiting RemoteDataFetcher.remoteInMemoryDataFetch");
	}


}
