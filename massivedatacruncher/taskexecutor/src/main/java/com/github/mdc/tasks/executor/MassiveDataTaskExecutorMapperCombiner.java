package com.github.mdc.tasks.executor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.ApplicationTask.TaskType;

public class MassiveDataTaskExecutorMapperCombiner implements Runnable {
	static Logger log = Logger.getLogger(MassiveDataTaskExecutorMapperCombiner.class);
	BlocksLocation blockslocation;
	@SuppressWarnings("rawtypes")
	List<CrunchMapper> cm = new ArrayList<>();
	@SuppressWarnings("rawtypes")
	List<CrunchCombiner> cc = new ArrayList<>();
	@SuppressWarnings("rawtypes")
	Context ctx;
	File file;
	HeartBeatTaskScheduler hbts;
	String applicationid;
	String taskid;
	ExecutorService taskpool;
	SnappyInputStream datastream;
	int port;
	@SuppressWarnings({ "rawtypes" })
	public MassiveDataTaskExecutorMapperCombiner(BlocksLocation blockslocation,SnappyInputStream datastream,String applicationid, String taskid,
			ExecutorService taskpool,
			ClassLoader cl,int port,
			HeartBeatTaskScheduler hbts) throws Exception {
		this.blockslocation = blockslocation;
		this.datastream = datastream;
		this.port = port;
		Class<?> clz = null;
		try {
			if(blockslocation.mapperclasses!=null) {
				for(var mapperclass:blockslocation.mapperclasses) {
					clz = cl.loadClass(mapperclass);
					cm.add((CrunchMapper) clz.newInstance());
				}
			}
			if(blockslocation.combinerclasses!=null) {
				for(var combinerclass:blockslocation.combinerclasses) {
					clz = cl.loadClass(combinerclass);
					cc.add((CrunchCombiner) clz.newInstance());
				}
			}
		}
		catch(Throwable ex) {
			log.debug("Exception in loading class:",ex);
		}
		finally {
			
		}
		this.applicationid = applicationid;
		this.taskid = taskid;
		this.taskpool = taskpool;
		this.hbts = hbts;
	}

	@Override
	public void run() {
		

		try {
			hbts.pingOnce(taskid, TaskStatus.SUBMITTED, TaskType.MAPPERCOMBINER, null);
			var es = Executors.newWorkStealingPool();
			var mdcmc = new MassiveDataCruncherMapperCombiner(blockslocation, datastream, cm, cc);
			hbts.pingOnce(taskid, TaskStatus.RUNNING, TaskType.MAPPERCOMBINER, null);
			var fc = es.submit(mdcmc);
			ctx = fc.get();
			RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, applicationid, ((applicationid+taskid)+ MDCConstants.DATAFILEEXTN));
			ctx = null;
			hbts.pingOnce(taskid, TaskStatus.COMPLETED, TaskType.MAPPERCOMBINER, null);
		} catch (Throwable ex) {
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				hbts.pingOnce(taskid, TaskStatus.FAILED, TaskType.MAPPERCOMBINER, new String(baos.toByteArray()));
			} catch (Exception e) {
				log.info("Exception in Sending message to Failed Task: "+blockslocation,ex);
			}
			log.info("Exception in Executing Task: "+blockslocation,ex);
		} finally {
			if(taskpool!=null) {
				taskpool.shutdown();
			}
		}
	}

	public HeartBeatTaskScheduler getHbts() {
		return hbts;
	}
	
}
