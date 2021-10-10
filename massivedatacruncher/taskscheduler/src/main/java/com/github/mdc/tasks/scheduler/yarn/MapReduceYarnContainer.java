package com.github.mdc.tasks.scheduler.yarn;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.jooq.lambda.tuple.Tuple2;
import org.springframework.yarn.integration.container.AbstractIntegrationYarnContainer;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;

import com.esotericsoftware.kryo.io.Input;
import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Utils;
import com.github.mdc.tasks.executor.Combiner;
import com.github.mdc.tasks.executor.Mapper;
import com.github.mdc.tasks.executor.Reducer;
import com.github.mdc.tasks.executor.MapperCombinerExecutor;
import com.github.mdc.tasks.executor.ReducerExecutor;

/**
 * 
 * @author Arun The yarn container executor for to process Map Reduce pipelining
 *         API.
 */
public class MapReduceYarnContainer extends AbstractIntegrationYarnContainer {

	private Map<String, String> containerprops;

	private static final Log log = LogFactory.getLog(MapReduceYarnContainer.class);

	
	
	
	/**
	 * Pull the Job to perform MR operation execution requesting the Yarn App Master
	 * Service. The various Yarn operation What operation to execute i.e
	 * WHATTODO,JOBDONE,JOBFAILED. The various operations response from Yarn App
	 * master are STANDBY,RUNJOB or DIE.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void runInternal() {
		log.info("Container Started...");
		JobRequest request;
		byte[] job = null;
		var containerid = getEnvironment().get(MDCConstants.SHDP_CONTAINERID);
		MindAppmasterServiceClient client = null;
		try {
			ByteBufferPoolDirect.init();
			ByteBufferPool.init(3);
			while (true) {
				request = new JobRequest();
				request.setState(JobRequest.State.WHATTODO);
				request.setContainerid(containerid);
				request.setTimerequested(System.currentTimeMillis());
				client = (MindAppmasterServiceClient) getIntegrationServiceClient();
				var response = (JobResponse) client.doMindRequest(request);
				log.info(containerid + ": Response containerid: " + response);
				if (response == null) {
					sleep(1);
					continue;
				}
				log.info(containerid + ": Response State: " + response.getState() + " " + response.getResmsg());
				if (response.getState().equals(JobResponse.State.STANDBY)) {
					sleep(1);
					continue;
				} else if (response.getState().equals(JobResponse.State.RUNJOB)) {
					log.info(containerid + ": Environment " + getEnvironment());
					job = response.getJob();
					var kryo = Utils.getKryoNonDeflateSerializer();
					var input = new Input(new ByteArrayInputStream(job));
					var object = kryo.readClassAndObject(input);
					if(object instanceof MapperCombiner mc) {
						System.setProperty(MDCConstants.TASKEXECUTOR_HDFSNN,
								containerprops.get(MDCConstants.TASKEXECUTOR_HDFSNN));
						var cm = new ArrayList<Mapper>();
						var cc = new ArrayList<Combiner>();
						var prop = new Properties();
						prop.putAll(containerprops);
						MDCProperties.put(prop);
						try (var hdfs = FileSystem.newInstance(
								new URI(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HDFSNN)),
								new Configuration());) {
							Class<?> clz = null;
							if (mc.mapperclasses != null) {
								for (var mapperclass : mc.mapperclasses) {
									clz = getClass().getClassLoader().loadClass(mapperclass);
									cm.add((Mapper) clz.newInstance());
								}
							}
							if (mc.combinerclasses != null) {
								for (var combinerclass : mc.combinerclasses) {
									clz = getClass().getClassLoader().loadClass(combinerclass);
									cc.add((Combiner) clz.newInstance());
								}
							}
	
							var es = Executors.newWorkStealingPool();
							var mdcmc = new MapperCombinerExecutor(
									mc.blockslocation, CacheUtils.getBlockData(mc.blockslocation, hdfs), cm, cc);
							var fc = (Future<Context>) es.submit(mdcmc);
							var ctx = fc.get();
							es.shutdown();
							RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, mc.apptask.applicationid,
									((mc.apptask.applicationid + mc.apptask.taskid) + MDCConstants.DATAFILEEXTN));
							ctx = null;
							request = new JobRequest();
							request.setState(JobRequest.State.JOBDONE);
							request.setJob(job);
							request.setContainerid(containerid);
							response = (JobResponse) client.doMindRequest(request);
							log.info(containerid + ": Task Completed=" + mc);
							sleep(1);
						}
					}else if(object instanceof YarnReducer red) {
						Class<?> clz = null;
						clz = getClass().getClassLoader().loadClass(red.reducerclasses.iterator().next());
						var cr = (Reducer) clz.newInstance();
						var complete = new DataCruncherContext();
						var apptaskcontextmap = new ConcurrentHashMap<String,Context>();
						Context currentctx;
						var es = Executors.newWorkStealingPool();
						for (var tuple2 : (List<Tuple2>)red.tuples) {
							var ctx = new DataCruncherContext();
							for (var apptaskids : (Collection<String>) tuple2.v2) {
								if(apptaskcontextmap.get(apptaskids)!=null) {
									currentctx = apptaskcontextmap.get(apptaskids);
								}
								else {
									currentctx = (Context) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(red.apptask.applicationid,
											(apptaskids + MDCConstants.DATAFILEEXTN), false);
									apptaskcontextmap.put(apptaskids, currentctx);
								}
								ctx.addAll(tuple2.v1, currentctx.get(tuple2.v1));
							}
							log.info("In Reducer ctx: "+ctx);
							var mdcr = new ReducerExecutor((DataCruncherContext) ctx, cr,
									tuple2.v1);
							var fc = (Future<Context>) es.submit(mdcr);
							Context results = fc.get();
							complete.add(results);
							log.info("Complete Result: "+complete);
						}
						RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(complete, red.apptask.applicationid,
								((red.apptask.applicationid + red.apptask.taskid) + MDCConstants.DATAFILEEXTN));
						es.shutdown();
						request = new JobRequest();
						request.setState(JobRequest.State.JOBDONE);
						request.setJob(job);
						request.setContainerid(containerid);
						response = (JobResponse) client.doMindRequest(request);
						log.info(containerid + ": Task Completed=" + red);
						sleep(1);
					}
				} else if (response.getState().equals(JobResponse.State.DIE)) {
					log.info(containerid + ": Container dies: " + response.getState());
					break;
				}
				log.info(containerid + ": Response state=" + response.getState());

			}
			log.info(containerid + ": Completed Job Exiting with status 0...");
			ByteBufferPoolDirect.get().close();
			System.exit(0);
		} catch (Exception ex) {
			request = new JobRequest();
			request.setState(JobRequest.State.JOBFAILED);
			request.setJob(job);
			if (client != null) {
				JobResponse response = (JobResponse) client.doMindRequest(request);
				log.info("Job Completion Error..." + response.getState() + "..., See cause below \n", ex);
			}
			ByteBufferPoolDirect.get().close();
			System.exit(-1);
		}
	}

	public Map<String, String> getContainerprops() {
		return containerprops;
	}

	public void setContainerprops(Map<String, String> containerprops) {
		this.containerprops = containerprops;
	}

	private static void sleep(int seconds) {
		try {
			Thread.sleep(1000l * seconds);
		} catch (Exception ex) {
			log.info("Delay error, See cause below \n", ex);
		}
	}

}
