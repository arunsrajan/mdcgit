package com.github.mdc.stream.executors;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.IntSupplier;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.Blocks;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ByteBufferOutputStream;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.HdfsBlockReader;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.common.functions.CalculateCount;
import com.github.mdc.common.functions.Coalesce;
import com.github.mdc.common.functions.CountByKeyFunction;
import com.github.mdc.common.functions.CountByValueFunction;
import com.github.mdc.common.functions.FoldByKey;
import com.github.mdc.common.functions.GroupByKeyFunction;
import com.github.mdc.common.functions.IntersectionFunction;
import com.github.mdc.common.functions.Join;
import com.github.mdc.common.functions.JoinPredicate;
import com.github.mdc.common.functions.LeftJoin;
import com.github.mdc.common.functions.LeftOuterJoinPredicate;
import com.github.mdc.common.functions.Max;
import com.github.mdc.common.functions.Min;
import com.github.mdc.common.functions.PipelineCoalesceFunction;
import com.github.mdc.common.functions.RightJoin;
import com.github.mdc.common.functions.RightOuterJoinPredicate;
import com.github.mdc.common.functions.StandardDeviation;
import com.github.mdc.common.functions.Sum;
import com.github.mdc.common.functions.SummaryStatistics;
import com.github.mdc.common.functions.UnionFunction;
import com.github.mdc.stream.CsvOptions;
import com.github.mdc.stream.Json;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.PipelineIntStreamCollect;
import com.github.mdc.stream.PipelineUtils;
import com.github.mdc.stream.utils.StreamUtils;
import com.pivovarit.collectors.ParallelCollectors;

/**
 * 
 * @author Arun Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public sealed class StreamPipelineTaskExecutor implements
		Runnable permits StreamPipelineTaskExecutorInMemory, StreamPipelineTaskExecutorJGroups, StreamPipelineTaskExecutorMesos, StreamPipelineTaskExecutorYarn, StreamPipelineTaskExecutorLocal {
	protected JobStage jobstage;
	protected HeartBeatTaskSchedulerStream hbtss;
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutor.class);
	protected FileSystem hdfs = null;
	protected boolean completed = false;
	public long starttime = 0l;
	public long endtime = 0l;
	Cache cache;
	Task task;
	boolean iscacheable = false;
	ExecutorService executor;

	public StreamPipelineTaskExecutor(JobStage jobstage, Cache cache) {
		this.jobstage = jobstage;
		this.cache = cache;
	}

	public boolean isCompleted() {
		return completed;
	}

	public HeartBeatTaskSchedulerStream getHbtss() {
		return hbtss;
	}

	public void setHbtss(HeartBeatTaskSchedulerStream hbtss) {
		this.hbtss = hbtss;
	}

	public FileSystem getHdfs() {
		return hdfs;
	}

	public void setHdfs(FileSystem hdfs) {
		this.hdfs = hdfs;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
		if (task.finalphase && task.saveresulttohdfs) {
			try {
				this.hdfs = FileSystem.newInstance(new URI(task.hdfsurl), new Configuration());
			} catch (IOException | URISyntaxException e) {
				log.error("Exception in creating hdfs client connection: ", e);
			}
		}
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public void setExecutor(ExecutorService executor) {
		this.executor = executor;
	}

	/**
	 * Get the list of all the functions.
	 * 
	 * @return
	 */
	private List getFunctions() {
		log.debug("Entered MassiveDataStreamTaskDExecutor.getFunctions");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		log.debug("Exiting MassiveDataStreamTaskDExecutor.getFunctions");
		return functions;
	}

	protected String getStagesTask() {
		log.debug("Entered MassiveDataStreamTaskDExecutor.getStagesTask");
		var tasks = jobstage.getStage().tasks;
		var builder = new StringBuilder();
		for (var task : tasks) {
			builder.append(PipelineUtils.getFunctions(task));
			builder.append(", ");
		}
		log.debug("Exiting MassiveDataStreamTaskDExecutor.getStagesTask");
		return builder.toString();
	}

	/**
	 * Process the data using intersection function.
	 * 
	 * @param blocksfirst
	 * @param blockssecond
	 * @param hdfs
	 * @throws Exception
	 */
	public double processBlockHDFSIntersection(BlocksLocation blocksfirst, BlocksLocation blockssecond, FileSystem hdfs)
			throws Exception {
		long starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);

				var bais1 = HdfsBlockReader.getBlockDataInputStream(blocksfirst, hdfs);
				var buffer1 = new BufferedReader(new InputStreamReader(bais1));
				var bais2 = HdfsBlockReader.getBlockDataInputStream(blockssecond, hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamfirst = buffer1.lines();
				var streamsecond = buffer2.lines();) {
			var cf = (CompletableFuture) streamsecond.distinct().collect(ParallelCollectors.parallel(value -> value,
					Collectors.toCollection(LinkedHashSet::new), executor, Runtime.getRuntime().availableProcessors()));
			;
			var setsecond = (Set) cf.get();
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					streamfirst.distinct().filter(setsecond::contains).forEach(val -> {
						try {
							os.write(val.toString().getBytes());
							os.write(ch);
						} catch (IOException e) {
						}
					});
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// Get the result of intersection functions parallel.
			cf = (CompletableFuture) streamfirst.distinct().filter(setsecond::contains)
					.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new), executor,
							Runtime.getRuntime().availableProcessors()));
			var kryo = Utils.getKryoSerializerDeserializer();
			Object result = cf.get();
			kryo.writeClassAndObject(output, result);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<Object>(result);
			result = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
		}

	}

	/**
	 * Process the data using intersection function.
	 * 
	 * @param fsstreamfirst
	 * @param blockssecond
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSIntersection(Set<InputStream> fsstreamfirst, List<BlocksLocation> blockssecond,
			FileSystem hdfs) throws Exception {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next()));
				var bais2 = HdfsBlockReader.getBlockDataInputStream(blockssecond.get(0), hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamsecond = buffer2.lines();) {
			var kryo = Utils.getKryoSerializerDeserializer();
			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			var cf = (CompletableFuture) streamsecond.distinct().collect(ParallelCollectors.parallel(value -> value,
					Collectors.toCollection(LinkedHashSet::new), executor, Runtime.getRuntime().availableProcessors()));
			;
			var setsecond = (Set) cf.get();
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					datafirst.stream().distinct().filter(setsecond::contains).forEach(val -> {
						try {
							os.write(val.toString().getBytes());
							os.write(ch);
						} catch (IOException e) {
						}
					});
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// Parallel execution of the intersection function.
			cf = (CompletableFuture) datafirst.stream().distinct().filter(setsecond::contains)
					.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new), executor,
							Runtime.getRuntime().availableProcessors()));
			Object result = cf.get();
			kryo.writeClassAndObject(output, result);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<Object>(result);
			result = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
		}
	}

	/**
	 * Process the data using intersection function.
	 * 
	 * @param fsstreamfirst
	 * @param fsstreamsecond
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSIntersection(List<InputStream> fsstreamfirst, List<InputStream> fsstreamsecond)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next()));
				var inputsecond = new Input(new BufferedInputStream(fsstreamsecond.iterator().next()));

		) {

			var kryo = Utils.getKryoSerializerDeserializer();
			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			var datasecond = (List) kryo.readClassAndObject(inputsecond);
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					datafirst.stream().distinct().filter(datasecond::contains).forEach(val -> {
						try {
							os.write(val.toString().getBytes());
							os.write(ch);
						} catch (IOException e) {
						}
					});
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// parallel execution of intersection function.
			var cf = (CompletableFuture) datafirst.stream().distinct().filter(datasecond::contains)
					.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new), executor,
							Runtime.getRuntime().availableProcessors()));
			Object result = cf.get();
			kryo.writeClassAndObject(output, result);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<Object>(result);
			result = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
		}
	}

	/**
	 * Get the HDFS file path using the job and stage id.
	 * 
	 * @return
	 */
	public String getIntermediateDataFSFilePath(Task task) {
		return (MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + jobstage.getJobid()
				+ MDCConstants.FORWARD_SLASH + task.taskid);
	}

	/**
	 * Create a file in HDFS and return the stream.
	 * 
	 * @return
	 * @throws Exception
	 */
	public OutputStream createIntermediateDataToFS(Task task, int buffersize) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskDExecutor.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			new File(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + MDCConstants.FORWARD_SLASH
					+ FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + jobstage.getJobid()).mkdirs();
			log.debug("Exiting MassiveDataStreamTaskDExecutor.createIntermediateDataToFS");
			return new FileOutputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path);
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		}
	}

	/**
	 * Open the already existing file using the job and stageid.
	 * 
	 * @return
	 * @throws Exception
	 */
	public InputStream getIntermediateInputStreamFS(Task task) throws Exception {
		var path = getIntermediateDataFSFilePath(task);
		return new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path);
	}

	/**
	 * Perform the union operation in parallel.
	 * 
	 * @param blocksfirst
	 * @param blockssecond
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSUnion(BlocksLocation blocksfirst, BlocksLocation blockssecond, FileSystem hdfs)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var bais1 = HdfsBlockReader.getBlockDataInputStream(blocksfirst, hdfs);
				var buffer1 = new BufferedReader(new InputStreamReader(bais1));
				var bais2 = HdfsBlockReader.getBlockDataInputStream(blockssecond, hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamfirst = buffer1.lines();
				var streamsecond = buffer2.lines();) {
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List result;
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + java.util.stream.Stream.concat(streamfirst, streamsecond).distinct().count())
								.getBytes());
					} else {
						java.util.stream.Stream.concat(streamfirst, streamsecond).distinct().forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			if (terminalCount) {
				result = new Vector<>();
				result.add(java.util.stream.Stream.concat(streamfirst, streamsecond).distinct().count());
			} else {
				// parallel stream union operation result
				var cf = (CompletableFuture) java.util.stream.Stream.concat(streamfirst, streamsecond).distinct()
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				result = (List) cf.get();
			}
			var kryo = Utils.getKryoSerializerDeserializer();
			kryo.writeClassAndObject(output, result);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(result);
			result = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSUNION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSUNION, ex);
		}
	}

	/**
	 * Perform the union operation in parallel.
	 * 
	 * @param fsstreamfirst
	 * @param blockssecond
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSUnion(Set<InputStream> fsstreamfirst, List<BlocksLocation> blockssecond,
			FileSystem hdfs) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next()));
				var bais2 = HdfsBlockReader.getBlockDataInputStream(blockssecond.get(0), hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamsecond = buffer2.lines();) {
			var kryo = Utils.getKryoSerializerDeserializer();

			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write((""
								+ java.util.stream.Stream.concat(datafirst.stream(), streamsecond).distinct().count())
								.getBytes());
					} else {
						java.util.stream.Stream.concat(datafirst.stream(), streamsecond).distinct().forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List result;
			if (terminalCount) {
				result = new Vector<>();
				result.add(java.util.stream.Stream.concat(datafirst.stream(), streamsecond).distinct().count());
			} else {
				// parallel stream union operation result
				var cf = (CompletableFuture) java.util.stream.Stream.concat(datafirst.stream(), streamsecond).distinct()
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				result = (List) cf.get();
			}
			kryo.writeClassAndObject(output, result);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(result);
			result = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSUNION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSUNION, ex);
		}
	}

	/**
	 * Perform the union operation in parallel.
	 * 
	 * @param fsstreamfirst
	 * @param fsstreamsecond
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSUnion(List<InputStream> fsstreamfirst, List<InputStream> fsstreamsecond)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next()));
				var inputsecond = new Input(new BufferedInputStream(fsstreamsecond.iterator().next()));) {

			var kryo = Utils.getKryoSerializerDeserializer();

			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			var datasecond = (List) kryo.readClassAndObject(inputsecond);
			List result;
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + java.util.stream.Stream.concat(datafirst.stream(), datasecond.stream())
								.distinct().count()).getBytes());
					} else {
						java.util.stream.Stream.concat(datafirst.stream(), datasecond.stream()).distinct()
								.forEach(val -> {
									try {
										os.write(val.toString().getBytes());
										os.write(ch);
									} catch (IOException e) {
									}
								});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			if (terminalCount) {
				result = new ArrayList<>();
				result.add(java.util.stream.Stream.concat(datafirst.parallelStream(), datasecond.parallelStream())
						.distinct().count());
			} else {
				// parallel stream union operation result
				var cf = (CompletableFuture) java.util.stream.Stream.concat(datafirst.stream(), datasecond.stream())
						.distinct()
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				result = (List) cf.get();
			}

			kryo.writeClassAndObject(output, result);
			output.flush();
			fsdos.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(result);
			result = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSUNION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSUNION, ex);
		}
	}

	/**
	 * Perform map operation to obtain intermediate stage result.
	 * 
	 * @param blockslocation
	 * @param hdfs
	 * @throws Throwable
	 */
	@SuppressWarnings("unchecked")
	public double processBlockHDFSMap(BlocksLocation blockslocation, FileSystem hdfs) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSMap");
		log.info(blockslocation);
		CSVParser records = null;
		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
				var buffer = new BufferedReader(new InputStreamReader(bais));) {

			var kryo = Utils.getKryoSerializerDeserializer();
			Stream intermediatestreamobject;
			if (jobstage.getStage().tasks.get(0) instanceof Json) {
				intermediatestreamobject = buffer.lines();
				intermediatestreamobject = intermediatestreamobject.map(line -> {
					try {
						return new JSONParser().parse((String) line);
					} catch (ParseException e) {
						return null;
					}
				});
			} else {
				if (jobstage.getStage().tasks.get(0) instanceof CsvOptions csvoptions) {
					var csvformat = CSVFormat.DEFAULT;
					csvformat = csvformat.withDelimiter(',').withHeader(csvoptions.getHeader()).withIgnoreHeaderCase()
							.withTrim();

					try {
						records = csvformat.parse(buffer);
						Stream<CSVRecord> streamcsv = StreamSupport.stream(records.spliterator(), false);
						intermediatestreamobject = streamcsv;
					} catch (IOException ioe) {
						log.error(PipelineConstants.FILEIOERROR, ioe);
						throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
					} catch (Exception ex) {
						log.error(PipelineConstants.PROCESSHDFSERROR, ex);
						throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
					}
				} else {
					intermediatestreamobject = buffer.lines();
				}

			}
			intermediatestreamobject.onClose(() -> {
				log.debug("Stream closed");
			});
			var finaltask = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);

			try (var streammap = (BaseStream) StreamUtils.getFunctionsToStream(getFunctions(),
					intermediatestreamobject);) {
				List out;

				if (finaltask instanceof CalculateCount) {
					out = new Vector<>();
					if (streammap instanceof IntStream stmap) {
						out.add(stmap.count());
					} else {
						out.add(((Stream) streammap).count());
					}
				} else if (finaltask instanceof PipelineIntStreamCollect piplineistream) {
					out = new Vector<>();
					out.add(((IntStream) streammap).collect(piplineistream.getSupplier(),
							piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

				} else if (finaltask instanceof SummaryStatistics) {
					out = new Vector<>();
					out.add(((IntStream) streammap).summaryStatistics());

				} else if (finaltask instanceof Max) {
					out = new Vector<>();
					out.add(((IntStream) streammap).max().getAsInt());

				} else if (finaltask instanceof Min) {
					out = new Vector<>();
					out.add(((IntStream) streammap).min().getAsInt());

				} else if (finaltask instanceof Sum) {
					out = new Vector<>();
					out.add(((IntStream) streammap).sum());

				} else if (finaltask instanceof StandardDeviation) {
					out = new Vector<>();
					CompletableFuture<List> cf = (CompletableFuture) ((java.util.stream.IntStream) streammap).boxed()
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					var streamtmp = cf.get();
					var mean = (streamtmp).stream().mapToInt(Integer.class::cast).average().getAsDouble();
					var variance = (streamtmp).stream().mapToInt(Integer.class::cast)
							.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
					var standardDeviation = Math.sqrt(variance);
					out.add(standardDeviation);

				} else if (task.finalphase && task.saveresulttohdfs) {
					try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
							Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
									MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
						int ch = (int) '\n';
						((Stream) streammap).forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
					var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
					return timetaken;
				} else {
					log.info("Map Collect Start");
					CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap).collect(
							ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					out = cf.get();
					log.info("Map Collect End");
				}
				kryo.writeClassAndObject(output, out);
				output.flush();
				byte[] intermediatedata = fsdos.toByteArray();
				OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
				IOUtils.write(intermediatedata, os);
				if (os instanceof ByteBufferOutputStream bbos) {
					bbos.get().flip();
				}
				cacheAble(fsdos);
				var wr = new WeakReference<List>(out);
				out = null;
				log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSMap");
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				log.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
				log.debug("GC Status Map task:" + Utils.getGCStats());
				return timetaken;
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSHDFSERROR, ex);
				throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
			}
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSERROR, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
		} finally {
			if (!Objects.isNull(records)) {
				try {
					records.close();
				} catch (Exception e) {
					log.error(MDCConstants.EMPTY, e);
				}
			}
		}

	}

	@SuppressWarnings("unchecked")
	public void cacheAble(OutputStream fsdos) {
		if (iscacheable) {
			byte[] bt = ((ByteArrayOutputStream) fsdos).toByteArray();
			cache.put(getIntermediateDataFSFilePath(task), bt);
		}
	}

	/**
	 * Perform map operation to obtain intermediate stage result.
	 * 
	 * @param fsstreamfirst
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processBlockHDFSMap(Set<InputStream> fsstreamfirst) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSMap");
		try (var fsdos = new ByteArrayOutputStream(); var output = new Output(fsdos);) {
			var kryo = Utils.getKryoSerializerDeserializer();
			var functions = getFunctions();

			List out = new ArrayList<>();
			var finaltask = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);
			for (var is : fsstreamfirst) {
				try (var input = new Input(is);) {
					// while (input.available() > 0) {
					var inputdatas = (List) kryo.readClassAndObject(input);
					// Get Streams object from list of map functions.
					try (BaseStream streammap = (BaseStream) StreamUtils.getFunctionsToStream(functions,
							inputdatas.stream());) {
						if (finaltask instanceof CalculateCount) {
							out = new Vector<>();
							if (streammap instanceof IntStream stmap) {
								out.add(stmap.count());
							} else {
								out.add(((Stream) streammap).count());
							}
						} else if (finaltask instanceof PipelineIntStreamCollect piplineistream) {
							out = new Vector<>();
							out.add(((IntStream) streammap).collect(piplineistream.getSupplier(),
									piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

						} else if (finaltask instanceof SummaryStatistics) {
							out = new Vector<>();
							out.add(((IntStream) streammap).summaryStatistics());

						} else if (finaltask instanceof Max) {
							out = new Vector<>();
							out.add(((IntStream) streammap).max().getAsInt());

						} else if (finaltask instanceof Min) {
							out = new Vector<>();
							out.add(((IntStream) streammap).min().getAsInt());

						} else if (finaltask instanceof Sum) {
							out = new Vector<>();
							out.add(((IntStream) streammap).sum());

						} else if (finaltask instanceof StandardDeviation) {
							out = new Vector<>();
							CompletableFuture<List> cf = (CompletableFuture) ((java.util.stream.IntStream) streammap)
									.boxed()
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							var streamtmp = cf.get();
							double mean = (streamtmp).stream().mapToInt(Integer.class::cast).average().getAsDouble();
							double variance = (double) (streamtmp).stream().mapToInt(Integer.class::cast)
									.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
							double standardDeviation = Math.sqrt(variance);
							out.add(standardDeviation);

						} else if (task.finalphase && task.saveresulttohdfs

						) {
							try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
									Short.parseShort(
											MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
													MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
								int ch = (int) '\n';
								((Stream) streammap).forEach(val -> {
									try {
										os.write(val.toString().getBytes());
										os.write(ch);
									} catch (IOException e) {
									}
								});
							}
							var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
							return timetaken;
						} else {
							CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap).collect(
									ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
											executor, Runtime.getRuntime().availableProcessors()));
							out = cf.get();
						}
					} catch (Exception ex) {
						log.error(PipelineConstants.PROCESSHDFSERROR, ex);
						throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
					}
					// }
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSHDFSERROR, ex);
					throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
				}
			}
			kryo.writeClassAndObject(output, out);
			output.flush();
			fsdos.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(out);
			out = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSMap");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
			log.debug("GC Status Map task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSERROR, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
		}
	}

	@SuppressWarnings("unchecked")
	public double processSamplesBlocks(Integer numofsample, BlocksLocation blockslocation, FileSystem hdfs)
			throws Exception {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processSamplesBlocks");
		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
				var buffer = new BufferedReader(new InputStreamReader(bais));
				var stringdata = buffer.lines();) {
			Kryo kryo = Utils.getKryoSerializerDeserializer();
			// Limit the sample using the limit method.
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + stringdata.limit(numofsample).count()).getBytes());
					} else {
						stringdata.limit(numofsample).forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List out;
			if (terminalCount) {
				out = new Vector<>();
				out.add(stringdata.limit(numofsample).count());
			} else {
				CompletableFuture<List> cf = (CompletableFuture) stringdata.limit(numofsample)
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				out = cf.get();
			}
			kryo.writeClassAndObject(output, out);
			output.flush();

			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(out);
			out = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processSamplesBlocks");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Sampling Task is " + timetaken + " seconds");
			log.debug("GC Status Sampling task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSSAMPLE, ex);
			throw new PipelineException(PipelineConstants.PROCESSSAMPLE, ex);
		}
	}

	/**
	 * Obtain data samples using this method.
	 * 
	 * @param numofsample
	 * @param fsstreams
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processSamplesObjects(Integer numofsample, List fsstreams) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processSamplesObjects");
		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(((InputStream) (fsstreams.iterator().next()))));) {
			var kryo = Utils.getKryoSerializerDeserializer();
			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + datafirst.parallelStream().limit(numofsample).count()).getBytes());
					} else {
						datafirst.parallelStream().limit(numofsample).forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List out;
			if (terminalCount) {
				out = new Vector<>();
				out.add(datafirst.parallelStream().limit(numofsample).count());
			} else {
				// Limit the sample using the limit method.
				CompletableFuture<List> cf = (CompletableFuture) datafirst.stream().limit(numofsample)
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				out = cf.get();
			}
			kryo.writeClassAndObject(output, out);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(out);
			out = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processSamplesObjects");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Sampling Task is " + timetaken + " seconds");
			log.debug("GC Status Sampling task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSSAMPLE, ex);
			throw new PipelineException(PipelineConstants.PROCESSSAMPLE, ex);
		}
	}

	public String getIntermediateDataFSFilePath(String jobid, String stageid, String taskid) {
		return (jobid + MDCConstants.HYPHEN + stageid + MDCConstants.HYPHEN + taskid);
	}

	public String getIntermediateDataRDF(String taskid) {
		return taskid;
	}

	@Override
	public void run() {
		starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.call");
		var stageTasks = getStagesTask();
		var stagePartition = jobstage.getStageid();
		var timetakenseconds = 0.0;
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL);
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
			this.hdfs = hdfs;

			log.debug("Submitted Stage: " + stagePartition);
			log.debug("Processing Stage Tasks: " + stageTasks);

			log.debug("Running Stage: " + stagePartition);

			if (task.input != null && task.parentremotedatafetch != null) {
				var numinputs = task.parentremotedatafetch.length;
				for (var inputindex = 0; inputindex < numinputs; inputindex++) {
					var input = task.parentremotedatafetch[inputindex];
					if (input != null) {
						var rdf = input;
						InputStream is = RemoteDataFetcher.readIntermediatePhaseOutputFromFS(rdf.getJobid(),
								rdf.getTaskid());
						if (Objects.isNull(is)) {
							RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
							task.input[inputindex] = 
									new BufferedInputStream(new ByteArrayInputStream(rdf.getData()));
						} else {
							task.input[inputindex] = is;
						}
					}
				}
			}
			endtime = System.currentTimeMillis();
			hbtss.pingOnce(task, Task.TaskStatus.RUNNING, new Long[] { starttime, endtime }, timetakenseconds, null);
			timetakenseconds = computeTasks(task, hdfs);
			log.debug("Completed Stage: " + stagePartition);
			completed = true;
			hbtss.setTimetakenseconds(timetakenseconds);
			endtime = System.currentTimeMillis();
			hbtss.pingOnce(task, Task.TaskStatus.COMPLETED, new Long[] { starttime, endtime }, timetakenseconds, null);
		} catch (Throwable ex) {
			log.error("Failed Stage: " + stagePartition, ex);
			completed = true;
			log.error("Failed Stage: " + task.stageid, ex);
			try (var baos = new ByteArrayOutputStream();) {
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				endtime = System.currentTimeMillis();
				hbtss.pingOnce(task, Task.TaskStatus.FAILED, new Long[] { starttime, endtime }, 0.0,
						new String(baos.toByteArray()));
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ", e);
			}
		}
		log.debug("Exiting MassiveDataStreamTaskDExecutor.call");
	}

	@SuppressWarnings("unchecked")
	public double computeTasks(Task task, FileSystem hdfs) throws Exception {
		var timetakenseconds = 0.0;
		if (jobstage.getStage().tasks.get(0) instanceof JoinPredicate jp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = ((InputStream) streamfirst);
					var streamsecondtocompute = ((InputStream) streamsecond);) {
				timetakenseconds = processJoinLZF(streamfirsttocompute, streamsecondtocompute, jp,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
			}

		} else if (jobstage.getStage().tasks.get(0) instanceof LeftOuterJoinPredicate ljp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = ((InputStream) streamfirst);
					var streamsecondtocompute = ((InputStream) streamsecond);) {
				timetakenseconds = processLeftOuterJoinLZF(streamfirsttocompute, streamsecondtocompute, ljp,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof RightOuterJoinPredicate rjp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = ((InputStream) streamfirst);
					var streamsecondtocompute = ((InputStream) streamsecond);) {
				timetakenseconds = processRightOuterJoinLZF(streamfirsttocompute, streamsecondtocompute, rjp,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof Join jp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = ((InputStream) streamfirst);
					var streamsecondtocompute = ((InputStream) streamsecond);) {
				timetakenseconds = processJoin(streamfirsttocompute, streamsecondtocompute,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
			}

		} else if (jobstage.getStage().tasks.get(0) instanceof LeftJoin ljp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = ((InputStream) streamfirst);
					var streamsecondtocompute = ((InputStream) streamsecond);) {
				timetakenseconds = processLeftJoin(streamfirsttocompute, streamsecondtocompute,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof RightJoin rjp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = ((InputStream) streamfirst);
					var streamsecondtocompute = ((InputStream) streamsecond);) {
				timetakenseconds = processRightJoin(streamfirsttocompute, streamsecondtocompute,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof IntersectionFunction) {

			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				timetakenseconds = processBlockHDFSIntersection(blfirst, blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof Blocks
							|| task.input[1] instanceof BlocksLocation)) {
				var streamfirst = new LinkedHashSet<InputStream>();
				var blockssecond = new ArrayList<BlocksLocation>();
				for (var input : task.input) {
					if (input instanceof InputStream inpstr)
						streamfirst.add(inpstr);
					else {
						if (input instanceof BlocksLocation blockslocation) {
							blockssecond.add(blockslocation);
						}
					}
				}
				timetakenseconds = processBlockHDFSIntersection(streamfirst, blockssecond, hdfs);
			} else if (task.input[0] instanceof InputStream && task.input[1] instanceof InputStream) {
				timetakenseconds = processBlockHDFSIntersection((List) Arrays.asList(task.input[0]),
						(List) Arrays.asList(task.input[1]));
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof UnionFunction) {
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				timetakenseconds = processBlockHDFSUnion(blfirst, blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof Blocks
							|| task.input[1] instanceof BlocksLocation)) {
				var streamfirst = new LinkedHashSet<InputStream>();
				var blockssecond = new ArrayList<BlocksLocation>();
				for (var input : task.input) {
					if (input instanceof InputStream inpstr)
						streamfirst.add(inpstr);
					else {
						if (input instanceof BlocksLocation blockslocation) {
							blockssecond.add(blockslocation);
						}
					}
				}
				timetakenseconds = processBlockHDFSUnion(streamfirst, blockssecond, hdfs);
			} else if (task.input[0] instanceof InputStream && task.input[1] instanceof InputStream) {
				timetakenseconds = processBlockHDFSUnion((List) Arrays.asList(task.input[0]),
						(List) Arrays.asList(task.input[1]));
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof IntSupplier sample) {
			var numofsample = sample.getAsInt();
			if (task.input[0] instanceof BlocksLocation bl) {
				timetakenseconds = processSamplesBlocks(numofsample, (BlocksLocation) bl, hdfs);
			} else {
				timetakenseconds = processSamplesObjects(numofsample, (List) Arrays.asList(task.input));
			}
		} else if (task.input[0] instanceof BlocksLocation bl) {
			timetakenseconds = processBlockHDFSMap(bl, hdfs);
		} else if (jobstage.getStage().tasks.get(0) instanceof GroupByKeyFunction) {
			timetakenseconds = processGroupByKeyTuple2();
		} else if (jobstage.getStage().tasks.get(0) instanceof FoldByKey) {
			timetakenseconds = processFoldByKeyTuple2();
		} else if (jobstage.getStage().tasks.get(0) instanceof CountByKeyFunction) {
			timetakenseconds = processCountByKeyTuple2();
		} else if (jobstage.getStage().tasks.get(0) instanceof CountByValueFunction) {
			timetakenseconds = processCountByValueTuple2();
		} else if (task.input[0] instanceof InputStream) {
			if (jobstage.getStage().tasks.get(0) instanceof Coalesce) {
				timetakenseconds = processCoalesce();
			} else {
				var streams = new LinkedHashSet<InputStream>();
				streams.addAll((List) Arrays.asList(task.input));
				timetakenseconds = processBlockHDFSMap(streams);
			}
		}
		return timetakenseconds;
	}

	/**
	 * Join pair operation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in milliseconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processJoin(InputStream streamfirst, InputStream streamsecond, boolean isinputfirstblocks,
			boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskDExecutor.processJoin");
		var starttime = System.currentTimeMillis();

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoSerializerDeserializer();
			final List<Tuple2> inputs1, inputs2;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}

			Stream<Tuple2> joinpairs = inputs1.parallelStream().flatMap(tup1 -> {
				return inputs2.parallelStream().filter(tup2 -> tup1.v1.equals(tup2.v1))
						.map(tup2 -> new Tuple2(tup2.v1, new Tuple2(tup1.v2, tup2.v2))).collect(Collectors.toList())
						.stream();
			});

			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + joinpairs.count()).getBytes());
					} else {
						joinpairs.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try {
					joinpairsout.add(joinpairs.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				joinpairsout = joinpairs.collect(Collectors.toList());

			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processJoin");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Join task is " + timetaken + " seconds");
			log.debug("GC Status Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
		}
	}

	/**
	 * Left Join pair operation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in milliseconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processLeftJoin(InputStream streamfirst, InputStream streamsecond, boolean isinputfirstblocks,
			boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskDExecutor.processLeftJoin");
		var starttime = System.currentTimeMillis();

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoSerializerDeserializer();
			final List<Tuple2> inputs1, inputs2;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}

			Stream<Tuple2> joinpairs = inputs1.parallelStream().flatMap(tup1 -> {
				List<Tuple2> joinlist = inputs2.parallelStream().filter(tup2 -> tup1.v1.equals(tup2.v1))
						.map(tup2 -> new Tuple2(tup2.v1, new Tuple2(tup1.v2, tup2.v2))).collect(Collectors.toList());
				if (joinlist.isEmpty()) {
					return Arrays.asList(new Tuple2(tup1.v1, new Tuple2(tup1.v2, null))).stream();
				}
				return joinlist.stream();
			});

			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + joinpairs.count()).getBytes());
					} else {
						joinpairs.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try {
					joinpairsout.add(joinpairs.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				joinpairsout = joinpairs.collect(Collectors.toList());

			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processLeftJoin");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Join task is " + timetaken + " seconds");
			log.debug("GC Status Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
		}
	}

	/**
	 * Left Join pair operation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in milliseconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processRightJoin(InputStream streamfirst, InputStream streamsecond, boolean isinputfirstblocks,
			boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskDExecutor.processRightJoin");
		var starttime = System.currentTimeMillis();

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoSerializerDeserializer();
			final List<Tuple2> inputs1, inputs2;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}

			Stream<Tuple2> joinpairs = inputs2.parallelStream().flatMap(tup1 -> {
				List<Tuple2> joinlist = inputs1.parallelStream().filter(tup2 -> tup1.v1.equals(tup2.v1))
						.map(tup2 -> new Tuple2(tup2.v1, new Tuple2(tup2.v2, tup1.v2))).collect(Collectors.toList());
				if (joinlist.isEmpty()) {
					return Arrays.asList(new Tuple2(tup1.v1, new Tuple2(null, tup1.v2))).stream();
				}
				return joinlist.stream();
			});

			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + joinpairs.count()).getBytes());
					} else {
						joinpairs.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try {
					joinpairsout.add(joinpairs.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				joinpairsout = joinpairs.collect(Collectors.toList());

			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processRightJoin");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Join task is " + timetaken + " seconds");
			log.debug("GC Status Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
		}
	}

	/**
	 * Join pair operation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processJoinLZF(InputStream streamfirst, InputStream streamsecond, JoinPredicate joinpredicate,
			boolean isinputfirstblocks, boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskDExecutor.processJoinLZF");
		var starttime = System.currentTimeMillis();

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoSerializerDeserializer();
			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));
						var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqinnerjoin = seq1.innerJoin(seq2, joinpredicate);) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + seqinnerjoin.count()).getBytes());
					} else {
						seqinnerjoin.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqinnerjoin = seq1.innerJoin(seq2, joinpredicate)) {
					joinpairsout.add(seqinnerjoin.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqinnerjoin = seq1.innerJoin(seq2, joinpredicate)) {
					joinpairsout = seqinnerjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							var cf = (CompletableFuture) joinpairsout.stream().filter(val -> val instanceof Tuple2)
									.filter(value -> {
										Tuple2 csvrec = (Tuple2) value;
										Object rec1 = csvrec.v1;
										Object rec2 = csvrec.v2;
										return rec1 instanceof CSVRecord && rec2 instanceof CSVRecord;
									}).map((Object rec) -> {
										try {
											Tuple2 csvrec = (Tuple2) rec;
											CSVRecord rec1 = (CSVRecord) csvrec.v1;
											CSVRecord rec2 = (CSVRecord) csvrec.v2;
											Map<String, String> keyvalue = rec1.toMap();
											keyvalue.putAll(rec2.toMap());
											List<String> keys = new ArrayList<>(keyvalue.keySet());
											CSVRecord recordmutated = CSVParser
													.parse(keyvalue.values().stream().collect(Collectors.joining(",")),
															CSVFormat.DEFAULT
																	.withHeader(keys.toArray(new String[keys.size()])))
													.getRecords().get(0);
											return recordmutated;
										} catch (IOException e) {

										}
										return null;
									})
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							joinpairsout = (List<Map<String, String>>) cf.get();
						}
					}
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}

			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Join task is " + timetaken + " seconds");
			log.debug("GC Status Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
		}
	}

	@SuppressWarnings("unchecked")
	public double processLeftOuterJoinLZF(InputStream streamfirst, InputStream streamsecond,
			LeftOuterJoinPredicate leftouterjoinpredicate, boolean isinputfirstblocks, boolean isinputsecondblocks)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processLeftOuterJoinLZF");

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoSerializerDeserializer();
			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));
						var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqleftouterjoin = seq1.leftOuterJoin(seq2, leftouterjoinpredicate);) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + seqleftouterjoin.count()).getBytes());
					} else {
						seqleftouterjoin.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqleftouterjoin = seq1.leftOuterJoin(seq2, leftouterjoinpredicate)) {
					joinpairsout.add(seqleftouterjoin.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqleftouterjoin = seq1.leftOuterJoin(seq2, leftouterjoinpredicate)) {
					joinpairsout = seqleftouterjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							var cf = (CompletableFuture) joinpairsout.stream().filter(val -> val instanceof Tuple2)
									.filter(value -> {
										Tuple2 csvrec = (Tuple2) value;
										Object rec1 = csvrec.v1;
										Object rec2 = csvrec.v2;
										return rec1 instanceof CSVRecord && rec2 instanceof CSVRecord;
									}).map((Object rec) -> {
										try {
											Tuple2 csvrec = (Tuple2) rec;
											CSVRecord rec1 = (CSVRecord) csvrec.v1;
											CSVRecord rec2 = (CSVRecord) csvrec.v2;
											Map<String, String> keyvalue = rec1.toMap();
											keyvalue.putAll(rec2.toMap());
											List<String> keys = new ArrayList<>(keyvalue.keySet());
											CSVRecord recordmutated = CSVParser
													.parse(keyvalue.values().stream().collect(Collectors.joining(",")),
															CSVFormat.DEFAULT
																	.withHeader(keys.toArray(new String[keys.size()])))
													.getRecords().get(0);
											return recordmutated;
										} catch (IOException e) {

										}
										return null;
									})
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							joinpairsout = (List) cf.get();
						}
					}
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				}
			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processLeftOuterJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Left Outer Join task is " + timetaken + " seconds");
			log.debug("GC Status Left Outer Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
		}
	}

	@SuppressWarnings("unchecked")
	public double processRightOuterJoinLZF(InputStream streamfirst, InputStream streamsecond,
			RightOuterJoinPredicate rightouterjoinpredicate, boolean isinputfirstblocks, boolean isinputsecondblocks)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processRightOuterJoinLZF");

		try (var fsdos = new ByteArrayOutputStream();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoSerializerDeserializer();
			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));
						var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqrightouterjoin = seq1.rightOuterJoin(seq2, rightouterjoinpredicate);) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + seqrightouterjoin.count()).getBytes());
					} else {
						seqrightouterjoin.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqrightouterjoin = seq1.rightOuterJoin(seq2, rightouterjoinpredicate)) {
					joinpairsout.add(seqrightouterjoin.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqrightouterjoin = seq1.rightOuterJoin(seq2, rightouterjoinpredicate)) {
					joinpairsout = seqrightouterjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							var cf = (CompletableFuture) joinpairsout.stream().filter(val -> val instanceof Tuple2)
									.filter(value -> {
										Tuple2 csvrec = (Tuple2) value;
										Object rec1 = csvrec.v1;
										Object rec2 = csvrec.v2;
										return rec1 instanceof CSVRecord && rec2 instanceof CSVRecord;
									}).map((Object rec) -> {
										try {
											Tuple2 csvrec = (Tuple2) rec;
											CSVRecord rec1 = (CSVRecord) csvrec.v1;
											CSVRecord rec2 = (CSVRecord) csvrec.v2;
											Map<String, String> keyvalue = rec1.toMap();
											keyvalue.putAll(rec2.toMap());
											List<String> keys = new ArrayList<>(keyvalue.keySet());
											CSVRecord recordmutated = CSVParser
													.parse(keyvalue.values().stream().collect(Collectors.joining(",")),
															CSVFormat.DEFAULT
																	.withHeader(keys.toArray(new String[keys.size()])))
													.getRecords().get(0);
											return recordmutated;
										} catch (IOException e) {

										}
										return null;
									})
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							joinpairsout = (List) cf.get();
						}
					}
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				}
			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processRightOuterJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Right Outer Join task is " + timetaken + " seconds");
			log.debug("GC Status Right Outer Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
		}
	}

	/**
	 * Group by key pair operation.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processGroupByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processGroupByKeyTuple2");

		try (var fsdos = new ByteArrayOutputStream(); var output = new Output(fsdos);) {
			var kryo = Utils.getKryoSerializerDeserializer();

			var allpairs = new ArrayList<>();
			var mapgpbykey = new LinkedHashSet<Map>();
			for (var fs : task.input) {
				try (var fsdis = ((InputStream) fs); var input = new Input(new BufferedInputStream(fsdis));) {
					// while (input.available() > 0) {
					var keyvaluepair = kryo.readClassAndObject(input);
					if (keyvaluepair instanceof List kvp) {
						allpairs.addAll(kvp);
					} else if (keyvaluepair instanceof Map kvpmap) {
						mapgpbykey.add(kvpmap);
					}
					// }
				} catch (IOException ioe) {
					log.error(PipelineConstants.FILEIOERROR, ioe);
					throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSGROUPBYKEY, ex);
					throw new PipelineException(PipelineConstants.PROCESSGROUPBYKEY, ex);
				}
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (!allpairs.isEmpty()) {
						var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(
								tup2 -> tup2.v1, Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
						processedgroupbykey.keySet().stream().map(key -> Tuple.tuple(key, processedgroupbykey.get(key)))
								.forEach(val -> {
									try {
										os.write(val.toString().getBytes());
										os.write(ch);
									} catch (IOException e) {
									}
								});

					} else if (!mapgpbykey.isEmpty()) {
						var result = (Map) mapgpbykey.parallelStream().flatMap(map1 -> map1.entrySet().parallelStream())
								.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors.mapping(
										(Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
						result.keySet().stream().map(key -> Tuple.tuple(key, result.get(key))).forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});

					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// Parallel processing of group by key operation.
			if (!allpairs.isEmpty()) {
				var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(tup2 -> tup2.v1,
						Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
				var cf = (CompletableFuture) processedgroupbykey.keySet().stream()
						.map(key -> Tuple.tuple(key, processedgroupbykey.get(key)))
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				kryo.writeClassAndObject(output, cf.get());
			} else if (!mapgpbykey.isEmpty()) {
				var result = (Map) mapgpbykey.parallelStream().flatMap(map1 -> map1.entrySet().parallelStream())
						.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors
								.mapping((Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
				var cf = (CompletableFuture) result.keySet().stream().map(key -> Tuple.tuple(key, result.get(key)))
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				var out = (List) cf.get();
				result.keySet().parallelStream().forEach(key -> out.add(Tuple.tuple(key, result.get(key))));
				kryo.writeClassAndObject(output, out);
			}
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processGroupByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Group By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Group By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSGROUPBYKEY, ex);
			throw new PipelineException(PipelineConstants.PROCESSGROUPBYKEY, ex);
		}
	}

	/**
	 * Fold by key pair operation.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processFoldByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processFoldByKeyTuple2");

		try (var fsdos = new ByteArrayOutputStream(); var output = new Output(fsdos);) {
			var kryo = Utils.getKryoSerializerDeserializer();
			var allpairs = new ArrayList<Tuple2>();
			var mapgpbykey = new LinkedHashSet<Map>();
			for (var fs : task.input) {
				try (var fsdis = ((InputStream) fs); var input = new Input(new BufferedInputStream(fsdis));) {
					// while (input.available() > 0) {
					var keyvaluepair = kryo.readClassAndObject(input);
					if (keyvaluepair instanceof List kvp) {
						allpairs.addAll(kvp);
					} else if (keyvaluepair instanceof Map kvpmap) {
						mapgpbykey.add(kvpmap);
					}
					// }
				} catch (IOException ioe) {
					log.error(PipelineConstants.FILEIOERROR, ioe);
					throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSGROUPBYKEY, ex);
					throw new PipelineException(PipelineConstants.PROCESSGROUPBYKEY, ex);
				}
			}
			// Parallel processing of fold by key operation.
			var foldbykey = (FoldByKey) jobstage.getStage().tasks.get(0);
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (!allpairs.isEmpty()) {
						var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(
								tup2 -> tup2.v1, Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
						for (var key : processedgroupbykey.keySet()) {
							var seqtuple2 = Seq.of(processedgroupbykey.get(key).toArray());
							Object foldbykeyresult;
							if (foldbykey.isLeft()) {
								foldbykeyresult = seqtuple2.foldLeft(foldbykey.getValue(),
										foldbykey.getReduceFunction());
							} else {
								foldbykeyresult = seqtuple2.foldRight(foldbykey.getValue(),
										foldbykey.getReduceFunction());
							}
							os.write(Tuple.tuple(key, foldbykeyresult).toString().getBytes());
							os.write(ch);

						}
					} else if (!mapgpbykey.isEmpty()) {
						var result = (Map<Object, List<Object>>) mapgpbykey.parallelStream()
								.flatMap(map1 -> map1.entrySet().parallelStream())
								.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors.mapping(
										(Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
						result.keySet().parallelStream().forEach(key -> {
							try {
								os.write(Tuple.tuple(key, result.get(key)).toString().getBytes());
								os.write(ch);
							} catch (Exception ex) {

							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}

			if (!allpairs.isEmpty()) {
				var finalfoldbykeyobj = new ArrayList<>();
				var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(tup2 -> tup2.v1,
						Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
				for (var key : processedgroupbykey.keySet()) {
					var seqtuple2 = Seq.of(processedgroupbykey.get(key).toArray());
					Object foldbykeyresult;
					if (foldbykey.isLeft()) {
						foldbykeyresult = seqtuple2.foldLeft(foldbykey.getValue(), foldbykey.getReduceFunction());
					} else {
						foldbykeyresult = seqtuple2.foldRight(foldbykey.getValue(), foldbykey.getReduceFunction());
					}
					finalfoldbykeyobj.add(Tuple.tuple(key, foldbykeyresult));
				}
				kryo.writeClassAndObject(output, finalfoldbykeyobj);
				output.flush();
				var wr = new WeakReference<List>(finalfoldbykeyobj);
				finalfoldbykeyobj = null;

			} else if (!mapgpbykey.isEmpty()) {
				var result = (Map<Object, List<Object>>) mapgpbykey.parallelStream()
						.flatMap(map1 -> map1.entrySet().parallelStream())
						.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors
								.mapping((Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));

				var out = result.keySet().parallelStream().map(key -> Tuple.tuple(key, result.get(key)))
						.collect(Collectors.toList());
				kryo.writeClassAndObject(output, out);
				output.flush();
				var wr = new WeakReference<List>(out);
				out = null;
			}
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processFoldByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Fold By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Fold By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSFOLDBYKEY, ex);
			throw new PipelineException(PipelineConstants.PROCESSFOLDBYKEY, ex);
		}
	}

	/**
	 * Count by key pair operation.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processCountByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processCountByKeyTuple2");
		try (var fsdos = new ByteArrayOutputStream(); var output = new Output(fsdos);) {
			var kryo = Utils.getKryoSerializerDeserializer();

			var allpairs = new ArrayList<Tuple2<Object, Object>>();
			for (var fs : task.input) {
				var fsdis = ((InputStream) fs);
				var input = new Input(new BufferedInputStream(fsdis));
				// while (input.available() > 0) {
				var keyvaluepair = kryo.readClassAndObject(input);
				if (keyvaluepair instanceof List kvp) {
					allpairs.addAll(kvp);
				}
				// }
				input.close();
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (!allpairs.isEmpty()) {
						var processedcountbykey = (Map) allpairs.parallelStream()
								.collect(Collectors.toMap(Tuple2::v1, (Object v2) -> 1l, (a, b) -> a + b));
						var cf = processedcountbykey.entrySet().stream()
								.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()));
						if (jobstage.getStage().tasks.size() > 1) {
							var functions = getFunctions();
							functions.remove(0);
							cf = ((Stream) StreamUtils.getFunctionsToStream(functions, cf.parallel()));
						}
						cf.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// Parallel processing of group by key operation.
			var intermediatelist = (List<Tuple2>) null;
			if (!allpairs.isEmpty()) {
				var processedcountbykey = (Map) allpairs.parallelStream()
						.collect(Collectors.toMap(Tuple2::v1, (Object v2) -> 1l, (a, b) -> a + b));
				var cf = (CompletableFuture) processedcountbykey.entrySet().stream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				intermediatelist = (List<Tuple2>) cf.get();
				if (jobstage.getStage().tasks.size() > 1) {
					var functions = getFunctions();
					functions.remove(0);
					cf = (CompletableFuture) ((Stream) StreamUtils.getFunctionsToStream(functions,
							intermediatelist.parallelStream()))
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					intermediatelist = (List<Tuple2>) cf.get();
				}
				kryo.writeClassAndObject(output, intermediatelist);
			}
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(intermediatelist);
			intermediatelist = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processCountByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Count By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSCOUNTBYKEY, ex);
			throw new PipelineException(PipelineConstants.PROCESSCOUNTBYKEY, ex);
		}
	}

	/**
	 * Count by key pair operation.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processCountByValueTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processCountByValueTuple2");
		try (var fsdos = new ByteArrayOutputStream(); var output = new Output(fsdos);) {
			var kryo = Utils.getKryoSerializerDeserializer();

			var allpairs = new ArrayList<Tuple2<Object, Object>>();
			for (var fs : task.input) {
				var fsdis = ((InputStream) fs);
				var input = new Input(new BufferedInputStream(fsdis));
				// while (input.available() > 0) {
				var keyvaluepair = kryo.readClassAndObject(input);
				if (keyvaluepair instanceof List kvp) {
					allpairs.addAll(kvp);
				}
				// }
				input.close();
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (!allpairs.isEmpty()) {
						var processedcountbyvalue = (Map) allpairs.parallelStream()
								.collect(Collectors.toMap(tuple2 -> tuple2, (Object v2) -> 1l, (a, b) -> a + b));
						var cf = processedcountbyvalue.entrySet().stream()
								.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()));
						if (jobstage.getStage().tasks.size() > 1) {
							var functions = getFunctions();
							functions.remove(0);
							cf = ((Stream) StreamUtils.getFunctionsToStream(functions, cf.parallel()));
						}
						cf.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// Parallel processing of group by key operation.
			var intermediatelist = (List<Tuple2>) null;
			if (!allpairs.isEmpty()) {
				var processedcountbyvalue = (Map) allpairs.parallelStream()
						.collect(Collectors.toMap(tuple2 -> tuple2, (Object v2) -> 1l, (a, b) -> a + b));
				var cf = (CompletableFuture) processedcountbyvalue.entrySet().stream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				intermediatelist = (List) cf.get();
				if (jobstage.getStage().tasks.size() > 1) {
					var functions = getFunctions();
					functions.remove(0);
					cf = (CompletableFuture) ((Stream) StreamUtils.getFunctionsToStream(functions,
							intermediatelist.stream()))
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					intermediatelist = (List) cf.get();
				}

				kryo.writeClassAndObject(output, intermediatelist);
			}
			output.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(intermediatelist);
			intermediatelist = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processCountByValueTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Count By Value Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Value task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSCOUNTBYVALUE, ex);
			throw new PipelineException(PipelineConstants.PROCESSCOUNTBYVALUE, ex);
		}
	}

	public void closeFSOutput(Collection<Output> outvalues, Collection<OutputStream> fsdoss) throws Exception {
		for (var value : outvalues) {
			value.close();
		}
		for (var fs : fsdoss) {
			fs.close();
		}
	}

	/**
	 * Result of Coalesce by key operation
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processCoalesce() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processCoalesce");
		var coalescefunction = (List<Coalesce>) getFunctions();
		try (var fsdos = new ByteArrayOutputStream();
				var currentoutput = new Output(fsdos);) {
			var kryo = Utils.getKryoSerializerDeserializer();
			var keyvaluepairs = new ArrayList<Tuple2>();
			for (var fs : task.input) {
				try (var fsis = (InputStream) fs; Input input = new Input(new BufferedInputStream(fsis));) {
					keyvaluepairs.addAll((List) kryo.readClassAndObject(input));
				}
			}
			log.debug("Coalesce Data Size:" + keyvaluepairs.size());
			// Parallel execution of reduce by key stream execution.
			List out = null;
			if (Objects.nonNull(coalescefunction.get(0))
					&& Objects.nonNull(coalescefunction.get(0).getCoalescefunction())) {
				if (coalescefunction.get(0).getCoalescefunction() instanceof PipelineCoalesceFunction pcf) {
					out = Arrays.asList(keyvaluepairs.parallelStream().reduce(pcf).get());
				} else {
					out = keyvaluepairs.parallelStream().collect(Collectors.toMap(Tuple2::v1, Tuple2::v2,
							(input1, input2) -> coalescefunction.get(0).getCoalescefunction().apply(input1, input2)))
							.entrySet().stream()
							.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()))
							.get();
				}
			} else {
				out = keyvaluepairs;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (out instanceof List) {
						out.parallelStream().forEach(obj -> {
							try {
								if (obj instanceof List objs) {
									objs.stream().forEach(object -> {
										try {
											os.write(object.toString().getBytes());
											os.write(ch);
										} catch (Exception ex) {

										}
									});
								} else {
									os.write(obj.toString().getBytes());
									os.write(ch);
								}
							} catch (Exception ex) {

							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			var outpairs = out;
			var functions = getFunctions();
			if (functions.size() > 1) {
				functions.remove(0);
				var finaltask = functions.get(functions.size() - 1);
				var stream = StreamUtils.getFunctionsToStream(functions, outpairs.stream());
				if (finaltask instanceof CalculateCount) {
					outpairs = new Vector<>();
					if (stream instanceof IntStream ints) {
						outpairs.add(ints.count());
					} else {
						outpairs.add(((Stream) stream).count());
					}
				} else if (finaltask instanceof PipelineIntStreamCollect piplineistream) {
					outpairs = new Vector<>();
					outpairs.add(((IntStream) stream).collect(piplineistream.getSupplier(),
							piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

				} else {
					CompletableFuture<Vector> cf = (CompletableFuture) ((Stream) stream)
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					outpairs = cf.get();
				}
			}
			kryo.writeClassAndObject(currentoutput, outpairs);
			currentoutput.flush();
			fsdos.flush();
			byte[] intermediatedata = fsdos.toByteArray();
			OutputStream os = createIntermediateDataToFS(task, intermediatedata.length);
			IOUtils.write(intermediatedata, os);
			if (os instanceof ByteBufferOutputStream bbos) {
				bbos.get().flip();
			}
			cacheAble(fsdos);
			var wr = new WeakReference<List>(outpairs);
			outpairs = null;
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processCoalesce");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Coalesce Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Value task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSCOALESCE, ex);
			throw new PipelineException(PipelineConstants.PROCESSCOALESCE, ex);
		}
	}

	public void writeFinalPhaseResultsToHdfs(OutputStream os) throws Exception {
		if (os instanceof ByteArrayOutputStream baos) {
			try (InputStream sis = new ByteArrayInputStream(baos.toByteArray());) {
				Utils.writeResultToHDFS(task.hdfsurl, task.filepath, sis);
			} catch (Exception ex) {
				log.error(MDCConstants.EMPTY, ex);
				throw ex;
			}
		}
		log.info("Exiting writeFinalPhaseResultsToHdfs: ");
	}
}
