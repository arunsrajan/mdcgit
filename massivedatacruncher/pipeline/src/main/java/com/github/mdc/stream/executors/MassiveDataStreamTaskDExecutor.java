package com.github.mdc.stream.executors;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
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
import java.util.concurrent.Callable;
import java.util.function.IntSupplier;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.Blocks;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ByteArrayOutputStreamPool;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.HdfsBlockReader;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.MassiveDataPipelineConstants;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.CsvOptions;
import com.github.mdc.stream.Json;
import com.github.mdc.stream.MassiveDataPipelineException;
import com.github.mdc.stream.MassiveDataPipelineUtils;
import com.github.mdc.stream.PipelineIntStreamCollect;
import com.github.mdc.stream.functions.CalculateCount;
import com.github.mdc.stream.functions.Coalesce;
import com.github.mdc.stream.functions.CountByKeyFunction;
import com.github.mdc.stream.functions.CountByValueFunction;
import com.github.mdc.stream.functions.FoldByKey;
import com.github.mdc.stream.functions.GroupByKeyFunction;
import com.github.mdc.stream.functions.IntersectionFunction;
import com.github.mdc.stream.functions.JoinPredicate;
import com.github.mdc.stream.functions.LeftOuterJoinPredicate;
import com.github.mdc.stream.functions.Max;
import com.github.mdc.stream.functions.Min;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;
import com.github.mdc.stream.functions.StandardDeviation;
import com.github.mdc.stream.functions.Sum;
import com.github.mdc.stream.functions.SummaryStatistics;
import com.github.mdc.stream.functions.UnionFunction;
import com.github.mdc.stream.utils.StreamUtils;

/**
 * 
 * @author Arun Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public sealed class MassiveDataStreamTaskDExecutor implements
		Callable<MassiveDataStreamTaskDExecutor> permits MassiveDataStreamTaskExecutorInMemory,MassiveDataStreamJGroupsTaskExecutor,MassiveDataStreamTaskExecutorMesos,MassiveDataStreamTaskExecutorYarn {
	protected JobStage jobstage;
	protected HeartBeatTaskSchedulerStream hbtss;
	private static Logger log = Logger.getLogger(MassiveDataStreamTaskDExecutor.class);
	protected FileSystem hdfs = null;
	protected boolean completed = false;
	Cache cache;
	Task task;
	boolean iscacheable=false;
	public MassiveDataStreamTaskDExecutor(JobStage jobstage, Cache cache) {
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
	}

	/**
	 * Get the list of all the functions.
	 * 
	 * @return
	 */
	private List getFunctions() {
		log.debug("Entered MassiveDataStreamTaskDExecutor.getFunctions");
		var tasks = jobstage.stage.tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		log.debug("Exiting MassiveDataStreamTaskDExecutor.getFunctions");
		return functions;
	}

	protected String getStagesTask() {
		log.debug("Entered MassiveDataStreamTaskDExecutor.getStagesTask");
		var tasks = jobstage.stage.tasks;
		var builder = new StringBuilder();
		for (var task : tasks) {
			builder.append(MassiveDataPipelineUtils.getFunctions(task));
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

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));

				var bais1 = HdfsBlockReader.getBlockDataLZFStream(blocksfirst, hdfs);
				var buffer1 = new BufferedReader(new InputStreamReader(bais1));
				var bais2 = HdfsBlockReader.getBlockDataLZFStream(blockssecond, hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamfirst = buffer1.lines().parallel();
				var streamsecond = buffer2.lines().parallel();) {
			var setsecond = (Set) streamsecond.distinct().collect(Collectors.toSet());
			// Get the result of intersection functions parallel.
			var result = (List) streamfirst.distinct().filter(setsecond::contains)
					.collect(Collectors.toCollection(Vector::new));
			var kryo = Utils.getKryoNonDeflateSerializer();
			kryo.writeClassAndObject(output, result);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSINTERSECTION, ex);
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

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next()));
				var bais2 = HdfsBlockReader.getBlockDataLZFStream(blockssecond.get(0), hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamsecond = buffer2.lines().parallel();) {
			var kryo = Utils.getKryoNonDeflateSerializer();
			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			var setsecond = (Set) streamsecond.distinct().collect(Collectors.toSet());
			// Parallel execution of the intersection function.
			var result = (List) datafirst.parallelStream().distinct().filter(setsecond::contains)
					.collect(Collectors.toCollection(Vector::new));
			kryo.writeClassAndObject(output, result);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSINTERSECTION, ex);
		}
	}

	/**
	 * Process the data using intersection function.
	 * 
	 * @param fsstreamfirst
	 * @param fsstreamsecond
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSIntersection(List<InputStream> fsstreamfirst, List<InputStream> fsstreamsecond)
			throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next()));
				var inputsecond = new Input(new BufferedInputStream(fsstreamsecond.iterator().next()));

		) {

			var kryo = Utils.getKryoNonDeflateSerializer();
			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			var datasecond = (List) kryo.readClassAndObject(inputsecond);
			// parallel execution of intersection function.
			var result = (List) datafirst.parallelStream().distinct().filter(datasecond::contains)
					.collect(Collectors.toCollection(Vector::new));
			kryo.writeClassAndObject(output, result);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSINTERSECTION, ex);
		}
	}

	/**
	 * Get the HDFS file path using the job and stage id.
	 * 
	 * @return
	 */
	public String getIntermediateDataFSFilePath(Task task) {
		return (MDCConstants.BACKWARD_SLASH + FileSystemSupport.MDS + MDCConstants.BACKWARD_SLASH + jobstage.jobid
				+ MDCConstants.BACKWARD_SLASH + jobstage.jobid + MDCConstants.HYPHEN + jobstage.stageid
				+ MDCConstants.HYPHEN + task.taskid + MDCConstants.DATAFILEEXTN);
	}

	/**
	 * Create a file in HDFS and return the stream.
	 * 
	 * @param hdfs
	 * @return
	 * @throws Exception
	 */
	public OutputStream createIntermediateDataToFS(Task task) throws MassiveDataPipelineException {
		log.debug("Entered MassiveDataStreamTaskDExecutor.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			var hdfspath = new Path(path);
			log.debug("Exiting MassiveDataStreamTaskDExecutor.createIntermediateDataToFS");
			return hdfs.create(hdfspath, false);
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		}
	}

	/**
	 * Open the already existing file.
	 * 
	 * @param hdfs
	 * @param block
	 * @return
	 * @throws Exception
	 */
	private InputStream getInputStreamHDFS(FileSystem hdfs, String filename) throws MassiveDataPipelineException {
		log.debug("Entered MassiveDataStreamTaskDExecutor.getInputStreamHDFS");
		try {
			var fileStatus = hdfs.listStatus(new Path(hdfs.getUri().toString() + filename));
			var paths = FileUtil.stat2Paths(fileStatus);
			log.debug("Exiting MassiveDataStreamTaskDExecutor.getInputStreamHDFS");
			return hdfs.open(paths[0]);
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		}
	}

	/**
	 * Open the already existing file using the job and stageid.
	 * 
	 * @param hdfs
	 * @return
	 * @throws Exception
	 */
	public InputStream getIntermediateInputStreamFS(Task task) throws Exception {
		var path = getIntermediateDataFSFilePath(task);
		return getInputStreamHDFS(hdfs, path);
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
			throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var bais1 = HdfsBlockReader.getBlockDataLZFStream(blocksfirst, hdfs);
				var buffer1 = new BufferedReader(new InputStreamReader(bais1));
				var bais2 = HdfsBlockReader.getBlockDataLZFStream(blockssecond, hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamfirst = buffer1.lines().parallel();
				var streamsecond = buffer2.lines().parallel();) {
			boolean terminalCount = false;
			if (jobstage.stage.tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List result;
			if (terminalCount) {
				result = new Vector<>();
				result.add(java.util.stream.Stream.concat(streamfirst, streamsecond).distinct().count());
			} else {
				// parallel stream union operation result
				result = (List) java.util.stream.Stream.concat(streamfirst, streamsecond).distinct()
						.collect(Collectors.toCollection(Vector::new));
			}
			var kryo = Utils.getKryoNonDeflateSerializer();
			kryo.writeClassAndObject(output, result);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSHDFSUNION, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSUNION, ex);
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
			FileSystem hdfs) throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next()));
				var bais2 = HdfsBlockReader.getBlockDataLZFStream(blockssecond.get(0), hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamsecond = buffer2.lines().parallel();) {
			var kryo = Utils.getKryoNonDeflateSerializer();

			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			boolean terminalCount = false;
			if (jobstage.stage.tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List result;
			if (terminalCount) {
				result = new Vector<>();
				result.add(java.util.stream.Stream.concat(datafirst.parallelStream(), streamsecond).distinct().count());
			} else {
				// parallel stream union operation result
				result = (List) java.util.stream.Stream.concat(datafirst.parallelStream(), streamsecond).distinct()
						.collect(Collectors.toCollection(Vector::new));
			}
			kryo.writeClassAndObject(output, result);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSHDFSUNION, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSUNION, ex);
		}
	}

	/**
	 * Perform the union operation in parallel.
	 * 
	 * @param fsstreamfirst
	 * @param fsstreamsecond
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSUnion(List<InputStream> fsstreamfirst, List<InputStream> fsstreamsecond)
			throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next()));
				var inputsecond = new Input(new BufferedInputStream(fsstreamsecond.iterator().next()));) {

			var kryo = Utils.getKryoNonDeflateSerializer();

			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			var datasecond = (List) kryo.readClassAndObject(inputsecond);
			List result;
			var terminalCount = false;
			if (jobstage.stage.tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}

			if (terminalCount) {
				result = new ArrayList<>();
				result.add(java.util.stream.Stream.concat(datafirst.parallelStream(), datasecond.parallelStream())
						.distinct().count());
			} else {
				// parallel stream union operation result
				result = (List) java.util.stream.Stream.concat(datafirst.parallelStream(), datasecond.parallelStream())
						.distinct().collect(Collectors.toCollection(Vector::new));
			}

			kryo.writeClassAndObject(output, result);
			output.flush();			
			fsdos.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSHDFSUNION, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSUNION, ex);
		}
	}

	/**
	 * Perform map operation to obtain intermediate stage result.
	 * 
	 * @param blockslocation
	 * @param hdfs
	 * @param skeletons
	 * @throws Throwable
	 */
	@SuppressWarnings("unchecked")
	public double processBlockHDFSMap(BlocksLocation blockslocation, FileSystem hdfs)
			throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSMap");
		log.info(blockslocation);
		CSVParser records = null;
		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var bais = HdfsBlockReader.getBlockDataLZFStream(blockslocation, hdfs);
				var buffer = new BufferedReader(new InputStreamReader(bais));) {

			var kryo = Utils.getKryoNonDeflateSerializer();
			List datastream = null;
			var tasks = jobstage.stage.tasks;
			Stream intermediatestreamobject;
			if (jobstage.stage.tasks.get(0) instanceof Json) {
				intermediatestreamobject = buffer.lines().parallel();
				intermediatestreamobject = intermediatestreamobject.map(line -> {
					try {
						return new JSONParser().parse((String) line);
					} catch (ParseException e) {
						return null;
					}
				});
			} else {
				if (jobstage.stage.tasks.get(0) instanceof CsvOptions csvoptions) {
					var csvformat = CSVFormat.EXCEL;
					csvformat = csvformat.withDelimiter(',').withHeader(csvoptions.getHeader()).withIgnoreHeaderCase()
							.withTrim();

					try {
						records = csvformat.parse(buffer);
						Stream<CSVRecord> streamcsv = StreamSupport.stream(records.spliterator(), false);
						intermediatestreamobject = streamcsv.parallel();
					} catch (IOException ioe) {
						log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
						throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
					} catch (Exception ex) {
						log.error(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
						throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
					}
				} else {
					intermediatestreamobject = buffer.lines().parallel();
				}

			}
			intermediatestreamobject.onClose(() -> {
				log.debug("Stream closed");
			});
			var finaltask = jobstage.stage.tasks.get(jobstage.stage.tasks.size() - 1);

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
					var streamtmp = ((java.util.stream.IntStream) streammap).boxed().collect(Collectors.toList());
					var mean = (streamtmp).stream().mapToInt(Integer.class::cast).average().getAsDouble();
					var variance = (streamtmp).stream().mapToInt(Integer.class::cast)
							.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
					var standardDeviation = Math.sqrt(variance);
					out.add(standardDeviation);

				} else {
					out = (List) ((Stream) streammap).collect(Collectors.toCollection(ArrayList::new));
				}
				kryo.writeClassAndObject(output, out);
				output.flush();
				fsdos.flush();
				if(iscacheable) {
					cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
					((ByteArrayOutputStream) fsdos).reset();
					ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
				}
				log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSMap");
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				log.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
				log.debug("GC Status Map task:" + Utils.getGCStats());
				return timetaken;
			} catch (IOException ioe) {
				log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
				throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
				throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
			}
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
		}
		finally {
			if(!Objects.isNull(records)) {
				try {
					records.close();
				} catch (Exception e) {
					log.error(MDCConstants.EMPTY,e);
				}
			}
		}

	}

	/**
	 * Perform map operation to obtain intermediate stage result.
	 * 
	 * @param fsstreamfirst
	 * @param hdfs
	 * @param skeletons
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processBlockHDFSMap(Set<InputStream> fsstreamfirst) throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processBlockHDFSMap");
		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));) {
			var kryo = Utils.getKryoNonDeflateSerializer();
			var functions = getFunctions();

			List out = new ArrayList<>();
			var finaltask = jobstage.stage.tasks.get(jobstage.stage.tasks.size() - 1);
			for (var InputStream : fsstreamfirst) {
				try (var input = new Input(InputStream);) {
					// while (input.available() > 0) {
					var inputdatas = (List) kryo.readClassAndObject(input);
					// Get Streams object from list of map functions.
					try (BaseStream streammap = (BaseStream) StreamUtils.getFunctionsToStream(functions,
							inputdatas.parallelStream());) {
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
							var streamtmp = (List) ((java.util.stream.IntStream) streammap).boxed()
									.collect(Collectors.toList());
							double mean = (streamtmp).stream().mapToInt(Integer.class::cast).average().getAsDouble();
							double variance = (double) (streamtmp).stream().mapToInt(Integer.class::cast)
									.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
							double standardDeviation = Math.sqrt(variance);
							out.add(standardDeviation);

						} else {
							out = (List) ((Stream) streammap).collect(Collectors.toCollection(Vector::new));
						}
					} catch (Exception ex) {
						log.error(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
						throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
					}
					// }
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
				}
			}
			kryo.writeClassAndObject(output, out);
			output.flush();
			fsdos.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processBlockHDFSMap");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
			log.debug("GC Status Map task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSHDFSERROR, ex);
		}
	}

	@SuppressWarnings("unchecked")
	public double processSamplesBlocks(Integer numofsample, BlocksLocation blockslocation, FileSystem hdfs)
			throws Exception {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processSamplesBlocks");
		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var bais = HdfsBlockReader.getBlockDataLZFStream(blockslocation, hdfs);
				var buffer = new BufferedReader(new InputStreamReader(bais));
				var stringdata = buffer.lines().parallel();) {
			Kryo kryo = Utils.getKryoNonDeflateSerializer();
			// Limit the sample using the limit method.
			boolean terminalCount = false;
			if (jobstage.stage.tasks.get(jobstage.stage.tasks.size() - 1) instanceof CalculateCount) {
				terminalCount = true;
			}
			List out;
			if (terminalCount) {
				out = new Vector<>();
				out.add(stringdata.limit(numofsample).count());
			} else {
				out = (List) stringdata.limit(numofsample).collect(Collectors.toCollection(Vector::new));
			}
			kryo.writeClassAndObject(output, out);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processSamplesBlocks");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Sampling Task is " + timetaken + " seconds");
			log.debug("GC Status Sampling task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSSAMPLE, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSSAMPLE, ex);
		}
	}

	/**
	 * Obtain data samples using this method.
	 * 
	 * @param numofsample
	 * @param fsstreams
	 * @param hdfs
	 * @throws Exception
	 */
	public double processSamplesObjects(Integer numofsample, List fsstreams) throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processSamplesObjects");
		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var inputfirst = new Input(new BufferedInputStream(((InputStream) (fsstreams.iterator().next()))));) {
			var kryo = Utils.getKryoNonDeflateSerializer();
			var datafirst = (List) kryo.readClassAndObject(inputfirst);
			var terminalCount = false;
			if (jobstage.stage.tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List out;
			if (terminalCount) {
				out = new Vector<>();
				out.add(datafirst.parallelStream().limit(numofsample).count());
			} else {
				// Limit the sample using the limit method.
				out = (List) datafirst.parallelStream().limit(numofsample)
						.collect(Collectors.toCollection(Vector::new));
			}
			kryo.writeClassAndObject(output, out);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processSamplesObjects");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Sampling Task is " + timetaken + " seconds");
			log.debug("GC Status Sampling task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSSAMPLE, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSSAMPLE, ex);
		}
	}

	public String getIntermediateDataFSFilePath(String jobid, String stageid, String taskid) {
		return (jobid + MDCConstants.HYPHEN + stageid + MDCConstants.HYPHEN + taskid + MDCConstants.DATAFILEEXTN);
	}

	@Override
	public MassiveDataStreamTaskDExecutor call() {
		log.debug("Entered MassiveDataStreamTaskDExecutor.call");
		var stageTasks = getStagesTask();
		var stagePartition = jobstage.stageid;
		var timetakenseconds = 0.0;
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HDFSNN);
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
						task.input[inputindex] = RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(rdf.jobid,
								getIntermediateDataFSFilePath(rdf.jobid, rdf.stageid, rdf.taskid), hdfs);
					}
				}
			}
			hbtss.pingOnce(task.stageid, task.taskid, task.hostport, Task.TaskStatus.RUNNING, timetakenseconds, null);
			timetakenseconds = computeTasks(task, hdfs);
			log.debug("Completed Stage: " + stagePartition);
			completed = true;
			hbtss.setTimetakenseconds(timetakenseconds);
			hbtss.pingOnce(task.stageid, task.taskid, task.hostport, Task.TaskStatus.COMPLETED, timetakenseconds, null);
		} catch (Exception ex) {
			log.error("Failed Stage: " + stagePartition, ex);
			completed = true;
			log.error("Failed Stage: " + task.stageid, ex);
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				hbtss.pingOnce(task.stageid, task.taskid, task.hostport, Task.TaskStatus.FAILED, 0.0,
						new String(baos.toByteArray()));
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ", e);
			}
		}
		log.debug("Exiting MassiveDataStreamTaskDExecutor.call");
		return this;
	}

	public double computeTasks(Task task, FileSystem hdfs) throws Exception {
		var timetakenseconds = 0.0;
		if (jobstage.stage.tasks.get(0) instanceof JoinPredicate jp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataLZFStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataLZFStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataLZFStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataLZFStream((BlocksLocation) task.input[1], hdfs)
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
				log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
				throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(MassiveDataPipelineConstants.PROCESSJOIN, ex);
				throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSJOIN, ex);
			}

		} else if (jobstage.stage.tasks.get(0) instanceof LeftOuterJoinPredicate ljp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataLZFStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataLZFStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataLZFStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataLZFStream((BlocksLocation) task.input[1], hdfs)
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
				log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
				throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(MassiveDataPipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSLEFTOUTERJOIN, ex);
			}
		} else if (jobstage.stage.tasks.get(0) instanceof RightOuterJoinPredicate rjp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataLZFStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataLZFStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataLZFStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataLZFStream((BlocksLocation) task.input[1], hdfs)
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
				log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
				throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(MassiveDataPipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
			}
		} else if (jobstage.stage.tasks.get(0) instanceof IntersectionFunction) {

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
		} else if (jobstage.stage.tasks.get(0) instanceof UnionFunction) {
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
		} else if (jobstage.stage.tasks.get(0) instanceof IntSupplier sample) {
			var numofsample = sample.getAsInt();
			if (task.input[0] instanceof BlocksLocation bl) {
				timetakenseconds = processSamplesBlocks(numofsample, (BlocksLocation) bl, hdfs);
			} else {
				timetakenseconds = processSamplesObjects(numofsample, (List) Arrays.asList(task.input));
			}
		} else if (task.input[0] instanceof BlocksLocation bl) {
			timetakenseconds = processBlockHDFSMap(bl, hdfs);
		} else if (jobstage.stage.tasks.get(0) instanceof GroupByKeyFunction) {
			timetakenseconds = processGroupByKeyTuple2();
		} else if (jobstage.stage.tasks.get(0) instanceof FoldByKey) {
			timetakenseconds = processFoldByKeyTuple2();
		} else if (jobstage.stage.tasks.get(0) instanceof CountByKeyFunction) {
			timetakenseconds = processCountByKeyTuple2();
		} else if (jobstage.stage.tasks.get(0) instanceof CountByValueFunction) {
			timetakenseconds = processCountByValueTuple2();
		} else if (task.input[0] instanceof InputStream) {
			if (jobstage.stage.tasks.get(0) instanceof Coalesce) {
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
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processJoinLZF(InputStream streamfirst, InputStream streamsecond, JoinPredicate joinpredicate,
			boolean isinputfirstblocks, boolean isinputsecondblocks) throws MassiveDataPipelineException {
		log.debug("Entered MassiveDataStreamTaskDExecutor.processJoinLZF");
		var starttime = System.currentTimeMillis();

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoNonDeflateSerializer();
			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				inputs1 = buffreader1.lines().collect(Collectors.toList());
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				inputs2 = buffreader2.lines().collect(Collectors.toList());
			}
			var terminalCount = false;
			if (jobstage.stage.tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqinnerjoin = seq1.parallel().innerJoin(seq2.parallel(), joinpredicate)) {
					joinpairsout.add(seqinnerjoin.count());
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSJOIN, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqinnerjoin = seq1.parallel().innerJoin(seq2.parallel(), joinpredicate)) {
					joinpairsout = seqinnerjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							joinpairsout = (List<Map<String, String>>) joinpairsout.stream()
									.filter(val -> val instanceof Tuple2).filter(value -> {
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
									}).collect(Collectors.toList());
						}
					}
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSJOIN, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSJOIN, ex);
				}

			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Join task is " + timetaken + " seconds");
			log.debug("GC Status Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSJOIN, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSJOIN, ex);
		}
	}

	@SuppressWarnings("unchecked")
	public double processLeftOuterJoinLZF(InputStream streamfirst, InputStream streamsecond,
			LeftOuterJoinPredicate leftouterjoinpredicate, boolean isinputfirstblocks, boolean isinputsecondblocks)
			throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processLeftOuterJoinLZF");

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoNonDeflateSerializer();
			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				inputs1 = buffreader1.lines().collect(Collectors.toList());
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				inputs2 = buffreader2.lines().collect(Collectors.toList());
			}
			boolean terminalCount = false;
			if (jobstage.stage.tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqleftouterjoin = seq1.parallel().leftOuterJoin(seq2.parallel(), leftouterjoinpredicate)) {
					joinpairsout.add(seqleftouterjoin.count());
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSLEFTOUTERJOIN, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqleftouterjoin = seq1.parallel().leftOuterJoin(seq2.parallel(), leftouterjoinpredicate)) {
					joinpairsout = seqleftouterjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							joinpairsout = (List<Map<String, String>>) joinpairsout.stream()
									.filter(val -> val instanceof Tuple2).filter(value -> {
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
									}).collect(Collectors.toList());
						}
					}
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSLEFTOUTERJOIN, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				}
			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processLeftOuterJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Left Outer Join task is " + timetaken + " seconds");
			log.debug("GC Status Left Outer Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSLEFTOUTERJOIN, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSLEFTOUTERJOIN, ex);
		}
	}

	@SuppressWarnings("unchecked")
	public double processRightOuterJoinLZF(InputStream streamfirst, InputStream streamsecond,
			RightOuterJoinPredicate rightouterjoinpredicate, boolean isinputfirstblocks, boolean isinputsecondblocks)
			throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processRightOuterJoinLZF");

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			var kryo = Utils.getKryoNonDeflateSerializer();
			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) kryo.readClassAndObject(inputfirst);
			} else {
				inputs1 = buffreader1.lines().collect(Collectors.toList());
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) kryo.readClassAndObject(inputsecond);
			} else {
				inputs2 = buffreader2.lines().collect(Collectors.toList());
			}
			boolean terminalCount = false;
			if (jobstage.stage.tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqrightouterjoin = seq1.parallel().rightOuterJoin(seq2.parallel(),
								rightouterjoinpredicate)) {
					joinpairsout.add(seqrightouterjoin.count());
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqrightouterjoin = seq1.parallel().rightOuterJoin(seq2.parallel(),
								rightouterjoinpredicate)) {
					joinpairsout = seqrightouterjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							joinpairsout = (List<Map<String, String>>) joinpairsout.stream()
									.filter(val -> val instanceof Tuple2).filter(value -> {
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
									}).collect(Collectors.toList());
						}
					}
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				}
			}
			kryo.writeClassAndObject(output, joinpairsout);
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processRightOuterJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Right Outer Join task is " + timetaken + " seconds");
			log.debug("GC Status Right Outer Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
		}
	}

	/**
	 * Group by key pair operation.
	 * 
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processGroupByKeyTuple2() throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processGroupByKeyTuple2");

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));) {
			var kryo = Utils.getKryoNonDeflateSerializer();

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
					log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSGROUPBYKEY, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSGROUPBYKEY, ex);
				}
			}
			// Parallel processing of group by key operation.
			if (!allpairs.isEmpty()) {
				var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).parallel()
						.groupBy(tup2 -> tup2.v1, Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
				var out = processedgroupbykey.keySet().parallelStream()
						.map(key -> Tuple.tuple(key, processedgroupbykey.get(key)))
						.collect(Collectors.toCollection(ArrayList::new));
				kryo.writeClassAndObject(output, out);
			} else if (!mapgpbykey.isEmpty()) {
				var result = (Map) mapgpbykey.parallelStream().flatMap(map1 -> map1.entrySet().parallelStream())
						.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors
								.mapping((Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
				var out = (List<Tuple2>) result.keySet().parallelStream().map(key -> Tuple.tuple(key, result.get(key)))
						.collect(Collectors.toCollection(ArrayList::new));
				result.keySet().parallelStream().forEach(key -> out.add(Tuple.tuple(key, result.get(key))));
				kryo.writeClassAndObject(output, out);
			}
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processGroupByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Group By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Group By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSGROUPBYKEY, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSGROUPBYKEY, ex);
		}
	}

	/**
	 * Fold by key pair operation.
	 * 
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processFoldByKeyTuple2() throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processFoldByKeyTuple2");

		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));) {
			var kryo = Utils.getKryoNonDeflateSerializer();

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
					log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
				} catch (Exception ex) {
					log.error(MassiveDataPipelineConstants.PROCESSGROUPBYKEY, ex);
					throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSGROUPBYKEY, ex);
				}
			}
			// Parallel processing of fold by key operation.
			var foldbykey = (FoldByKey) jobstage.stage.tasks.get(0);
			if (!allpairs.isEmpty()) {
				var finalfoldbykeyobj = new ArrayList<>();
				var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).parallel()
						.groupBy(tup2 -> tup2.v1, Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
				for (var key : processedgroupbykey.keySet()) {
					var seqtuple2 = Seq.of(processedgroupbykey.get(key).toArray()).parallel();
					Object foldbykeyresult;
					if (foldbykey.isLeft()) {
						foldbykeyresult = seqtuple2.foldLeft(foldbykey.getValue(), foldbykey.getReduceFunction());
					} else {
						foldbykeyresult = seqtuple2.foldRight(foldbykey.getValue(), foldbykey.getReduceFunction());
					}
					finalfoldbykeyobj.add(Tuple.tuple(key, foldbykeyresult));
				}
				kryo.writeClassAndObject(output, finalfoldbykeyobj);
			} else if (!mapgpbykey.isEmpty()) {
				var result = (Map<Object, List<Object>>) mapgpbykey.parallelStream()
						.flatMap(map1 -> map1.entrySet().parallelStream())
						.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors
								.mapping((Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
				var out = new ArrayList<>();
				result.keySet().parallelStream().forEach(key -> out.add(Tuple.tuple(key, result.get(key))));
				kryo.writeClassAndObject(output, out);
			}
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processFoldByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Fold By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Fold By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSFOLDBYKEY, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSFOLDBYKEY, ex);
		}
	}

	/**
	 * Count by key pair operation.
	 * 
	 * @param hdfs
	 * @throws Exception
	 */
	public double processCountByKeyTuple2() throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processCountByKeyTuple2");
		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));) {
			var kryo = Utils.getKryoNonDeflateSerializer();

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
			// Parallel processing of group by key operation.
			if (!allpairs.isEmpty()) {
				var processedcountbykey = (Map) allpairs.parallelStream()
						.collect(Collectors.toMap(Tuple2::v1, (Object v2) -> 1l, (a, b) -> a + b));
				var intermediatelist = (List<Tuple2>) processedcountbykey.entrySet().parallelStream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.collect(Collectors.toCollection(Vector::new));
				if (jobstage.stage.tasks.size() > 1) {
					var functions = getFunctions();
					functions.remove(0);
					intermediatelist = (List<Tuple2>) ((Stream) StreamUtils.getFunctionsToStream(functions,
							intermediatelist.parallelStream())).collect(Collectors.toCollection(Vector::new));
				}
				kryo.writeClassAndObject(output, intermediatelist);
			}
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processCountByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Count By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSCOUNTBYKEY, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSCOUNTBYKEY, ex);
		}
	}

	/**
	 * Count by key pair operation.
	 * 
	 * @param hdfs
	 * @throws Exception
	 */
	public double processCountByValueTuple2() throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processCountByValueTuple2");
		try (var fsdos = createIntermediateDataToFS(task);
				var output = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));) {
			var kryo = Utils.getKryoNonDeflateSerializer();

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
			// Parallel processing of group by key operation.
			if (!allpairs.isEmpty()) {
				var processedcountbyvalue = (Map) allpairs.parallelStream()
						.collect(Collectors.toMap(tuple2 -> tuple2, (Object v2) -> 1l, (a, b) -> a + b));
				var intermediatelist = (List<Tuple2>) processedcountbyvalue.entrySet().parallelStream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.collect(Collectors.toCollection(Vector::new));
				if (jobstage.stage.tasks.size() > 1) {
					var functions = getFunctions();
					functions.remove(0);
					intermediatelist = (List<Tuple2>) ((Stream) StreamUtils.getFunctionsToStream(functions,
							intermediatelist.parallelStream())).collect(Collectors.toCollection(Vector::new));
				}

				kryo.writeClassAndObject(output, intermediatelist);
			}
			output.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processCountByValueTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Count By Value Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Value task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSCOUNTBYVALUE, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSCOUNTBYVALUE, ex);
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
	 * @param hdfs
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processCoalesce() throws MassiveDataPipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskDExecutor.processCoalesce");
		var coalescefunction = (List<Coalesce>) getFunctions();
		try (var fsdos = createIntermediateDataToFS(task);
				var currentoutput = new Output(new SnappyOutputStream(new BufferedOutputStream(fsdos)));) {
			var kryo = Utils.getKryoNonDeflateSerializer();
			var keyvaluepairs = new ArrayList<Tuple2>();
			for (var fs : task.input) {
				try (var fsos = (InputStream) fs; Input input = new Input(new BufferedInputStream(fsos));) {
					keyvaluepairs.addAll((List) kryo.readClassAndObject(input));
				}
			}
			log.debug("Coalesce Data Size:" + keyvaluepairs.size());
			// Parallel execution of reduce by key stream execution.
			var out = keyvaluepairs.parallelStream().collect(Collectors.toMap(Tuple2::v1, Tuple2::v2,
					(input1, input2) -> coalescefunction.get(0).coalescefuncion.apply(input1, input2)));
			var outpairs = (List) out.entrySet().parallelStream()
					.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
					.collect(Collectors.toCollection(Vector::new));
			var functions = getFunctions();
			if (functions.size() > 1) {
				functions.remove(0);
				var finaltask = functions.get(functions.size() - 1);
				var stream = StreamUtils.getFunctionsToStream(functions, outpairs.parallelStream());
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
					outpairs = (List) ((Stream) stream).collect(Collectors.toCollection(Vector::new));
				}
			}
			kryo.writeClassAndObject(currentoutput, outpairs);
			currentoutput.flush();
			fsdos.flush();
			if(iscacheable) {
				cache.put(getIntermediateDataFSFilePath(task), ((ByteArrayOutputStream) fsdos).toByteArray());
				((ByteArrayOutputStream) fsdos).reset();
				ByteArrayOutputStreamPool.get().returnObject(((ByteArrayOutputStream) fsdos));
			}
			log.debug("Exiting MassiveDataStreamTaskDExecutor.processCoalesce");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Coalesce Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Value task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PROCESSCOALESCE, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PROCESSCOALESCE, ex);
		}
	}
}
