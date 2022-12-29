package com.github.mdc.stream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.AllDirectedPaths;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.io.ComponentNameProvider;
import org.jgrapht.io.DOTExporter;
import org.jgrapht.io.ExportException;
import org.jgrapht.io.GraphExporter;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;
import com.github.mdc.common.DAGEdge;
import com.github.mdc.common.Dummy;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.Job;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCJobMetrics;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Utils;
import com.github.mdc.common.functions.AggregateFunction;
import com.github.mdc.common.functions.AggregateReduceFunction;
import com.github.mdc.common.functions.CalculateCount;
import com.github.mdc.common.functions.Coalesce;
import com.github.mdc.common.functions.CountByKeyFunction;
import com.github.mdc.common.functions.CountByValueFunction;
import com.github.mdc.common.functions.Distinct;
import com.github.mdc.common.functions.DoubleFlatMapFunction;
import com.github.mdc.common.functions.FlatMapFunction;
import com.github.mdc.common.functions.FoldByKey;
import com.github.mdc.common.functions.GroupByKeyFunction;
import com.github.mdc.common.functions.IntersectionFunction;
import com.github.mdc.common.functions.Join;
import com.github.mdc.common.functions.JoinPredicate;
import com.github.mdc.common.functions.KeyByFunction;
import com.github.mdc.common.functions.LeftJoin;
import com.github.mdc.common.functions.LeftOuterJoinPredicate;
import com.github.mdc.common.functions.LongFlatMapFunction;
import com.github.mdc.common.functions.MapFunction;
import com.github.mdc.common.functions.MapToPairFunction;
import com.github.mdc.common.functions.PeekConsumer;
import com.github.mdc.common.functions.PipelineCoalesceFunction;
import com.github.mdc.common.functions.PredicateSerializable;
import com.github.mdc.common.functions.ReduceFunction;
import com.github.mdc.common.functions.RightJoin;
import com.github.mdc.common.functions.RightOuterJoinPredicate;
import com.github.mdc.common.functions.SToIntFunction;
import com.github.mdc.common.functions.SortedComparator;
import com.github.mdc.common.functions.TupleFlatMapFunction;
import com.github.mdc.common.functions.UnionFunction;
import com.github.mdc.stream.scheduler.StreamJobScheduler;
import com.github.mdc.stream.utils.FileBlocksPartitionerHDFS;
import com.github.mdc.stream.utils.PipelineConfigValidator;

/**
 * 
 * @author arun
 * The class MassiveDataPipeline is the class for the core DataPipeline .
 * @param <I1>
 */
public sealed class StreamPipeline<I1> extends AbstractPipeline permits CsvStream,JsonStream {
	private List<Path> filepaths = new ArrayList<>();
	protected String protocol;
	private int blocksize;
	IntSupplier supplier;
	public PipelineConfig pipelineconfig;
	private String hdfspath;
	private String folder;
	private static Logger log = Logger.getLogger(StreamPipeline.class);
	
	protected StreamPipeline() {
		
	}
	
	/**
	 * private Constructor for MassiveDataPipeline 
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @throws PipelineException
	 */
	private StreamPipeline(String hdfspath, String folder,PipelineConfig pipelineconfig) throws PipelineException {
		var validator = new PipelineConfigValidator();
		var errormessages = validator.validate(pipelineconfig);
		if(!errormessages.isEmpty()) {
			var errors = new StringBuilder();
			errormessages.stream().forEach(error->errors.append(error+MDCConstants.NEWLINE));
			throw new PipelineException(errors.toString());
		}
		this.pipelineconfig = pipelineconfig;
		pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
		this.hdfspath = hdfspath;
		this.folder = folder;
		this.protocol = FileSystemSupport.HDFS;
		blocksize = Integer.parseInt(pipelineconfig.getBlocksize()) * 1024 * 1024;
	}
	
	/**
	 * The function newStreamHDFS creates Data Pipeline
	 * accepts the three params hdfs path, folder in HDFS and
	 * config object.
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public static StreamPipeline<String> newStreamHDFS(String hdfspath, String folder,PipelineConfig pipelineconfig) throws PipelineException {
		return new StreamPipeline<String>(hdfspath,folder,pipelineconfig);
		
	}
	
	
	public static CsvStream<CSVRecord> newCsvStreamHDFS(String hdfspath, String folder,PipelineConfig pipelineconfig,String[] header) throws PipelineException {
		return new StreamPipeline<String>(hdfspath,folder,pipelineconfig).csvWithHeader(header);
		
	}
	
	public static JsonStream<JSONObject> newJsonStreamHDFS(String hdfspath, String folder,PipelineConfig pipelineconfig) throws PipelineException {
		return new StreamPipeline<String>(hdfspath,folder,pipelineconfig).toJson();
		
	}
	
	public static StreamPipeline<String> newStream(String filepathwithscheme, PipelineConfig pipelineconfig) throws PipelineException {
		StreamPipeline<String> mdp = null;
		URL url;
		try {
			url = new URL(filepathwithscheme);
			if (url.getProtocol().equals(FileSystemSupport.HDFS)) {
				mdp = newStreamHDFS(url.getProtocol() + MDCConstants.COLON+MDCConstants.FORWARD_SLASH+MDCConstants.FORWARD_SLASH+ url.getHost() + MDCConstants.COLON + url.getPort(), url.getPath(), pipelineconfig);
			} 
			return mdp;
		}
		catch(MalformedURLException use) {
			throw new PipelineException(PipelineConstants.URISYNTAXNOTPROPER,use); 
		}
	}
	
	/**
	 * Creates csv stream object
	 * @param header
	 * @return CsvStream object.
	 */
	private CsvStream<CSVRecord> csvWithHeader(String[] header) {
		return new CsvStream<>(this,new CsvOptions(header));
	}
	
	/**
	 * Creates Json stream object.
	 * @return JsonStream object
	 */
	private JsonStream<JSONObject> toJson() {
		return new JsonStream<>(this);
	}
	
	/**
	 * MassiveDataPipeline constructor for MapFunction.
	 * @param <T>
	 * @param root
	 * @param map
	 */
	
	public <T> StreamPipeline(AbstractPipeline root,
			MapFunction<I1, ? extends T> map) {
		this.task = map;
		root.finaltask=task;
		this.root = root;
	}

	/**
	 * MassiveDataPipeline accepts the MapFunction.
	 * @param <T>
	 * @param map
	 * @return MassiveDataPipeline object
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamPipeline<T> map(MapFunction<I1 ,? extends T> map) throws PipelineException{
		if(Objects.isNull(map)) {
			throw new PipelineException(PipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new StreamPipeline(root,map);
		this.childs.add(mapobj);
		mapobj.parents.add(this);
		return mapobj;
	}
	
	public List<Path> getFilepaths() {
		return filepaths;
	}
	public void setFilepaths(List<Path> filepaths) {
		this.filepaths = filepaths;
	}
	
	/**
	 * MassiveDataPipeline constructor for Peek function.
	 * @param root
	 * @param peekConsumer
	 */
	
	private StreamPipeline(AbstractPipeline root,
			PeekConsumer<I1> peekConsumer) {
		this.task = peekConsumer;
		this.root = root;
		root.finaltask=task;
		mdsroots.add(root);
	}
	
	/**
	 * MassiveDataPipeline constructor for count.
	 * @param root
	 * @param calculatecount
	 */
	
	protected StreamPipeline(AbstractPipeline root,
			CalculateCount calculatecount) {
		this.task = calculatecount;
		this.root = root;
		mdsroots.add(root);
		root.finaltask=task;
	}

	/**
	 * MassiveDataPipeline constructor for filter.
	 * @param root
	 * @param predicate
	 */
	
	private StreamPipeline(AbstractPipeline root,
			PredicateSerializable<I1> predicate) {
		this.task = predicate;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the filter i.e predicate function.
	 * @param predicate
	 * @return MassiveDataPipeline object
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> filter(PredicateSerializable<I1> predicate) throws PipelineException {
		if(Objects.isNull(predicate)) {
			throw new PipelineException(PipelineConstants.PREDICATENULL);
		}
		var filter = new StreamPipeline<>(root,predicate);
		this.childs.add(filter);
		filter.parents.add(this);
		return filter;
	}
	
	/**
	 * MassiveDataPipeline constructor for union.
	 * @param root
	 * @param unionfunction
	 */
	
	private StreamPipeline(AbstractPipeline root,
			UnionFunction unionfunction) {
		this.task = unionfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * StreamPipeline accepts the union function.
	 * @param union
	 * @return MassiveDataPipeline object
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> union(StreamPipeline<I1> union) throws PipelineException {
		if(Objects.isNull(union)) {
			throw new PipelineException(PipelineConstants.UNIONNULL);
		}
		var unionfunction = new UnionFunction();
		var unionchild =new  StreamPipeline(root,unionfunction);
		this.childs.add(unionchild);
		unionchild.parents.add(this);
		union.childs.add(unionchild);
		unionchild.parents.add(union);
		root.mdsroots.add(this.root);
		root.mdsroots.add(union.root);
		return unionchild;
	}
	
	/**
	 * MassiveDataPipeline constructor for intersection.
	 * @param root
	 * @param intersectionfunction
	 */
	
	private StreamPipeline(AbstractPipeline root,
			IntersectionFunction intersectionfunction) {
		this.task = intersectionfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the intersection function.
	 * @param intersection
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> intersection(StreamPipeline<I1> intersection) throws PipelineException {
		if(Objects.isNull(intersection)) {
			throw new PipelineException(PipelineConstants.INTERSECTIONNULL);
		}
		var intersectionfunction = new IntersectionFunction();
		var intersectionchild =new  StreamPipeline(root,intersectionfunction);
		this.childs.add(intersectionchild);
		intersectionchild.parents.add(this);
		intersection.childs.add(intersectionchild);
		intersectionchild.parents.add(intersection);
		root.mdsroots.add(this.root);
		root.mdsroots.add(intersection.root);
		return intersectionchild;
	}
	
	/**
	 * MassiveDataPipeline accepts the MapPair function.
	 * @param <I3>
	 * @param <I4>
	 * @param pf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <I3,I4> MapPair<I3,I4> mapToPair(MapToPairFunction<? super I1, ? extends Tuple2<I3,I4>> pf) throws PipelineException {
		if(Objects.isNull(pf)) {
			throw new PipelineException(PipelineConstants.MAPPAIRNULL);
		}
		var mappair = new MapPair(root, pf);
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}
	
	/**
	 * MassiveDataPipeline constructor for sample.
	 * @param root
	 * @param sampleintegersupplier
	 */
	
	private StreamPipeline(AbstractPipeline root,
			SampleSupplierInteger sampleintegersupplier) {
		this.task = sampleintegersupplier;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the sample function.
	 * @param numsample
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> sample(Integer numsample) throws PipelineException {
		if(Objects.isNull(numsample)) {
			throw new PipelineException(PipelineConstants.SAMPLENULL);
		}
		var sampleintegersupplier = new SampleSupplierInteger(numsample);
		var samplesupplier = new StreamPipeline(root,sampleintegersupplier);
		this.childs.add(samplesupplier);
		samplesupplier.parents.add(this);
		return samplesupplier;
	}
	
	/**
	 * MassiveDataPipeline accepts the right outer join function.
	 * @param mappair
	 * @param conditionrightouterjoin
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public <I2> StreamPipeline<Tuple2<I1,I2>> rightOuterjoin(StreamPipeline<? extends I2> mappair,RightOuterJoinPredicate<? super I1, ? super I2> conditionrightouterjoin) throws PipelineException {
		if(Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOIN);
		}
		if(Objects.isNull(conditionrightouterjoin)) {
			throw new PipelineException(PipelineConstants.RIGHTOUTERJOINCONDITION);
		}
		var mdp = new StreamPipeline(root, conditionrightouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappair.root);
		if(!Objects.isNull(mappair.root.mdsroots)) {
			this.root.mdsroots.addAll(mappair.root.mdsroots);
		}
		return mdp;
	}
	
	/**
	 * MassiveDataPipeline accepts the left outer join function.
	 * @param mappair
	 * @param conditionleftouterjoin
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public <I2> StreamPipeline<Tuple2<I1,I2>> leftOuterjoin(StreamPipeline<I2> mappair,
			LeftOuterJoinPredicate<I1, I2> conditionleftouterjoin) throws PipelineException {
		if (Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOIN);
		}
		if (Objects.isNull(conditionleftouterjoin)) {
			throw new PipelineException(PipelineConstants.LEFTOUTERJOINCONDITION);
		}
		StreamPipeline<Tuple2<I1, I2>> mdp = new StreamPipeline(root, conditionleftouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappair.root);
		if (!Objects.isNull(mappair.root.mdsroots)) {
			this.root.mdsroots.addAll(mappair.root.mdsroots);
		}
		return mdp;
	}
	
	/**
	 * MassiveDataPipeline accepts the inner join function.
	 * @param mappair
	 * @param innerjoin
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public <I2> StreamPipeline<Tuple2<I1,I2>> join(StreamPipeline<I2> mappair,JoinPredicate<I1,I2> innerjoin) throws PipelineException {
		if(Objects.isNull(mappair)) {
			throw new PipelineException(PipelineConstants.INNERJOIN);
		}
		if(Objects.isNull(innerjoin)) {
			throw new PipelineException(PipelineConstants.INNERJOINCONDITION);
		}
		StreamPipeline<Tuple2<I1, I2>> mdp = new StreamPipeline(root, innerjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappair.root);
		if(!Objects.isNull(mappair.root.mdsroots)) {
			this.root.mdsroots.addAll(mappair.root.mdsroots);
		}
		return mdp;
	}
	
	/**
	 * MassiveDataPipeline constructor for FlatMap function.
	 * @param <T>
	 * @param root
	 * @param fmf
	 */
	
	private <T> StreamPipeline(AbstractPipeline root,
			FlatMapFunction<I1, ? extends T> fmf) {
		this.task = fmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the FlatMap function.
	 * @param <T>
	 * @param fmf
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> StreamPipeline<T> flatMap(FlatMapFunction<I1, ? extends T> fmf) throws PipelineException {
		if(Objects.isNull(fmf)) {
			throw new PipelineException(PipelineConstants.FLATMAPNULL);
		}
		var mdp = new StreamPipeline(root, fmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipeline accepts the TupleFlatMap function.
	 * @param <I3>
	 * @param <I4>
	 * @param fmt
	 * @return MapPair object. 
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <I3,I4> MapPair<I3,I4> flatMapToTuple2(TupleFlatMapFunction<? super I1, ? extends Tuple2<I3,I4>> fmt) throws PipelineException {
		if(Objects.isNull(fmt)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new MapPair(root, fmt);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipeline accepts the TupleFlatMap function.
	 * @param fmt
	 * @return MassiveDataPipeline object. 
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public StreamPipeline<Tuple> flatMapToTuple(TupleFlatMapFunction<? super I1, ? extends Tuple> fmt) throws PipelineException {
		if(Objects.isNull(fmt)) {
			throw new PipelineException(PipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new StreamPipeline(root, fmt);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipeline constructor for TupleFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private StreamPipeline(AbstractPipeline root,
			TupleFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline constructor for LongFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private StreamPipeline(AbstractPipeline root,
			LongFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the LongFlatMap function.
	 * @param lfmf
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<Long> flatMapToLong(LongFlatMapFunction<I1> lfmf) throws PipelineException {
		if(Objects.isNull(lfmf)) {
			throw new PipelineException(PipelineConstants.LONGFLATMAPNULL);
		}
		var mdp = new StreamPipeline(root, lfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipeline constructor for DoubleFlatMap function.
	 * @param root
	 * @param dfmf
	 */
	
	private StreamPipeline(AbstractPipeline root,
			DoubleFlatMapFunction<I1> dfmf) {
		this.task = dfmf;
		this.root = root;
		root.finaltask=task;
	}

	/**
	 * MassiveDataPipeline accepts the DoubleFlatMap function.
	 * @param dfmf
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public StreamPipeline<Double> flatMapToDouble(DoubleFlatMapFunction<I1> dfmf) throws PipelineException {
		if(Objects.isNull(dfmf)) {
			throw new PipelineException(PipelineConstants.DOUBLEFLATMAPNULL);
		}
		var mdp = new StreamPipeline(root, dfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipeline accepts the peek function.
	 * @param consumer
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> peek(PeekConsumer<I1> consumer) throws PipelineException  {
		if(Objects.isNull(consumer)) {
			throw new PipelineException(PipelineConstants.PEEKNULL);
		}
		var map = new StreamPipeline<>(root,consumer);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MassiveDataPipeline constructor for sorting function.
	 * @param root
	 * @param sortedcomparator
	 */
	
	private StreamPipeline(AbstractPipeline root,
			SortedComparator<I1> sortedcomparator) {
		this.task = sortedcomparator;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline constructor for RightOuterJoin function.
	 * @param mdp
	 * @param rightouterjoinpredicate
	 */
	@SuppressWarnings("unchecked")
	protected StreamPipeline(StreamPipeline<I1> mdp,
			RightOuterJoinPredicate<I1,I1> rightouterjoinpredicate) {
		this.task = rightouterjoinpredicate;
		this.root = mdp.root;
		mdsroots.add(mdp.root);
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline constructor for LeftOuterJoin function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param leftouterjoinpredicate
	 */
	
	protected <T, O1, O2> StreamPipeline(StreamPipeline<I1> mdp,
			LeftOuterJoinPredicate<I1,I1> leftouterjoinpredicate) {
		this.task = leftouterjoinpredicate;
		this.root = mdp.root;
		mdsroots.add(mdp.root);
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline constructor for RightOuterJoin function.
	 * @param root
	 * @param conditionrightouterjoin
	 */
	
	private StreamPipeline(AbstractPipeline root,
			RightOuterJoinPredicate<? super I1, ? super I1> conditionrightouterjoin) {
		this.task = conditionrightouterjoin;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline constructor for LeftOuterJoin function.
	 * @param root
	 * @param conditionleftouterjoin
	 */
	@SuppressWarnings("unchecked")
	private StreamPipeline(AbstractPipeline root,
			LeftOuterJoinPredicate<? super I1, ? super I1> conditionleftouterjoin) {
		this.task = conditionleftouterjoin;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline constructor for InnerJoin function.
	 * @param root
	 * @param join
	 */
	@SuppressWarnings("unchecked")
	private StreamPipeline(AbstractPipeline root,
			JoinPredicate<? super I1, ? super I1> join) {
		this.task = join;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the sorting function.
	 * @param sortedcomparator
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> sorted(SortedComparator<I1> sortedcomparator) throws PipelineException  {
		if(Objects.isNull(sortedcomparator)) {
			throw new PipelineException(PipelineConstants.SORTEDNULL);
		}
		var map = new StreamPipeline<>(root,sortedcomparator);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MassiveDataPipeline constructor for Distinct.
	 * @param root
	 * @param distinct
	 */
	
	private StreamPipeline(AbstractPipeline root,
			Distinct distinct) {
		this.task = distinct;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the distinct.
	 * @return MassiveDataPipeline object.
	 */
	public StreamPipeline<I1> distinct()  {
		Distinct distinct = new Distinct();
		var map = new StreamPipeline(root,distinct);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MassiveDataPipeline constructor for ToInt function.
	 * @param root
	 * @param tointfunction
	 */
	
	protected StreamPipeline(AbstractPipeline root,
			ToIntFunction<I1> tointfunction) {
		this.task = tointfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the SToInt function.
	 * @param tointfunction
	 * @return PipelineIntStream object.
	 * @throws PipelineException
	 */
	public PipelineIntStream<I1> mapToInt(SToIntFunction<I1> tointfunction) throws PipelineException  {
		if(Objects.isNull(tointfunction)) {
			throw new PipelineException(PipelineConstants.MAPTOINTNULL);
		}
		var map = new PipelineIntStream<>(root, tointfunction);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MassiveDataPipeline constructor for KeyBy function.
	 * @param root
	 * @param keybyfunction
	 */
	protected StreamPipeline(AbstractPipeline root,
			KeyByFunction<I1,I1> keybyfunction) {
		this.task = keybyfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the KeyBy function.
	 * @param <O>
	 * @param keybyfunction
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	public <O> MapPair<O,I1> keyBy(KeyByFunction<I1,O> keybyfunction) throws PipelineException  {
		if(Objects.isNull(keybyfunction)) {
			throw new PipelineException(PipelineConstants.KEYBYNULL);
		}
		var mt = new MapPair(root,keybyfunction);
		mt.parents.add(this);
		this.childs.add(mt);
		return mt;
	}
	
	/**
	 * MassiveDataPipeline constructor for reduce function.
	 * @param root
	 * @param reduce
	 */
	protected StreamPipeline(AbstractPipeline root,
			ReduceFunction<I1> reduce) {
		this.task = reduce;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipeline accepts the Reduce function.
	 * @param reduce
	 * @return MassiveDataPipeline object.
	 * @throws PipelineException
	 */
	public StreamPipeline<I1> reduce(ReduceFunction<I1> reduce) throws PipelineException  {
		if(Objects.isNull(reduce)) {
			throw new PipelineException(PipelineConstants.KEYBYNULL);
		}
		var mdp = new StreamPipeline<I1>(root,reduce);
		mdp.parents.add(this);
		this.childs.add(mdp);
		return mdp;
	}
	
	/**
	 * StreamPipeline constructor for the Coalesce function.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	private <O1> StreamPipeline(AbstractPipeline root,
			Coalesce<I1> cf)  {
		this.task = cf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * StreamPipeline accepts the coalesce function.
	 * @param partition
	 * @param cf
	 * @return MapPair object.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public StreamPipeline<I1> coalesce(int partition,PipelineCoalesceFunction<I1> cf) throws PipelineException  {
		if(Objects.isNull(cf)) {
			throw new PipelineException(PipelineConstants.COALESCENULL);
		}
		var streampipelinecoalesce = new StreamPipeline<I1>(root, new Coalesce(partition, cf));
		this.childs.add(streampipelinecoalesce);
		streampipelinecoalesce.parents.add(this);
		return streampipelinecoalesce;
	}
	
	protected DirectedAcyclicGraph<AbstractPipeline, DAGEdge> graph = new DirectedAcyclicGraph<>(DAGEdge.class);
	
	boolean reexecutealltasks;
	private Job job = null;
	

	/**
	 * Create Job and get DAG
	 * @return
	 * @throws PipelineException 
	 * @throws ExportException 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @ 
	 */
	protected Job createJob() throws PipelineException, ExportException, IOException, URISyntaxException  {
		Job jobCreated;
		if(this.job!=null) {
			jobCreated = this.job;
		}
		else {
			jobCreated = new Job();
			jobCreated.setId(MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis()+MDCConstants.HYPHEN+Utils.getUniqueJobID());
			jobCreated.setJm(new JobMetrics());
			jobCreated.getJm().setJobstarttime(System.currentTimeMillis());
			jobCreated.getJm().setJobid(jobCreated.getId());
			PipelineConfig pipelineconfig = ((StreamPipeline)root).pipelineconfig;
			jobCreated.getJm().setMode(Boolean.parseBoolean(pipelineconfig.getYarn())?MDCConstants.YARN:Boolean.parseBoolean(pipelineconfig.getMesos())?MDCConstants.MESOS:Boolean.parseBoolean(pipelineconfig.getJgroups())?MDCConstants.JGROUPS:Boolean.parseBoolean(pipelineconfig.getLocal())?MDCConstants.LOCAL:MDCConstants.EXECMODE_DEFAULT);
			jobCreated.getJm().setJobname(pipelineconfig.getJobname());
			MDCJobMetrics.put(jobCreated.getJm());
		}
		
		getDAG(jobCreated);
		return jobCreated;
	}
	int tmptaskid = 0;
	/**
	 * Form nodes and edges and get Directed Acyclic graph 
	 * @param root
	 * @param absfunction
	 */
	protected void formDAGAbstractFunction(AbstractPipeline root, Collection<AbstractPipeline> absfunction) {
		for (var func : absfunction) {
			//Add the verted to graph. 
			graph.addVertex(func);
			//If root not null add edges between root and child nodes.
			if (root != null) {
				graph.addEdge(root, func);
			}
			//recursively form edges for root and child nodes.
			formDAGAbstractFunction(func, func.childs);
		}
	}
	private int stageid = 1;
	
	private String printTasks(List<AbstractPipeline> functions) {
		var tasksnames = functions.stream().map(absfunc->absfunc.task).collect(Collectors.toList());
		return tasksnames.toString();
	}
	private String printStages(Set<Stage> stages) {
		var stagenames = stages.stream().map(sta->sta.getId()).collect(Collectors.toList());
		return stagenames.toString();
	}
	private Set<Stage> finalstages = new LinkedHashSet<>();
	private Set<Stage> rootstages = new LinkedHashSet<>();
	Set<Object> finaltasks = new LinkedHashSet<>();
	
	/**
	 * Get Directed Acyclic graph for Map reduce API from functions graph 
	 * to stages graph.
	 * @param job
	 * @throws PipelineException 
	 * @throws ExportException 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @ 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void getDAG(Job job) throws PipelineException {
		try {
			log.debug("Induce of DAG started...");
			DirectedAcyclicGraph<Stage, DAGEdge> graphstages = null;
			Map<Object, Stage> taskstagemap = null;
			tmptaskid = 0;
			formDAGAbstractFunction(null, mdsroots);
			var absfunctions = graph.vertexSet();
			for (var absfunction : absfunctions) {
				log.debug("\n\nTasks " + absfunction);
				log.debug("[Parent] [Child]");
				log.debug(printTasks(absfunction.parents) + " , " + printTasks(absfunction.childs));
				log.debug("Task");
				log.debug(PipelineUtils.getFunctions(absfunction.task));
			}
			taskstagemap = new HashMap<>();

			graphstages = new DirectedAcyclicGraph<>(DAGEdge.class);
			rootstages.clear();
			var topoaf = new TopologicalOrderIterator<>(graph);
			while (topoaf.hasNext()) {
				var af = topoaf.next();
				// If AbstractFunction is mds then create a new stage object
				// parent and
				// child stage and form the edge between stages.
				if ((af instanceof StreamPipeline) && af.task instanceof Dummy) {
					var parentstage = new Stage();
					rootstages.add(parentstage);
					graphstages.addVertex(parentstage);
					taskstagemap.put(af.task, parentstage);
				}
				// If abstract functions parent size is greater than 0 then
				// check if the first childs size is greater than or equal to 2.
				// Create new child stage and add abstract function to child and
				// form the edges
				// between parent and child.
				else if (af.parents.size() >= 2) {
					var childstage = new Stage();
					for (var afparent : af.parents) {
						Stage parentstage = taskstagemap.get(afparent.task);
						graphstages.addVertex(parentstage);
						graphstages.addVertex(childstage);
						graphstages.addEdge(parentstage, childstage);
						childstage.parent.add(parentstage);
						parentstage.child.add(childstage);
					}
					childstage.tasks.add(af.task);
					taskstagemap.put(af.task, childstage);
				}
				// If the abstract functions are ReduceFunction,
				// GroupByKeyFunction, JoinPairFunction,
				// JoinPairFunction, AggregateReduceFunction
				// SampleSupplierInteger, SampleSupplierPartition
				// UnionFunction, IntersectionFunction
				// and if the previous tasks is not added i.e no tasks
				// are added to stage then add it to tasks of the last available
				// stage.
				else if (af.parents.size() == 1) {
					// create a new stage and add the abstract function to
					// new stage created and form the edges between last stage
					// and new stage.
					// and pushed to stack.
					if (af.parents.get(0).childs.size() >= 2) {
						var childstage = new Stage();
						for (var afparent : af.parents) {
							var parentstage = taskstagemap.get(afparent.task);
							graphstages.addVertex(parentstage);
							graphstages.addVertex(childstage);
							graphstages.addEdge(parentstage, childstage);
							childstage.parent.add(parentstage);
							parentstage.child.add(childstage);
						}
						childstage.tasks.add(af.task);
						taskstagemap.put(af.task, childstage);
					} else if ((!Objects.isNull(af.task) && (af.task instanceof Coalesce
							|| af.task instanceof GroupByKeyFunction
							|| af.task instanceof CountByKeyFunction
							|| af.task instanceof CountByValueFunction
							|| af.task instanceof JoinPredicate
							|| af.task instanceof Join
							|| af.task instanceof LeftJoin
							|| af.task instanceof RightJoin
							|| af.task instanceof LeftOuterJoinPredicate
							|| af.task instanceof RightOuterJoinPredicate
							|| af.task instanceof AggregateFunction
							|| af.task instanceof AggregateReduceFunction
							|| af.task instanceof SampleSupplierInteger
							|| af.task instanceof SampleSupplierPartition
							|| af.task instanceof UnionFunction
							|| af.task instanceof FoldByKey
							|| af.task instanceof IntersectionFunction))) {
						stageCreator(graphstages, taskstagemap, af);
					} else if (!Objects.isNull(af.parents.get(0).task)
							&& !(af.parents.get(0).task instanceof Coalesce
									|| af.parents.get(0).task instanceof GroupByKeyFunction
									|| af.parents.get(0).task instanceof CountByKeyFunction
									|| af.parents.get(0).task instanceof CountByValueFunction
									|| af.parents.get(0).task instanceof JoinPredicate
									|| af.parents.get(0).task instanceof LeftOuterJoinPredicate
									|| af.parents.get(0).task instanceof RightOuterJoinPredicate
									|| af.parents.get(0).task instanceof AggregateFunction
									|| af.parents.get(0).task instanceof AggregateReduceFunction
									|| af.parents.get(0).task instanceof SampleSupplierInteger
									|| af.parents.get(0).task instanceof SampleSupplierPartition
									|| af.parents.get(0).task instanceof UnionFunction
									|| af.parents.get(0).task instanceof FoldByKey
									|| af.parents.get(0).task instanceof IntersectionFunction
									|| af.parents.get(0).task instanceof Join
									|| af.parents.get(0).task instanceof LeftJoin
									|| af.parents.get(0).task instanceof RightJoin)) {
						var parentstage = taskstagemap.get(af.parents.get(0).task);
						parentstage.tasks.add(af.task);
						taskstagemap.put(af.task, parentstage);
					} else {
						stageCreator(graphstages, taskstagemap, af);
					}
				}
			}
			log.debug("Stages----------------------------------------");
			var stagesprocessed = graphstages.vertexSet();
			for (var stagetoprint : stagesprocessed) {
				log.info("\n\nStage " + stagetoprint.getId());
				log.debug("[Parent] [Child]");
				log.debug(printStages(stagetoprint.parent) + " , " + printStages(stagetoprint.child));
				log.info("Tasks");
				for (var task : stagetoprint.tasks) {
					log.info(PipelineUtils.getFunctions(task));
				}
			}

			finalstages.clear();
			finalstages.add(taskstagemap.get(finaltasks.iterator().next()));
			var stages = new LinkedHashSet<Stage>();
			if(rootstages.size() == 1 && finalstages.size() == 1 && rootstages.containsAll(finalstages)) {
				stages.addAll(rootstages);
			}
			else {
				// Directed paths
				var adp = new AllDirectedPaths<>(graphstages);
	
				// Get graph paths between root stage and final stage.
				List<GraphPath<Stage, DAGEdge>> graphPaths = adp.getAllPaths(rootstages, finalstages, true,
						Integer.MAX_VALUE);				
				// Collect the graph paths by getting source and target stages.
				for (var graphpath : graphPaths) {
					var dagedges = graphpath.getEdgeList();
					for (var dagedge : dagedges) {
						stages.add((Stage) dagedge.getSource());
						stages.add((Stage) dagedge.getTarget());
					}
				}
			}
			// Topological ordering of graph stages been computed so that
			// Stage of child will not be excuted not till all the parent stages
			// result been computed.
			Iterator<Stage> topostages = new TopologicalOrderIterator(graphstages);
			while (topostages.hasNext())
				job.getTopostages().add(topostages.next());
			job.getTopostages().retainAll(stages);
			var dbPartitioner = new FileBlocksPartitionerHDFS();
			dbPartitioner.getJobStageBlocks(job, supplier, ((StreamPipeline)root).protocol, rootstages, mdsroots, ((StreamPipeline)root).blocksize, ((StreamPipeline)root).pipelineconfig);
			var writer = new StringWriter();
			if (Boolean.parseBoolean((String) MDCProperties.get().get(MDCConstants.GRAPHSTOREENABLE))) {
				Utils.renderGraphStage(graphstages, writer);
			}

			if (Boolean.parseBoolean((String) MDCProperties.get().get(MDCConstants.GRAPHSTOREENABLE))) {
				writer = new StringWriter();
				renderGraph(graph, writer);
			}

			stages.clear();
			stages = null;
			log.debug("Induce of DAG ended.");
		} catch (Exception ex) {
			log.error(PipelineConstants.DAGERROR,ex);
			throw new PipelineException(PipelineConstants.DAGERROR, ex);
		}
	}

	/**
	 * The method stageCreator creates stage object and forms graph nodes
	 * and edges
	 * @param graphstages
	 * @param taskstagemap
	 * @param af
	 */
	private void stageCreator(DirectedAcyclicGraph<Stage, DAGEdge> graphstages,
	Map<Object, Stage> taskstagemap,AbstractPipeline af) {
		var parentstage = taskstagemap.get(af.parents.get(0).task);
		var childstage = new Stage();
		childstage.tasks.add(af.task);
		graphstages.addVertex(parentstage);
		graphstages.addVertex(childstage);
		graphstages.addEdge(parentstage, childstage);
		childstage.parent.add(parentstage);
		parentstage.child.add(childstage);
		taskstagemap.put(af.task, childstage);
	}
	
	
	/**
	 * The method renderGraph writes the graph information to files.
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	private static void renderGraph(Graph<AbstractPipeline, DAGEdge> graph,Writer writer) throws ExportException  {
		ComponentNameProvider<AbstractPipeline> vertexIdProvider = task -> {
			
			try {
				Thread.sleep(500);
			} catch (Exception ex) {
				log.error("Delay Error, see cause below \n",ex);
			}
			return "" + System.currentTimeMillis();
		
	};
	ComponentNameProvider<AbstractPipeline> vertexLabelProvider = AbstractPipeline::toString;
	GraphExporter<AbstractPipeline, DAGEdge> exporter = new DOTExporter<>(vertexIdProvider, vertexLabelProvider, null);
	exporter.exportGraph(graph, writer);
	var path = MDCProperties.get().getProperty(MDCConstants.GRAPDIRPATH);
	new File(path).mkdirs();
	try(var stagegraphfile = new FileWriter(path+MDCProperties.get().getProperty(MDCConstants.GRAPHTASKFILENAME)+System.currentTimeMillis());) {
		stagegraphfile.write(writer.toString());
	} catch (Exception e) {
		log.error("File Write Error, see cause below \n",e);
	}
}
	
	

	/**
	 * Terminal operation save as file.
	 * @param path
	 * @throws Throwable 
	 * @
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void saveAsTextFile(URI uri, String path) throws PipelineException,Exception  {
			var mdscollect = (StreamPipeline) root;
			if(mdscollect.finaltasks.isEmpty()) {
				mdscollect.finaltasks.add(mdscollect.finaltask);
				mdscollect.mdsroots.add(root);
			}
			var jobcreated = mdscollect.createJob();
			jobcreated.setTrigger(Job.TRIGGER.SAVERESULTSTOFILE);
			jobcreated.setUri(uri.toString());
			jobcreated.setSavepath(path);
			mdscollect.submitJob(jobcreated);			
	}

	/**
	 * Submit the job to job scheduler.
	 * @param job
	 * @return
	 * @throws Throwable 
	 * @throws Exception 
	 * @
	 */
	private Object submitJob(Job job) throws Exception  {
		var mdp = (StreamPipeline)root;
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(mdp.pipelineconfig);
		return js.schedule(job);

	}
	
	/**
	 * Collect the result which is terminal operation.
	 * @param toexecute
	 * @return
	 * @throws PipelineException 
	 * @
	 */
	@SuppressWarnings({ "rawtypes" })
	private List collect(boolean toexecute,Job.TRIGGER jobtrigger) throws PipelineException  {
		try {
			var job = createJob();
			job.setTrigger(jobtrigger);
			var results=new ArrayList();
			if(toexecute) {
				results = (ArrayList) submitJob(job);
			}
			return (List) results;
		}
		catch(Exception ex) {
			log.error(PipelineConstants.CREATEOREXECUTEJOBERROR, ex);
			throw new PipelineException(PipelineConstants.CREATEOREXECUTEJOBERROR, (Exception)ex);
		}
	}

	/**
	 * Collect result or just computes stages alone by passing the 
	 * toexecute parameter. 
	 * @param toexecute
	 * @param supplier
	 * @return list
	 * @throws PipelineException 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List collect(boolean toexecute, IntSupplier supplier) throws PipelineException  {
		try {
			log.debug("Collect task begin...");
			var mdp = (StreamPipeline)root;
			Utils.writeToOstream(mdp.pipelineconfig.getOutput(), "Collect task begin...");
			var mdscollect = (StreamPipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);
			mdscollect.mdsroots.add(root);
			if (mdscollect.supplier != null && supplier != null) {
				if (mdscollect.supplier.getAsInt()!=supplier.getAsInt()) {
					mdscollect.supplier = supplier;
					mdscollect.reexecutealltasks = true;
				} else {
					mdscollect.reexecutealltasks = false;
				}
			} else if (mdscollect.supplier == null && supplier != null) {
				mdscollect.supplier = supplier;
				mdscollect.reexecutealltasks = true;
			} else if (mdscollect.supplier == null && supplier == null) {
				mdscollect.reexecutealltasks = true;
			} else {
				mdscollect.reexecutealltasks = false;
			}		
			var result = mdscollect.collect(toexecute,Job.TRIGGER.COLLECT);
			log.debug("Collect task ended.");
			Utils.writeToOstream(mdp.pipelineconfig.getOutput(), "Collect task ended.");
			return result;
		}
		catch(Exception ex) {
			log.error(PipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOLLECTERROR,ex);
		}
	}
	
	/**
	 * The function count return the results of count.
	 * @param supplier
	 * @return result of count.
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object count(NumPartitions supplier) throws PipelineException  {
		try {
			var mdp = new StreamPipeline(root, new CalculateCount());
			mdp.parents.add(this);
			this.childs.add(mdp);
			var mdscollect = (StreamPipeline) root;
	
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdp.task);
	
			mdscollect.mdsroots.add(root);
			if (mdscollect.supplier != null && supplier != null) {
				if (mdscollect.supplier.getAsInt()!=supplier.getAsInt()) {
					mdscollect.supplier = supplier;
					mdscollect.reexecutealltasks = true;
				} else {
					mdscollect.reexecutealltasks = false;
				}
			} else if (mdscollect.supplier == null && supplier != null) {
				mdscollect.supplier = supplier;
				mdscollect.reexecutealltasks = true;
			} else if (mdscollect.supplier == null && supplier == null) {
				mdscollect.reexecutealltasks = true;
			} else {
				mdscollect.reexecutealltasks = false;
			}
			return mdscollect.collect(true,Job.TRIGGER.COUNT);
		}
		catch(Exception ex) {
			log.error(PipelineConstants.PIPELINECOUNTERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINECOUNTERROR,ex);
		}
	}
	
	/**
	 * This function executes the forEach tasks.
	 * @param consumer
	 * @param supplier
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void forEach(Consumer<?> consumer, IntSupplier supplier) throws PipelineException  {
		try {
			var mdscollect = (StreamPipeline) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);
	
			mdscollect.mdsroots.add(root);
			if (mdscollect.supplier != null && supplier != null) {
				if (mdscollect.supplier.getAsInt()!=supplier.getAsInt()) {
					mdscollect.supplier = supplier;
					mdscollect.reexecutealltasks = true;
				} else {
					mdscollect.reexecutealltasks = false;
				}
			} else if (mdscollect.supplier == null && supplier != null) {
				mdscollect.supplier = supplier;
				mdscollect.reexecutealltasks = true;
			} else if (mdscollect.supplier == null && supplier == null) {
				mdscollect.reexecutealltasks = true;
			} else {
				mdscollect.reexecutealltasks = false;
			}
			var results = mdscollect.collect(true,Job.TRIGGER.FOREACH);
			results.stream().forEach((Consumer) consumer);
		}
		catch(Exception ex) {
			log.error(PipelineConstants.PIPELINEFOREACHERROR, ex);
			throw new PipelineException(PipelineConstants.PIPELINEFOREACHERROR,ex);
		}
	}
	public String getHdfspath() {
		return hdfspath;
	}
	public String getFolder() {
		return folder;
	}
	@Override
	public String toString() {
		return "MassiveDataPipeline [task=" + task + "]";
	}
	
	
	
	
	
}
