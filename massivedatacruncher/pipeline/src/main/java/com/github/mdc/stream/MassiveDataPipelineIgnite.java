package com.github.mdc.stream;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MassiveDataPipelineConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.functions.CalculateCount;
import com.github.mdc.stream.functions.Distinct;
import com.github.mdc.stream.functions.DoubleFlatMapFunction;
import com.github.mdc.stream.functions.FlatMapFunction;
import com.github.mdc.stream.functions.IntersectionFunction;
import com.github.mdc.stream.functions.JoinPredicate;
import com.github.mdc.stream.functions.KeyByFunction;
import com.github.mdc.stream.functions.LeftOuterJoinPredicate;
import com.github.mdc.stream.functions.LongFlatMapFunction;
import com.github.mdc.stream.functions.MapFunction;
import com.github.mdc.stream.functions.MapToPairFunction;
import com.github.mdc.stream.functions.PeekConsumer;
import com.github.mdc.stream.functions.PredicateSerializable;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;
import com.github.mdc.stream.functions.SToIntFunction;
import com.github.mdc.stream.functions.SortedComparator;
import com.github.mdc.stream.functions.TupleFlatMapFunction;
import com.github.mdc.stream.functions.UnionFunction;
import com.github.mdc.stream.utils.PipelineConfigValidator;

/**
 * 
 * @author arun
 * The class MassiveDataPipelineIgnite is the class for the core DataPipeline 
 * executes job in ignite server..
 * @param <I1>
 */
public final class MassiveDataPipelineIgnite<I1> extends MassiveDataIgniteCommon {
	private List<Path> filepaths = new ArrayList<>();
	private String hdfspath;
	private static Logger log = Logger.getLogger(MassiveDataPipelineIgnite.class);
	protected MassiveDataPipelineIgnite() {
		
	}
	
	/**
	 * private Constructor for MassiveDataPipelineIgnite 
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @throws MassiveDataPipelineException
	 */
	private MassiveDataPipelineIgnite(String hdfspath, String folder,PipelineConfig pipelineconfig,
			String protocol) throws MassiveDataPipelineException {
		var validator = new PipelineConfigValidator();
		var errormessages = validator.validate(pipelineconfig);
		if(!errormessages.isEmpty()) {
			var errors = new StringBuilder();
			errormessages.stream().forEach(error->errors.append(error+MDCConstants.NEWLINE));
			throw new MassiveDataPipelineException(errors.toString());
		}
		this.pipelineconfig = pipelineconfig;
		pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
		this.hdfspath = hdfspath;
		this.folder = folder;
		this.protocol = protocol;
		blocksize = Integer.parseInt(pipelineconfig.getBlocksize()) * 1024 * 1024;
	}
	
	/**
	 * The function newStreamHDFS creates Data Pipeline
	 * accepts the three params hdfs path, folder in HDFS and
	 * config object.
	 * @param hdfspath
	 * @param folder
	 * @param pipelineconfig
	 * @return MassiveDataPipelineIgnite object
	 * @throws MassiveDataPipelineException
	 */
	public static MassiveDataPipelineIgnite<String> newStreamHDFS(String hdfspath, String folder,PipelineConfig pipelineconfig) throws MassiveDataPipelineException {
		return new MassiveDataPipelineIgnite<String>(hdfspath,folder,pipelineconfig,FileSystemSupport.HDFS);
	}
	
	
	/**
	 * The function newStreamFILE creates Data Pipeline
	 * accepts the three params file path, folder in FILE and
	 * config object.
	 * @param filepath
	 * @param folder
	 * @param pipelineconfig
	 * @return MassiveDataPipelineIgnite object
	 * @throws MassiveDataPipelineException
	 */
	public static MassiveDataPipelineIgnite<String> newStreamFILE(String folder,PipelineConfig pipelineconfig) throws MassiveDataPipelineException {
		return new MassiveDataPipelineIgnite<String>(MDCConstants.NULLSTRING,folder,pipelineconfig,FileSystemSupport.FILE);
	}
	
	public static MassiveDataPipelineIgnite<String> newStream(String filepathwithscheme, PipelineConfig pipelineconfig) throws MassiveDataPipelineException {
		MassiveDataPipelineIgnite<String> mdp = null;
		URL url;
		try {
			url = new URL(filepathwithscheme);
			if (url.getProtocol().equals(FileSystemSupport.HDFS)) {
				mdp = newStreamHDFS(url.getProtocol() + MDCConstants.COLON+MDCConstants.BACKWARD_SLASH+MDCConstants.BACKWARD_SLASH+ url.getHost() + MDCConstants.COLON + url.getPort(), url.getPath(), pipelineconfig);
			} 
			else if(url.getProtocol().equals(FileSystemSupport.FILE)) {
				mdp = newStreamFILE(url.getPath(), pipelineconfig);
			}
			else {
				throw new UnsupportedOperationException(FileSystemSupport.EXCEPTIONUNSUPPORTEDFILESYSTEM); 
			}
			return mdp;
		}
		catch(Exception e) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.URISYNTAXNOTPROPER,e); 
		}
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for MapFunction.
	 * @param <T>
	 * @param root
	 * @param map
	 */
	@SuppressWarnings({ "unchecked" })
	private <T> MassiveDataPipelineIgnite(AbstractPipeline root,
			MapFunction<I1, ? extends T> map) {
		this.task = map;
		root.finaltask=task;
		this.root = root;
	}

	/**
	 * MassiveDataPipelineIgnite accepts the MapFunction.
	 * @param <T>
	 * @param map
	 * @return MassiveDataPipelineIgnite object
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings("unchecked")
	public <T> MassiveDataPipelineIgnite<T> map(MapFunction<I1 ,? extends T> map) throws MassiveDataPipelineException{
		if(Objects.isNull(map)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new MassiveDataPipelineIgnite(root,map);
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
	 * MassiveDataPipelineIgnite constructor for Peek function.
	 * @param root
	 * @param peekConsumer
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			PeekConsumer<I1> peekConsumer) {
		this.task = peekConsumer;
		this.root = root;
		root.finaltask=task;
		mdsroots.add(root);
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for count.
	 * @param root
	 * @param calculatecount
	 */
	@SuppressWarnings({ "unchecked" })
	protected MassiveDataPipelineIgnite(AbstractPipeline root,
			CalculateCount calculatecount) {
		this.task = calculatecount;
		this.root = root;
		mdsroots.add(root);
		root.finaltask=task;
	}

	/**
	 * MassiveDataPipelineIgnite constructor for filter.
	 * @param root
	 * @param predicate
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			PredicateSerializable<I1> predicate) {
		this.task = predicate;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the filter i.e predicate function.
	 * @param predicate
	 * @return MassiveDataPipelineIgnite object
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> filter(PredicateSerializable<I1> predicate) throws MassiveDataPipelineException {
		if(Objects.isNull(predicate)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PREDICATENULL);
		}
		var filter = new MassiveDataPipelineIgnite<>(root,predicate);
		this.childs.add(filter);
		filter.parents.add(this);
		return filter;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for union.
	 * @param root
	 * @param unionfunction
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			UnionFunction unionfunction) {
		this.task = unionfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the union function.
	 * @param union
	 * @return MassiveDataPipelineIgnite object
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> union(MassiveDataPipelineIgnite<I1> union) throws MassiveDataPipelineException {
		if(Objects.isNull(union)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.UNIONNULL);
		}
		var unionfunction = new UnionFunction();
		var unionchild =new  MassiveDataPipelineIgnite(root,unionfunction);
		this.childs.add(unionchild);
		unionchild.parents.add(this);
		union.childs.add(unionchild);
		unionchild.parents.add(union);
		root.mdsroots.add(this.root);
		root.mdsroots.add(union.root);
		return unionchild;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for intersection.
	 * @param root
	 * @param intersectionfunction
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			IntersectionFunction intersectionfunction) {
		this.task = intersectionfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the intersection function.
	 * @param intersection
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> intersection(MassiveDataPipelineIgnite<I1> intersection) throws MassiveDataPipelineException {
		if(Objects.isNull(intersection)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.INTERSECTIONNULL);
		}
		var intersectionfunction = new IntersectionFunction();
		var intersectionchild =new  MassiveDataPipelineIgnite(root,intersectionfunction);
		this.childs.add(intersectionchild);
		intersectionchild.parents.add(this);
		intersection.childs.add(intersectionchild);
		intersectionchild.parents.add(intersection);
		root.mdsroots.add(this.root);
		root.mdsroots.add(intersection.root);
		return intersectionchild;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the MapPair function.
	 * @param <I3>
	 * @param <I4>
	 * @param pf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <I3,I4> MapPairIgnite<I3,I4> mapToPair(MapToPairFunction<? super I1, ? extends Tuple2<I3,I4>> pf) throws MassiveDataPipelineException {
		if(Objects.isNull(pf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.MAPPAIRNULL);
		}
		var mappairignite = new MapPairIgnite(root, pf);
		this.childs.add(mappairignite);
		mappairignite.parents.add(this);
		return mappairignite;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for sample.
	 * @param root
	 * @param sampleintegersupplier
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			SampleSupplierInteger sampleintegersupplier) {
		this.task = sampleintegersupplier;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the sample function.
	 * @param numsample
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> sample(Integer numsample) throws MassiveDataPipelineException {
		if(Objects.isNull(numsample)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.SAMPLENULL);
		}
		var sampleintegersupplier = new SampleSupplierInteger(numsample);
		var samplesupplier = new MassiveDataPipelineIgnite(root,sampleintegersupplier);
		this.childs.add(samplesupplier);
		samplesupplier.parents.add(this);
		return samplesupplier;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the right outer join function.
	 * @param mappairignite
	 * @param conditionrightouterjoin
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> rightOuterjoin(MassiveDataPipelineIgnite<? extends I1> mappairignite,RightOuterJoinPredicate<? super I1,? super I1> conditionrightouterjoin) throws MassiveDataPipelineException {
		if(Objects.isNull(mappairignite)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.RIGHTOUTERJOIN);
		}
		if(Objects.isNull(conditionrightouterjoin)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.RIGHTOUTERJOINCONDITION);
		}
		var mdp = new MassiveDataPipelineIgnite(root, conditionrightouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappairignite.childs.add(mdp);
		mdp.parents.add(mappairignite);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappairignite.root);
		return mdp;
	}
	/**
	 * MassiveDataPipelineIgnite accepts the left outer join function.
	 * @param mappairignite
	 * @param conditionleftouterjoin
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> leftOuterjoin(MassiveDataPipelineIgnite<I1> mappairignite,LeftOuterJoinPredicate<I1,I1> conditionleftouterjoin) throws MassiveDataPipelineException {
		if(Objects.isNull(mappairignite)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.LEFTOUTERJOIN);
		}
		if(Objects.isNull(conditionleftouterjoin)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.LEFTOUTERJOINCONDITION);
		}
		var mdp = new MassiveDataPipelineIgnite(root, conditionleftouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappairignite.childs.add(mdp);
		mdp.parents.add(mappairignite);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappairignite.root);
		return mdp;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the inner join function.
	 * @param mappairignite
	 * @param innerjoin
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> join(MassiveDataPipelineIgnite<I1> mappairignite,JoinPredicate<I1,I1> innerjoin) throws MassiveDataPipelineException {
		if(Objects.isNull(mappairignite)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.INNERJOIN);
		}
		if(Objects.isNull(innerjoin)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.INNERJOINCONDITION);
		}
		var mdp = new MassiveDataPipelineIgnite(root, innerjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappairignite.childs.add(mdp);
		mdp.parents.add(mappairignite);
		root.mdsroots.add(this.root);
		root.mdsroots.add(mappairignite.root);
		return mdp;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for FlatMap function.
	 * @param <T>
	 * @param root
	 * @param fmf
	 */
	@SuppressWarnings({ "unchecked" })
	private <T> MassiveDataPipelineIgnite(AbstractPipeline root,
			FlatMapFunction<I1, ? extends T> fmf) {
		this.task = fmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the FlatMap function.
	 * @param <T>
	 * @param fmf
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> MassiveDataPipelineIgnite<T> flatMap(FlatMapFunction<I1, ? extends T> fmf) throws MassiveDataPipelineException {
		if(Objects.isNull(fmf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FLATMAPNULL);
		}
		var mdp = new MassiveDataPipelineIgnite(root, fmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the TupleFlatMap function.
	 * @param <I3>
	 * @param <I4>
	 * @param fmt
	 * @return MapPairIgnite object. 
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <I3,I4> MapPairIgnite<I3,I4> flatMapToTuple2(TupleFlatMapFunction<? super I1, ? extends Tuple2<I3,I4>> fmt) throws MassiveDataPipelineException {
		if(Objects.isNull(fmt)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new MapPairIgnite(root, fmt);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the TupleFlatMap function.
	 * @param fmt
	 * @return MassiveDataPipelineIgnite object. 
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MassiveDataPipelineIgnite<Tuple> flatMapToTuple(TupleFlatMapFunction<? super I1, ? extends Tuple> fmt) throws MassiveDataPipelineException {
		if(Objects.isNull(fmt)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new MassiveDataPipelineIgnite(root, fmt);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for TupleFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			TupleFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for LongFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			LongFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the LongFlatMap function.
	 * @param lfmf
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<Long> flatMapToLong(LongFlatMapFunction<I1> lfmf) throws MassiveDataPipelineException {
		if(Objects.isNull(lfmf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.LONGFLATMAPNULL);
		}
		var mdp = new MassiveDataPipelineIgnite<Long>(root, lfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for DoubleFlatMap function.
	 * @param root
	 * @param dfmf
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			DoubleFlatMapFunction<I1> dfmf) {
		this.task = dfmf;
		this.root = root;
		root.finaltask=task;
	}

	/**
	 * MassiveDataPipelineIgnite accepts the DoubleFlatMap function.
	 * @param dfmf
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public MassiveDataPipelineIgnite<Double> flatMapToDouble(DoubleFlatMapFunction<I1> dfmf) throws MassiveDataPipelineException {
		if(Objects.isNull(dfmf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.DOUBLEFLATMAPNULL);
		}
		var mdp = new MassiveDataPipelineIgnite(root, dfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the peek function.
	 * @param consumer
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> peek(PeekConsumer<I1> consumer) throws MassiveDataPipelineException  {
		if(Objects.isNull(consumer)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PEEKNULL);
		}
		var map = new MassiveDataPipelineIgnite<>(root,consumer);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for sorting function.
	 * @param root
	 * @param sortedcomparator
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			SortedComparator<I1> sortedcomparator) {
		this.task = sortedcomparator;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for RightOuterJoin function.
	 * @param mdp
	 * @param rightouterjoinpredicate
	 */
	@SuppressWarnings("unchecked")
	protected MassiveDataPipelineIgnite(MassiveDataPipelineIgnite<I1> mdp,
			RightOuterJoinPredicate<I1,I1> rightouterjoinpredicate) {
		this.task = rightouterjoinpredicate;
		this.root = mdp.root;
		mdsroots.add(mdp.root);
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for LeftOuterJoin function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param leftouterjoinpredicate
	 */
	@SuppressWarnings({ "unchecked" })
	protected <T, O1, O2> MassiveDataPipelineIgnite(MassiveDataPipelineIgnite<I1> mdp,
			LeftOuterJoinPredicate<I1,I1> leftouterjoinpredicate) {
		this.task = leftouterjoinpredicate;
		this.root = mdp.root;
		mdsroots.add(mdp.root);
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for RightOuterJoin function.
	 * @param root
	 * @param conditionrightouterjoin
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			RightOuterJoinPredicate<? super I1, ? super I1> conditionrightouterjoin) {
		this.task = conditionrightouterjoin;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for LeftOuterJoin function.
	 * @param root
	 * @param conditionleftouterjoin
	 */
	@SuppressWarnings("unchecked")
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			LeftOuterJoinPredicate<? super I1, ? super I1> conditionleftouterjoin) {
		this.task = conditionleftouterjoin;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for InnerJoin function.
	 * @param root
	 * @param join
	 */
	@SuppressWarnings("unchecked")
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			JoinPredicate<? super I1, ? super I1> join) {
		this.task = join;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the sorting function.
	 * @param sortedcomparator
	 * @return MassiveDataPipelineIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MassiveDataPipelineIgnite<I1> sorted(SortedComparator<I1> sortedcomparator) throws MassiveDataPipelineException  {
		if(Objects.isNull(sortedcomparator)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.SORTEDNULL);
		}
		var map = new MassiveDataPipelineIgnite<>(root,sortedcomparator);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for Distinct.
	 * @param root
	 * @param distinct
	 */
	@SuppressWarnings({ "unchecked" })
	private MassiveDataPipelineIgnite(AbstractPipeline root,
			Distinct distinct) {
		this.task = distinct;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the distinct.
	 * @return MassiveDataPipelineIgnite object.
	 */
	public MassiveDataPipelineIgnite<I1> distinct()  {
		var distinct = new Distinct();
		var map = new MassiveDataPipelineIgnite(root,distinct);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for ToInt function.
	 * @param root
	 * @param tointfunction
	 */
	@SuppressWarnings({ "unchecked" })
	protected MassiveDataPipelineIgnite(AbstractPipeline root,
			ToIntFunction<I1> tointfunction) {
		this.task = tointfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the SToInt function.
	 * @param tointfunction
	 * @return PipelineIntStream object.
	 * @throws MassiveDataPipelineException
	 */
	public PipelineIntStream<I1> mapToInt(SToIntFunction<I1> tointfunction) throws MassiveDataPipelineException  {
		if(Objects.isNull(tointfunction)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.MAPTOINTNULL);
		}
		var map = new PipelineIntStream<>(root, tointfunction);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}
	
	/**
	 * MassiveDataPipelineIgnite constructor for KeyBy function.
	 * @param root
	 * @param keybyfunction
	 */
	@SuppressWarnings({ "unchecked" })
	protected MassiveDataPipelineIgnite(AbstractPipeline root,
			KeyByFunction<I1,I1> keybyfunction) {
		this.task = keybyfunction;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MassiveDataPipelineIgnite accepts the KeyBy function.
	 * @param <O>
	 * @param keybyfunction
	 * @return MapPair object.
	 * @throws MassiveDataPipelineException
	 */
	public <O> MapPair<O,I1> keyBy(KeyByFunction<I1,O> keybyfunction) throws MassiveDataPipelineException  {
		if(Objects.isNull(keybyfunction)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.KEYBYNULL);
		}
		var mt = new MapPair(root,keybyfunction);
		mt.parents.add(this);
		this.childs.add(mt);
		return mt;
	}
	
	/**
	 * Terminal operation save as file.
	 * @param path
	 * @throws Throwable 
	 * @
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void saveAsTextFile(URI uri, String path) throws MassiveDataPipelineException  {
			log.debug("Caching...");
			var kryo = Utils.getKryoNonDeflateSerializer();
			var mdp = (MassiveDataPipelineIgnite)root;
			Utils.writeKryoOutput(kryo, mdp.pipelineconfig.getOutput(), "Caching...");
			var mdscollect = (MassiveDataPipelineIgnite) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);
			mdscollect.mdsroots.add(root);
			mdscollect.cacheInternal(true,uri,path);
			log.debug("Cached....");
			Utils.writeKryoOutput(kryo, mdp.pipelineconfig.getOutput(), "Cached...");
	}

	

	/**
	 * Collect result or just computes stages alone by passing the 
	 * toexecute parameter. 
	 * @param toexecute
	 * @param supplier
	 * @return
	 * @throws MassiveDataPipelineException 
	 * @
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MassiveDataPipelineIgnite<I1> cache(boolean isresults) throws MassiveDataPipelineException  {
		try {
			log.debug("Caching...");
			var kryo = Utils.getKryoNonDeflateSerializer();
			var mdp = (MassiveDataPipelineIgnite)root;
			Utils.writeKryoOutput(kryo, mdp.pipelineconfig.getOutput(), "Caching...");
			var mdscollect = (MassiveDataPipelineIgnite) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);
			mdscollect.mdsroots.add(root);
			var job = mdscollect.cacheInternal(isresults,null,null);
			log.debug("Cached....");
			Utils.writeKryoOutput(kryo, mdp.pipelineconfig.getOutput(), "Cached...");
			var mdpcached = new MassiveDataPipelineIgnite();
			mdpcached.job = job;
			mdpcached.pipelineconfig = mdp.pipelineconfig;
			return mdpcached;
		}
		catch(Exception ex) {
			log.error(MassiveDataPipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PIPELINECOLLECTERROR,ex);
		}
	}
	
	/**
	 * Collect result or just computes stages alone by passing the 
	 * toexecute parameter. 
	 * @param toexecute
	 * @param supplier
	 * @return list
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "rawtypes" })
	public List collect(boolean toexecute, IntSupplier supplier) throws MassiveDataPipelineException {
		try {
			log.debug("Caching...");
			var kryo = Utils.getKryoNonDeflateSerializer();
			var mdp = (MassiveDataPipelineIgnite) root;
			Utils.writeKryoOutput(kryo, mdp.pipelineconfig.getOutput(), "Caching...");
			var mdscollect = (MassiveDataPipelineIgnite) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);
			mdscollect.mdsroots.add(root);
			var job = mdscollect.cacheInternal(true,null,null);
			return (List) job.results;
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}
	
	/**
	 * The function count return the results of count.
	 * @param supplier
	 * @return results of count.
	 * @throws MassiveDataPipelineException
	 */
	public Object count(NumPartitions supplier) throws MassiveDataPipelineException  {
		try {
			var mdp = new MassiveDataPipelineIgnite(root, new CalculateCount());
			mdp.parents.add(this);
			this.childs.add(mdp);
			var mdscollect = (MassiveDataPipelineIgnite) root;
	
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdp.task);
			mdscollect.mdsroots.add(root);
			var job = mdscollect.cacheInternal(true,null,null);
			return (List) job.results;
		}
		catch(Exception ex) {
			log.error(MassiveDataPipelineConstants.PIPELINECOUNTERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PIPELINECOUNTERROR,ex);
		}
	}
	
	/**
	 * This function executes the forEach tasks.
	 * @param consumer
	 * @param supplier
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void forEach(Consumer<?> consumer, IntSupplier supplier) throws MassiveDataPipelineException  {
		try {
			var mdscollect = (MassiveDataPipelineIgnite) root;
			mdscollect.finaltasks.clear();
			mdscollect.finaltasks.add(mdscollect.finaltask);
	
			mdscollect.mdsroots.add(root);
			var job = mdscollect.cacheInternal(true,null,null);
			var results = (List<?>) job.results;
			results.stream().forEach((Consumer) consumer);
		}
		catch(Exception ex) {
			log.error(MassiveDataPipelineConstants.PIPELINEFOREACHERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PIPELINEFOREACHERROR,ex);
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
		return "MassiveDataPipelineIgnite [task=" + task + "]";
	}
	
}
