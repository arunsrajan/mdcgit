package com.github.mdc.stream;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.Job;
import com.github.mdc.common.MassiveDataPipelineConstants;
import com.github.mdc.stream.functions.CalculateCount;
import com.github.mdc.stream.functions.Coalesce;
import com.github.mdc.stream.functions.CoalesceFunction;
import com.github.mdc.stream.functions.CountByKeyFunction;
import com.github.mdc.stream.functions.CountByValueFunction;
import com.github.mdc.stream.functions.Distinct;
import com.github.mdc.stream.functions.DoubleTupleFlatMapFunction;
import com.github.mdc.stream.functions.FlatMapFunction;
import com.github.mdc.stream.functions.FoldByKey;
import com.github.mdc.stream.functions.GroupByKeyFunction;
import com.github.mdc.stream.functions.IntersectionFunction;
import com.github.mdc.stream.functions.JoinPredicate;
import com.github.mdc.stream.functions.KeyByFunction;
import com.github.mdc.stream.functions.LeftOuterJoinPredicate;
import com.github.mdc.stream.functions.LongTupleFlatMapFunction;
import com.github.mdc.stream.functions.MapToPairFunction;
import com.github.mdc.stream.functions.MapValuesFunction;
import com.github.mdc.stream.functions.PeekConsumer;
import com.github.mdc.stream.functions.PredicateSerializable;
import com.github.mdc.stream.functions.ReduceByKeyFunction;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;
import com.github.mdc.stream.functions.SortedComparator;
import com.github.mdc.stream.functions.TupleFlatMapFunction;
import com.github.mdc.stream.functions.UnionFunction;

/**
 * 
 * @author arun
 * MapPairIgnite holding the MapPairFunction and JoinPair,
 * ReduceByKey, etc functions to run on ignite server.
 * @param <I1>
 * @param <I2>
 */
@SuppressWarnings("rawtypes")
public sealed class MapPairIgnite<I1, I2> extends MassiveDataIgniteCommon permits MapValuesIgnite {
	private static Logger log = Logger.getLogger(MapPairIgnite.class);

	public MapPairIgnite() {

	}
	
	/**
	 * MapPairIgnite constructor for rightouterjoin.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param conditionrightouterjoin
	 */
	@SuppressWarnings({ "unchecked" })
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root,
			RightOuterJoinPredicate<? super I1, ? super I2> conditionrightouterjoin) {
		this.task = conditionrightouterjoin;
		this.root = root;
		root.finaltask = task;
	}
	
	/**
	 * MapPairIgnite constructor for left outer join. 
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param conditionleftouterjoin
	 */
	@SuppressWarnings({ "unchecked" })
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root,
			LeftOuterJoinPredicate<? super I1, ? super I2> conditionleftouterjoin) {
		this.task = conditionleftouterjoin;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for keybyfunction.
	 * @param <I1>
	 * @param <I2>
	 * @param root
	 * @param keybyfunction
	 */
	@SuppressWarnings({ "unchecked", "hiding" })
	<I1, I2> MapPairIgnite(AbstractPipeline root, KeyByFunction<I1, I2> keybyfunction) {
		this.task = keybyfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts for MapvaluesIgnite
	 * @param <I3>
	 * @param <I4>
	 * @param mvf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapValuesIgnite<I1, Tuple2<I3, I4>> mapValues(MapValuesFunction<? super I2, ? extends Tuple2<I3, I4>> mvf)
			throws MassiveDataPipelineException {
		if (Objects.isNull(mvf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.MAPVALUESNULL);
		}
		var mapvalues = new MapValuesIgnite(root, mvf);
		this.childs.add(mapvalues);
		mapvalues.parents.add(this);
		return mapvalues;
	}

	/**
	 * MapPairIgnite constructor for MapPairIgnite function.
	 * @param <I3>
	 * @param <I4>
	 * @param root
	 * @param pf
	 */
	@SuppressWarnings({ "unchecked" })
	protected <I3, I4> MapPairIgnite(AbstractPipeline root,
			MapToPairFunction<? super I2, ? extends Tuple2<? super I3, ? super I4>> pf) {
		this.task = pf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite map function accepting MapPairIgniteFunction.
	 * @param <T>
	 * @param map
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <T> MapPairIgnite<T, T> map(MapToPairFunction<? super Tuple2<I1, I2>, ? extends T> map)
			throws MassiveDataPipelineException {
		if (Objects.isNull(map)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.MAPFUNCTIONNULL);
		}
		var mapobj = new MapPairIgnite(root, map);
		this.childs.add(mapobj);
		mapobj.parents.add(this);
		return mapobj;
	}

	/**
	 * MapPairIgnite constructor for Peek function
	 * @param <T>
	 * @param root
	 * @param peekConsumer
	 */
	@SuppressWarnings({ "unchecked" })
	private <T> MapPairIgnite(AbstractPipeline root, PeekConsumer peekConsumer) {
		this.task = peekConsumer;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask = task;
	}
	
	/**
	 * MapPairIgnite constructor for Join function
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param joinpredicate
	 */
	@SuppressWarnings({ "unchecked" })
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root, JoinPredicate joinpredicate) {
		this.task = joinpredicate;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for count
	 * @param root
	 * @param calculatecount
	 */
	@SuppressWarnings({ "unchecked" })
	protected MapPairIgnite(AbstractPipeline root, CalculateCount calculatecount) {
		this.task = calculatecount;
		this.root = root;
		root.mdsroots.add(root);
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts join function
	 * @param <T>
	 * @param mapright
	 * @param conditioninnerjoin
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <T> MapPairIgnite<T, T> join(AbstractPipeline mapright,
			JoinPredicate<Tuple2<I1, I2>, Tuple2<I1, I2>> conditioninnerjoin) throws MassiveDataPipelineException {
		if (Objects.isNull(mapright)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.INNERJOIN);
		}
		if (Objects.isNull(conditioninnerjoin)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.INNERJOINCONDITION);
		}
		var mp = new MapPairIgnite(root, conditioninnerjoin);
		this.childs.add(mp);
		mp.parents.add(this);
		mapright.childs.add(mp);
		mp.parents.add(mapright);
		root.mdsroots.add(mapright.root);
		return mp;
	}
	
	/**
	 * MapPairIgnite constructor for distinct 
	 * @param root
	 * @param distinct
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, Distinct distinct) {
		this.task = distinct;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts distinct
	 * @return MapPairIgnite objet
	 */
	public MapPairIgnite<I1, I2> distinct() {
		var distinct = new Distinct();
		var map = new MapPairIgnite<I1, I2>(root, distinct);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * MapPairIgnite constructor for Predicate for filter.
	 * @param root
	 * @param predicate
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, PredicateSerializable<I1> predicate) {
		this.task = predicate;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the Predicate for filter.
	 * @param predicate
	 * @return MapPairIgnite object
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I1> filter(PredicateSerializable<? super Tuple2> predicate)
			throws MassiveDataPipelineException {
		if (Objects.isNull(predicate)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PREDICATENULL);
		}
		var filter = new MapPairIgnite(root, predicate);
		this.childs.add(filter);
		filter.parents.add(this);
		return filter;
	}

	/**
	 * MapPairIgnite constructor for union.
	 * @param root
	 * @param unionfunction
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, UnionFunction unionfunction) {
		this.task = unionfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the union mappair object.
	 * @param union
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I1> union(MapPairIgnite union) throws MassiveDataPipelineException {
		if (Objects.isNull(union)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.UNIONNULL);
		}
		var unionfunction = new UnionFunction();
		var unionchild = new MapPairIgnite(root, unionfunction);
		this.childs.add(unionchild);
		unionchild.parents.add(this);
		union.childs.add(unionchild);
		unionchild.parents.add(union);
		root.mdsroots.add(union.root);
		return unionchild;
	}

	/**
	 * MapPairIgnite constructor for intersection function.
	 * @param root
	 * @param intersectionfunction
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, IntersectionFunction intersectionfunction) {
		this.task = intersectionfunction;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accept the intersection mappair object.
	 * @param intersection
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I1> intersection(MapPairIgnite intersection) throws MassiveDataPipelineException {
		if (Objects.isNull(intersection)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.INTERSECTIONNULL);
		}
		var intersectionfunction = new IntersectionFunction();
		var intersectionchild = new MapPairIgnite(root, intersectionfunction);
		this.childs.add(intersectionchild);
		intersectionchild.parents.add(this);
		intersection.childs.add(intersectionchild);
		intersectionchild.parents.add(intersection);
		root.mdsroots.add(intersection.root);
		return intersectionchild;
	}

	/**
	 * MapPairIgnite which accepts the MapPairIgnite function.
	 * @param <I3>
	 * @param <I4>
	 * @param pf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapPairIgnite<I3, I4> mapToPair(MapToPairFunction<? super Tuple2<I1, I2>, Tuple2<I3, I4>> pf)
			throws MassiveDataPipelineException {
		if (Objects.isNull(pf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.MAPPAIRNULL);
		}
		var mappair = new MapPairIgnite(root, pf);
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	/**
	 * MapPairIgnite constructor for sample.
	 * @param root
	 * @param sampleintegersupplier
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, SampleSupplierInteger sampleintegersupplier) {
		this.task = sampleintegersupplier;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the sample. 
	 * @param numsample
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> sample(Integer numsample) throws MassiveDataPipelineException {
		if (Objects.isNull(numsample)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.SAMPLENULL);
		}
		var sampleintegersupplier = new SampleSupplierInteger(numsample);
		var samplesupplier = new MapPairIgnite(root, sampleintegersupplier);
		this.childs.add(samplesupplier);
		samplesupplier.parents.add(this);
		return samplesupplier;
	}

	/**
	 * MapPairIgnite accepts the right outer join. 
	 * @param mappair
	 * @param conditionrightouterjoin
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> rightOuterjoin(AbstractPipeline mappair,
			RightOuterJoinPredicate<Tuple2<I1, I2>, Tuple2<I1, I2>> conditionrightouterjoin)
			throws MassiveDataPipelineException {
		if (Objects.isNull(mappair)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.RIGHTOUTERJOIN);
		}
		if (Objects.isNull(conditionrightouterjoin)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.RIGHTOUTERJOINCONDITION);
		}
		var mdp = new MapPairIgnite(root, conditionrightouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(mappair.root);
		return mdp;
	}

	/**
	 * MapPairIgnite accepts the left outer join.
	 * @param mappair
	 * @param conditionleftouterjoin
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> leftOuterjoin(AbstractPipeline mappair,
			LeftOuterJoinPredicate<Tuple2<I1, I2>, Tuple2<I1, I2>> conditionleftouterjoin)
			throws MassiveDataPipelineException {
		if (Objects.isNull(mappair)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.LEFTOUTERJOIN);
		}
		if (Objects.isNull(conditionleftouterjoin)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.LEFTOUTERJOINCONDITION);
		}
		var mdp = new MapPairIgnite(root, conditionleftouterjoin);
		this.childs.add(mdp);
		mdp.parents.add(this);
		mappair.childs.add(mdp);
		mdp.parents.add(mappair);
		root.mdsroots.add(mappair.root);
		return mdp;
	}

	/**
	 * MapPairIgnite constructor for FlatMap function.
	 * @param root
	 * @param fmf
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, FlatMapFunction fmf) {
		this.task = fmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the flatmap function.
	 * @param <T>
	 * @param fmf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <T> MapPairIgnite<T, T> flatMap(FlatMapFunction<? super Tuple2<I1, I2>, ? extends T> fmf)
			throws MassiveDataPipelineException {
		if (Objects.isNull(fmf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FLATMAPNULL);
		}
		var mdp = new MapPairIgnite(root, fmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * MapPairIgnite accepts the TupleFlatMap function.
	 * @param <I3>
	 * @param <I4>
	 * @param pfmf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public <I3, I4> MapPairIgnite<I3, I4> flatMapToTuple(
			TupleFlatMapFunction<? super I1, ? extends Tuple2<I3, I4>> pfmf) throws MassiveDataPipelineException {
		if (Objects.isNull(pfmf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FLATMAPPAIRNULL);
		}
		var mdp = new MapPairIgnite(root, pfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * MapPairIgnite constructor for LongTupleFlatMap function.
	 * @param root
	 * @param lfmf
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, LongTupleFlatMapFunction lfmf) {
		this.task = lfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the LongTupleFlatMap function.
	 * @param lfmf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<Long, Long> flatMapToLong(LongTupleFlatMapFunction<Tuple2<I1, I2>> lfmf)
			throws MassiveDataPipelineException {
		if (Objects.isNull(lfmf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.LONGFLATMAPNULL);
		}
		var mdp = new MapPairIgnite(root, lfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * MapPairIgnite constructor for DoubleTupleFlatMap function.
	 * @param root
	 * @param dfmf
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, DoubleTupleFlatMapFunction dfmf) {
		this.task = dfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the DoubleTupleFlatMap function.
	 * @param dfmf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<Double, Double> flatMapToDouble(DoubleTupleFlatMapFunction<Tuple2<I1, I2>> dfmf)
			throws MassiveDataPipelineException {
		if (Objects.isNull(dfmf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.DOUBLEFLATMAPNULL);
		}
		var mdp = new MapPairIgnite(root, dfmf);
		this.childs.add(mdp);
		mdp.parents.add(this);
		return mdp;
	}

	/**
	 * MapPairIgnite accepts the peek function.
	 * @param consumer
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I1> peek(PeekConsumer consumer) throws MassiveDataPipelineException {
		if (Objects.isNull(consumer)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PEEKNULL);
		}
		var map = new MapPairIgnite(root, consumer);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * MapPairIgnite constructor for the sorting.
	 * @param root
	 * @param sortedcomparator
	 */
	@SuppressWarnings({ "unchecked" })
	private MapPairIgnite(AbstractPipeline root, SortedComparator<Tuple2<I1, I2>> sortedcomparator) {
		this.task = sortedcomparator;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for the right outer join.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param rightouterjoinpredicate
	 */
	@SuppressWarnings({ "unchecked" })
	protected <T, O1, O2> MapPairIgnite(MapPairIgnite mdp, RightOuterJoinPredicate rightouterjoinpredicate) {
		this.task = rightouterjoinpredicate;
		this.root = mdp.root;
		root.mdsroots.add(mdp.root);
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for the left outer join.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param mdp
	 * @param leftouterjoinpredicate
	 */
	@SuppressWarnings({ "unchecked" })
	protected <T, O1, O2> MapPairIgnite(MapPairIgnite mdp, LeftOuterJoinPredicate leftouterjoinpredicate) {
		this.task = leftouterjoinpredicate;
		this.root = mdp.root;
		root.mdsroots.add(mdp.root);
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the sort function.
	 * @param sortedcomparator
	 * @return MapPairIgnite object
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> sorted(SortedComparator<? super Tuple2> sortedcomparator)
			throws MassiveDataPipelineException {
		if (Objects.isNull(sortedcomparator)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.SORTEDNULL);
		}
		var map = new MapPairIgnite(root, sortedcomparator);
		map.parents.add(this);
		this.childs.add(map);
		return map;
	}

	/**
	 * MapPairIgnite constructor for the Reducefunction.
	 * @param <O1>
	 * @param root
	 * @param rf
	 */
	@SuppressWarnings({ "unchecked" })
	private <O1> MapPairIgnite(AbstractPipeline root, ReduceByKeyFunction<I1> rf) {
		this.task = rf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite constructor for the Coalescefunction.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	@SuppressWarnings({ "unchecked" })
	private <O1> MapPairIgnite(AbstractPipeline root, Coalesce<I1> cf) {
		this.task = cf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the coalesce function.
	 * @param partition
	 * @param cf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> coalesce(int partition, CoalesceFunction<I2> cf) throws MassiveDataPipelineException {
		if (Objects.isNull(cf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.COALESCENULL);
		}
		var mappaircoalesce = new MapPairIgnite(root, new Coalesce(partition, cf));
		this.childs.add(mappaircoalesce);
		mappaircoalesce.parents.add(this);
		return mappaircoalesce;
	}

	/**
	 * MapPairIgnite accepts the reducefunction.
	 * @param rf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> reduceByKey(ReduceByKeyFunction<I2> rf) throws MassiveDataPipelineException {
		if (Objects.isNull(rf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.REDUCENULL);
		}
		var mappair = new MapPairIgnite(root, rf);
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	/**
	 * MapPairIgnite constructor for the fold function.
	 * @param <O1>
	 * @param root
	 * @param cf
	 */
	@SuppressWarnings({ "unchecked" })
	private <O1> MapPairIgnite(AbstractPipeline root, FoldByKey cf) {
		this.task = cf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the fold left.
	 * @param value
	 * @param rf
	 * @param partition
	 * @param cf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, I2> foldLeft(Object value, ReduceByKeyFunction<I2> rf, int partition, CoalesceFunction<I2> cf)
			throws MassiveDataPipelineException {
		if (Objects.isNull(rf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FOLDLEFTREDUCENULL);
		}
		if (Objects.isNull(cf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FOLDLEFTCOALESCENULL);
		}
		var mappair = new MapPairIgnite(root, new FoldByKey(value, rf, true));
		this.childs.add(mappair);
		mappair.parents.add(this);
		if (cf != null) {
			var mappaircoalesce = new MapPairIgnite(root, new Coalesce(partition, cf));
			mappair.childs.add(mappaircoalesce);
			mappaircoalesce.parents.add(mappair);
			return mappaircoalesce;
		}
		return mappair;
	}

	/**
	 * MapPairIgnite accepts the fold right.
	 * @param value
	 * @param rf
	 * @param partition
	 * @param cf
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MapPairIgnite<I1, I2> foldRight(Object value, ReduceByKeyFunction<I2> rf, int partition, CoalesceFunction<I2> cf)
			throws MassiveDataPipelineException {
		if (Objects.isNull(rf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FOLDRIGHTREDUCENULL);
		}
		if (Objects.isNull(cf)) {
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.FOLDRIGHTCOALESCENULL);
		}
		var mappair = new MapPairIgnite(root, new FoldByKey(value, rf, false));
		this.childs.add(mappair);
		mappair.parents.add(this);
		if (cf != null) {
			var mappaircoalesce = new MapPairIgnite(root, new Coalesce(partition, cf));
			mappair.childs.add(mappaircoalesce);
			mappaircoalesce.parents.add(mappair);
			return mappaircoalesce;
		}
		return mappair;
	}

	/**
	 * MapPairIgnite constructor for the GroupByKey function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param gbkf
	 */
	@SuppressWarnings({ "unchecked" })
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root, GroupByKeyFunction gbkf) {
		this.task = gbkf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the groupbykey.
	 * @return MapPairIgnite object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, List<I2>> groupByKey() {

		var mappair = new MapPairIgnite(root, new GroupByKeyFunction());
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	/**
	 * MapPairIgnite accepts the cogroup mappair object.
	 * @param mappair2
	 * @return MapPairIgnite object.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public MapPairIgnite<I1, Tuple2<List<I2>, List<I2>>> cogroup(MapPairIgnite<I1, I2> mappair2) throws Exception {
		MapPairIgnite<I1, List<I2>> gbkleft = this.groupByKey();
		Thread.sleep(500);
		var gbkright = mappair2.groupByKey();
		var mdp = new MapPairIgnite(root,
				(LeftOuterJoinPredicate<Tuple2<I1, List<I2>>, Tuple2<I1, List<I2>>>) ((Tuple2<I1, List<I2>> tuple1,
						Tuple2<I1, List<I2>> tuple2) -> tuple1.v1.equals(tuple2.v1)));
		gbkleft.childs.add(mdp);
		mdp.parents.add(gbkleft);
		gbkright.childs.add(mdp);
		mdp.parents.add(gbkright);
		root.mdsroots.add(gbkleft.root);
		root.mdsroots.add(gbkright.root);
		return mdp;
	}

	/**
	 * MapPairIgnite constructor for the CountByKey function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param cbkf
	 */
	@SuppressWarnings({ "unchecked" })
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root, CountByKeyFunction cbkf) {
		this.task = cbkf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the CountByKey function.
	 * @return MapPairIgnite object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I1, Long> countByKey() {
		var mappair = new MapPairIgnite(root, new CountByKeyFunction());
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	/**
	 * MapPairIgnite constructor for the CountByValue function.
	 * @param <T>
	 * @param <O1>
	 * @param <O2>
	 * @param root
	 * @param cbkf
	 */
	@SuppressWarnings({ "unchecked" })
	private <T, O1, O2> MapPairIgnite(AbstractPipeline root, CountByValueFunction cbkf) {
		this.task = cbkf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapPairIgnite accepts the CountByValue function.
	 * @return MapPairIgnite object.
	 */
	@SuppressWarnings({ "unchecked" })
	public MapPairIgnite<I2, Long> countByValue() {
		var mappair = new MapPairIgnite(root, new CountByValueFunction());
		this.childs.add(mappair);
		mappair.parents.add(this);
		return mappair;
	}

	
	/**
	 * MapPairIgnite constructor for the TupleFlatMap function.
	 * @param root
	 * @param tfmf
	 */
	@SuppressWarnings({ "unchecked" })
	protected MapPairIgnite(AbstractPipeline root, TupleFlatMapFunction<I1, Tuple> tfmf) {
		this.task = tfmf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * This function caches the data and returns MapPairIgnite object.
	 * @param isresultsneeded
	 * @return MapPairIgnite object.
	 * @throws MassiveDataPipelineException
	 */
	public MapPairIgnite<I1, I2> cache(boolean isresultsneeded) throws MassiveDataPipelineException {
		try {
			log.debug("Caching...");
			var mdpcached = new MapPairIgnite<I1, I2>();
			Job job = null;
			if (root instanceof MassiveDataPipelineIgnite mdpi) {
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(isresultsneeded,null,null);
				mdpcached.pipelineconfig = mdpi.pipelineconfig;
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(isresultsneeded,null,null);
				mdpcached.pipelineconfig = mti.pipelineconfig;
				log.debug("Cached....");
			}

			mdpcached.job = job;
			return mdpcached;
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PIPELINECOLLECTERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PIPELINECOLLECTERROR, ex);
		}
	}

	/**
	 * This method saves the result to the hdfs. 
	 * @param uri
	 * @param path
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings("unchecked")
	public void saveAsTextFile(URI uri, String path) throws MassiveDataPipelineException {
		
			if (root instanceof MassiveDataPipelineIgnite mdpi) {
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(true,uri,path);
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(true,uri,path);
				log.debug("Cached....");
			}
			
	}

	
	/**
	 * This function executes the forEach tasks.
	 * @param consumer
	 * @param supplier
	 * @throws MassiveDataPipelineException
	 */
	@SuppressWarnings("unchecked")
	public void forEach(Consumer<List<Tuple2>> consumer, IntSupplier supplier) throws MassiveDataPipelineException {
		try {
			Job job = null;
			if (root instanceof MassiveDataPipelineIgnite mdpi) {
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(true,null,null);
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(true,null,null);
				log.debug("Cached....");
			}
			var results = (List<?>) job.results;
			results.stream().forEach((Consumer) consumer);

		} catch (Exception e) {
			log.error(MassiveDataPipelineConstants.PIPELINEFOREACHERROR, e);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PIPELINEFOREACHERROR, e);
		}
	}

	/**
	 * This function executes the count tasks.
	 * @param supplier
	 * @return object
	 * @throws MassiveDataPipelineException
	 */
	public Object count(NumPartitions supplier) throws MassiveDataPipelineException {
		try {
			if (root instanceof MassiveDataPipelineIgnite mdpi) {
				var mdp = new MassiveDataPipelineIgnite(root, new CalculateCount());
				mdp.parents.add(this);
				this.childs.add(mdp);
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(true,null,null);
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				var mdp = new MapPairIgnite(root, new CalculateCount());
				mdp.parents.add(this);
				this.childs.add(mdp);
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(true,null,null);
				log.debug("Cached....");
			}
			return (List) job.results;
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PIPELINECOUNTERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PIPELINECOUNTERROR, ex);
		}
	}
	
	/**
	 * This function executes the collect tasks.
	 * @param toexecute
	 * @param supplier
	 * @return list
	 * @throws MassiveDataPipelineException
	 */
	public Object collect(boolean toexecute, IntSupplier supplier) throws MassiveDataPipelineException {
		try {
			if (root instanceof MassiveDataPipelineIgnite mdpi) {
				mdpi.finaltasks.clear();
				mdpi.finaltasks.add(mdpi.finaltask);
				mdpi.mdsroots.add(root);
				job = mdpi.cacheInternal(true,null,null);
				log.debug("Cached....");
			} else if (root instanceof MapPairIgnite mti) {
				var mdp = new MassiveDataPipelineIgnite(root, new CalculateCount());
				mdp.parents.add(this);
				this.childs.add(mdp);
				mti.finaltasks.clear();
				mti.finaltasks.add(mti.finaltask);
				mti.mdsroots.add(root);
				job = mti.cacheInternal(true,null,null);
				log.debug("Cached....");
			}
			return (List) job.results;
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.PIPELINECOUNTERROR, ex);
			throw new MassiveDataPipelineException(MassiveDataPipelineConstants.PIPELINECOUNTERROR, ex);
		}
	}
	@Override
	public String toString() {
		return "MapPairIgnite [task=" + task + "]";
	}

}
