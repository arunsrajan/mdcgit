package com.github.mdc.stream.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Vector;
import java.util.function.IntUnaryOperator;
import java.util.function.ToIntFunction;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.PipelineConstants;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.functions.CoalesceFunction;
import com.github.mdc.stream.functions.Distinct;
import com.github.mdc.stream.functions.DoubleFlatMapFunction;
import com.github.mdc.stream.functions.DoubleTupleFlatMapFunction;
import com.github.mdc.stream.functions.FlatMapFunction;
import com.github.mdc.stream.functions.KeyByFunction;
import com.github.mdc.stream.functions.LongFlatMapFunction;
import com.github.mdc.stream.functions.LongTupleFlatMapFunction;
import com.github.mdc.stream.functions.MapFunction;
import com.github.mdc.stream.functions.MapToPairFunction;
import com.github.mdc.stream.functions.MapValuesFunction;
import com.github.mdc.stream.functions.PeekConsumer;
import com.github.mdc.stream.functions.PredicateSerializable;
import com.github.mdc.stream.functions.ReduceByKeyFunction;
import com.github.mdc.stream.functions.ReduceByKeyFunctionValues;
import com.github.mdc.stream.functions.ReduceFunction;
import com.github.mdc.stream.functions.SortedComparator;
import com.github.mdc.stream.functions.TupleFlatMapFunction;

public class StreamUtils {
	private StreamUtils() {
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public static Object getFunctionsToStream(List functions, BaseStream stream) throws PipelineException {
		var streamparser = stream;
		for (var function : functions) {
			if (function instanceof MapFunction mf) {
				streamparser = map(mf, (Stream) streamparser);
			} else if (function instanceof MapToPairFunction mtp) {
				streamparser = map(mtp, (Stream) streamparser);
			} else if (function instanceof MapValuesFunction mv) {
				streamparser = map(mv, (Stream) streamparser);
			} else if (function instanceof PredicateSerializable ps) {
				streamparser = filter(ps, (Stream) streamparser);
			} else if (function instanceof FlatMapFunction fmf) {
				streamparser = flatMap(fmf, (Stream) streamparser);
			} else if (function instanceof TupleFlatMapFunction tfmf) {
				streamparser = flatMapToTuple(tfmf, (Stream) streamparser);
			} else if (function instanceof DoubleFlatMapFunction dfmf) {
				streamparser = flatMapToDouble(dfmf, (Stream) streamparser);
			} else if (function instanceof LongFlatMapFunction lfmf) {
				streamparser = flatMapToLong(lfmf, (Stream) streamparser);
			} else if (function instanceof ReduceFunction rf) {
				streamparser = reduce(rf, (Stream) streamparser);
			} else if (function instanceof ReduceByKeyFunction rbkf) {
				streamparser = reduce(rbkf, (Stream) streamparser);
			} else if (function instanceof ReduceByKeyFunctionValues rbkfv) {
				streamparser = reduce(rbkfv, (Stream) streamparser);
			} else if (function instanceof CoalesceFunction cf) {
				streamparser = coalesce(cf, (Stream) streamparser);
			} else if (function instanceof PeekConsumer pc) {
				streamparser = peek(pc, (Stream) streamparser);
			} else if (function instanceof SortedComparator sc) {
				streamparser = sorted(sc, (Stream) streamparser);
			} else if (function instanceof Distinct) {
				if (streamparser instanceof IntStream sp) {
					streamparser = distinct(sp);
				}
				else if (streamparser instanceof Stream sp) {
					streamparser = distinct(sp);
				}
			} else if (function instanceof ToIntFunction tif) {
				streamparser = mapToInt(tif, (Stream) streamparser);
			} else if (function instanceof KeyByFunction kbf) {
				streamparser = keyByFunction(kbf, (Stream) streamparser);
			} else if (function instanceof IntUnaryOperator iuo) {
				streamparser = map(iuo, (IntStream) streamparser);
			} else if (function instanceof LongTupleFlatMapFunction ltff) {
				streamparser = flatMapToLongTuple(ltff, (Stream) streamparser);
			} else if (function instanceof DoubleTupleFlatMapFunction dtff) {
				streamparser = flatMapToDoubleTuple(dtff, (Stream) streamparser);
			}
		}
		return streamparser;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream flatMap(FlatMapFunction flatmapfunction, Stream stream) {
		return stream.flatMap(map -> flatmapfunction.apply(map).stream());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream peek(PeekConsumer peekconsumer, Stream stream) {
		return stream.peek(peekconsumer);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream sorted(SortedComparator sortedcomparator, Stream stream) {
		return stream.sorted(sortedcomparator);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream flatMapToTuple(TupleFlatMapFunction pairflatmapfunction, Stream stream) {
		return stream.flatMap(map -> pairflatmapfunction.apply(map).stream());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream keyByFunction(KeyByFunction keybyfunction, Stream stream) {
		return stream.map(val -> new Tuple2(keybyfunction.apply(val), val));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream flatMapToDouble(DoubleFlatMapFunction doubleflatmapfunction, Stream stream) {
		return stream.flatMap(map -> doubleflatmapfunction.apply(map).stream());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream flatMapToLong(LongFlatMapFunction longflatmapfunction, Stream stream) {
		return stream.flatMap(map -> longflatmapfunction.apply(map).stream());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream flatMapToLongTuple(LongTupleFlatMapFunction longtupleflatmapfunction, Stream stream) {
		return stream.flatMap(map -> longtupleflatmapfunction.apply(map).stream());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream flatMapToDoubleTuple(DoubleTupleFlatMapFunction doubletupleflatmapfunction, Stream stream) {
		return stream.flatMap(map -> doubletupleflatmapfunction.apply(map).stream());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream map(MapFunction mapfunction, Stream stream) {
		return stream.map(mapfunction);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream map(MapToPairFunction mappairfunction, Stream stream) {
		return stream.map(mappairfunction);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream map(MapValuesFunction mapvaluesfunction, Stream<Tuple2> stream) {
		return stream.map(tuple2 -> Tuple.tuple(tuple2.v1, mapvaluesfunction.apply(tuple2.v2)));
	}


	@SuppressWarnings({"unchecked", "rawtypes"})
	private static Stream filter(PredicateSerializable predicate, Stream stream) {
		return stream.filter(predicate);
	}

	@SuppressWarnings({"rawtypes"})
	private static Stream distinct(Stream streamparser) {
		return streamparser.distinct();
	}

	private static IntStream distinct(IntStream streamparser) {
		return streamparser.distinct();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static IntStream mapToInt(ToIntFunction tointfinction, Stream stream) {
		return stream.mapToInt(tointfinction);
	}

	private static IntStream map(IntUnaryOperator intunaryoperator, IntStream stream) {
		return stream.map(intunaryoperator);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static Stream reduce(ReduceFunction reducefunction, Stream stream) throws PipelineException {
		Optional optional = stream.reduce(reducefunction);
		if (optional.isPresent()) {
			List out = Arrays.asList(optional.get());
			return out.stream();
		}
		else {
			throw new PipelineException(PipelineConstants.REDUCEEXECUTIONVALUEEMPTY);
		}
	}


	@SuppressWarnings({"rawtypes", "unchecked"})
	private static Stream reduce(ReduceByKeyFunction reducefunction, Stream<Tuple2> stream) {
		java.util.Map out = stream.collect(Collectors.toMap(Tuple2::v1, Tuple2::v2, reducefunction::apply));
		return ((List) out.entrySet().parallelStream()
				.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue())).collect(Collectors.toCollection(Vector::new))).stream();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static Stream reduce(ReduceByKeyFunctionValues reducefunctionvalues, Stream<Tuple2> stream) {
		java.util.Map out = stream.collect(Collectors.toMap(Tuple2::v1, Tuple2::v2, reducefunctionvalues::apply));
		return ((List) out.entrySet().parallelStream()
				.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue())).collect(Collectors.toCollection(Vector::new))).stream();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static Stream coalesce(CoalesceFunction coelescefunction, Stream<Tuple2> stream) {
		java.util.Map out = stream.collect(Collectors.toMap(Tuple2::v1, Tuple2::v2, coelescefunction::apply));
		return ((List) out.entrySet().stream()
				.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue())).collect(Collectors.toCollection(Vector::new))).parallelStream();
	}
}
