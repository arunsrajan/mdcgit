package com.github.mdc.stream.ignite;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.mdc.stream.IgnitePipeline;

@SuppressWarnings({"unchecked", "serial", "rawtypes"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IgnitePipelineDepth33Test extends StreamPipelineIgniteBase {

	boolean toexecute = true;
	int sum;

	@Test
	public void testMapPairSortedMapPairCount() throws Throwable {
		log.info("testMapPairSortedMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairSortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapPairForEach() throws Throwable {
		log.info("testMapPairSortedMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSortedMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairSortedMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairSortedMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairSortedMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairSortedMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairSortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairSortedMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSortedPeekCollect() throws Throwable {
		log.info("testMapPairSortedPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairSortedPeek After---------------------------------------");
	}

	@Test
	public void testMapPairSortedPeekCount() throws Throwable {
		log.info("testMapPairSortedPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairSortedPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedPeekForEach() throws Throwable {
		log.info("testMapPairSortedPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSortedPeek After---------------------------------------");
	}

	@Test
	public void testMapPairSortedSampleCollect() throws Throwable {
		log.info("testMapPairSortedSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairSortedSample After---------------------------------------");
	}

	@Test
	public void testMapPairSortedSampleCount() throws Throwable {
		log.info("testMapPairSortedSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairSortedSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedSampleForEach() throws Throwable {
		log.info("testMapPairSortedSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSortedSample After---------------------------------------");
	}

	@Test
	public void testMapPairSortedSortedCollect() throws Throwable {
		log.info("testMapPairSortedSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairSortedSorted After---------------------------------------");
	}

	@Test
	public void testMapPairSortedSortedCount() throws Throwable {
		log.info("testMapPairSortedSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairSortedSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedSortedForEach() throws Throwable {
		log.info("testMapPairSortedSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSortedSorted After---------------------------------------");
	}

	@Test
	public void testPeekFilterFilterCollect() throws Throwable {
		log.info("testPeekFilterFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterFilter After---------------------------------------");
	}

	@Test
	public void testPeekFilterFilterCount() throws Throwable {
		log.info("testPeekFilterFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterFilterForEach() throws Throwable {
		log.info("testPeekFilterFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilterFilter After---------------------------------------");
	}

	@Test
	public void testPeekFilterFlatMapCollect() throws Throwable {
		log.info("testPeekFilterFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekFilterFlatMapCount() throws Throwable {
		log.info("testPeekFilterFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterFlatMapForEach() throws Throwable {
		log.info("testPeekFilterFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapCollect() throws Throwable {
		log.info("testPeekFilterMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterMap After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapCount() throws Throwable {
		log.info("testPeekFilterMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterMapCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapForEach() throws Throwable {
		log.info("testPeekFilterMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilterMap After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapPairCollect() throws Throwable {
		log.info("testPeekFilterMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterMapPair After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapPairCount() throws Throwable {
		log.info("testPeekFilterMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapPairForEach() throws Throwable {
		log.info("testPeekFilterMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilterMapPair After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekFilterMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekFilterMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekFilterMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testPeekFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekFilterMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testPeekFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekFilterMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testPeekFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekFilterPeekCollect() throws Throwable {
		log.info("testPeekFilterPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterPeek After---------------------------------------");
	}

	@Test
	public void testPeekFilterPeekCount() throws Throwable {
		log.info("testPeekFilterPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterPeekForEach() throws Throwable {
		log.info("testPeekFilterPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilterPeek After---------------------------------------");
	}

	@Test
	public void testPeekFilterSampleCollect() throws Throwable {
		log.info("testPeekFilterSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterSample After---------------------------------------");
	}

	@Test
	public void testPeekFilterSampleCount() throws Throwable {
		log.info("testPeekFilterSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterSampleForEach() throws Throwable {
		log.info("testPeekFilterSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilterSample After---------------------------------------");
	}

	@Test
	public void testPeekFilterSortedCollect() throws Throwable {
		log.info("testPeekFilterSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterSorted After---------------------------------------");
	}

	@Test
	public void testPeekFilterSortedCount() throws Throwable {
		log.info("testPeekFilterSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterSortedForEach() throws Throwable {
		log.info("testPeekFilterSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilterSorted After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapFilterCollect() throws Throwable {
		log.info("testPeekFlatMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapFilterCount() throws Throwable {
		log.info("testPeekFlatMapFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapFilterForEach() throws Throwable {
		log.info("testPeekFlatMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapFlatMapCollect() throws Throwable {
		log.info("testPeekFlatMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapFlatMapCount() throws Throwable {
		log.info("testPeekFlatMapFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapFlatMapForEach() throws Throwable {
		log.info("testPeekFlatMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapCollect() throws Throwable {
		log.info("testPeekFlatMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapMap After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapCount() throws Throwable {
		log.info("testPeekFlatMapMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapForEach() throws Throwable {
		log.info("testPeekFlatMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekFlatMapMap After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapPairCollect() throws Throwable {
		log.info("testPeekFlatMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapPairCount() throws Throwable {
		log.info("testPeekFlatMapMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapPairForEach() throws Throwable {
		log.info("testPeekFlatMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testPeekFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekFlatMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekFlatMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekFlatMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekFlatMapMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekFlatMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapPeekCollect() throws Throwable {
		log.info("testPeekFlatMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapPeekCount() throws Throwable {
		log.info("testPeekFlatMapPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapPeekForEach() throws Throwable {
		log.info("testPeekFlatMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapSampleCollect() throws Throwable {
		log.info("testPeekFlatMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapSample After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapSampleCount() throws Throwable {
		log.info("testPeekFlatMapSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapSampleForEach() throws Throwable {
		log.info("testPeekFlatMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekFlatMapSample After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapSortedCollect() throws Throwable {
		log.info("testPeekFlatMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapSortedCount() throws Throwable {
		log.info("testPeekFlatMapSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapSortedForEach() throws Throwable {
		log.info("testPeekFlatMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testPeekMapFilterCollect() throws Throwable {
		log.info("testPeekMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekMapFilter After---------------------------------------");
	}

	@Test
	public void testPeekMapFilterCount() throws Throwable {
		log.info("testPeekMapFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekMapFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekMapFilterForEach() throws Throwable {
		log.info("testPeekMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekMapFilter After---------------------------------------");
	}

	@Test
	public void testPeekMapFlatMapCollect() throws Throwable {
		log.info("testPeekMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapFlatMapCount() throws Throwable {
		log.info("testPeekMapFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekMapFlatMapForEach() throws Throwable {
		log.info("testPeekMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapMapCollect() throws Throwable {
		log.info("testPeekMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapMap After---------------------------------------");
	}

	@Test
	public void testPeekMapMapCount() throws Throwable {
		log.info("testPeekMapMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapMapCount After---------------------------------------");
	}

	@Test
	public void testPeekMapMapForEach() throws Throwable {
		log.info("testPeekMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapMap After---------------------------------------");
	}

	@Test
	public void testPeekMapMapPairCollect() throws Throwable {
		log.info("testPeekMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapMapPairCount() throws Throwable {
		log.info("testPeekMapMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekMapMapPairForEach() throws Throwable {
		log.info("testPeekMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekMapMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPeekCollect() throws Throwable {
		log.info("testPeekMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPeek After---------------------------------------");
	}

	@Test
	public void testPeekMapPeekCount() throws Throwable {
		log.info("testPeekMapPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPeekCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPeekForEach() throws Throwable {
		log.info("testPeekMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPeek After---------------------------------------");
	}

	@Test
	public void testPeekMapSampleCollect() throws Throwable {
		log.info("testPeekMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapSample After---------------------------------------");
	}

	@Test
	public void testPeekMapSampleCount() throws Throwable {
		log.info("testPeekMapSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapSampleCount After---------------------------------------");
	}

	@Test
	public void testPeekMapSampleForEach() throws Throwable {
		log.info("testPeekMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapSample After---------------------------------------");
	}

	@Test
	public void testPeekMapSortedCollect() throws Throwable {
		log.info("testPeekMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapSorted After---------------------------------------");
	}

	@Test
	public void testPeekMapSortedCount() throws Throwable {
		log.info("testPeekMapSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapSortedCount After---------------------------------------");
	}

	@Test
	public void testPeekMapSortedForEach() throws Throwable {
		log.info("testPeekMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapSorted After---------------------------------------");
	}

	@Test
	public void testPeekMapPairFilterCollect() throws Throwable {
		log.info("testPeekMapPairFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairFilter After---------------------------------------");
	}

	@Test
	public void testPeekMapPairFilterCount() throws Throwable {
		log.info("testPeekMapPairFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairFilterForEach() throws Throwable {
		log.info("testPeekMapPairFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairFilter After---------------------------------------");
	}

	@Test
	public void testPeekMapPairFlatMapCollect() throws Throwable {
		log.info("testPeekMapPairFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairFlatMapCount() throws Throwable {
		log.info("testPeekMapPairFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairFlatMapForEach() throws Throwable {
		log.info("testPeekMapPairFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapCollect() throws Throwable {
		log.info("testPeekMapPairMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapCount() throws Throwable {
		log.info("testPeekMapPairMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapForEach() throws Throwable {
		log.info("testPeekMapPairMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapPairCollect() throws Throwable {
		log.info("testPeekMapPairMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapPairCount() throws Throwable {
		log.info("testPeekMapPairMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapPairForEach() throws Throwable {
		log.info("testPeekMapPairMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekMapPairMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekMapPairMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekMapPairMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekMapPairMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekMapPairMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairPeekCollect() throws Throwable {
		log.info("testPeekMapPairPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairPeek After---------------------------------------");
	}

	@Test
	public void testPeekMapPairPeekCount() throws Throwable {
		log.info("testPeekMapPairPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairPeekForEach() throws Throwable {
		log.info("testPeekMapPairPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairPeek After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeyFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeyPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeySampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeySortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testPeekMapPairSampleCollect() throws Throwable {
		log.info("testPeekMapPairSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairSample After---------------------------------------");
	}

	@Test
	public void testPeekMapPairSampleCount() throws Throwable {
		log.info("testPeekMapPairSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairSampleForEach() throws Throwable {
		log.info("testPeekMapPairSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairSample After---------------------------------------");
	}

	@Test
	public void testPeekMapPairSortedCollect() throws Throwable {
		log.info("testPeekMapPairSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairSorted After---------------------------------------");
	}

	@Test
	public void testPeekMapPairSortedCount() throws Throwable {
		log.info("testPeekMapPairSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairSortedForEach() throws Throwable {
		log.info("testPeekMapPairSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairSorted After---------------------------------------");
	}

	@Test
	public void testPeekPeekFilterCollect() throws Throwable {
		log.info("testPeekPeekFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekPeekFilter After---------------------------------------");
	}

	@Test
	public void testPeekPeekFilterCount() throws Throwable {
		log.info("testPeekPeekFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekPeekFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekFilterForEach() throws Throwable {
		log.info("testPeekPeekFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekPeekFilter After---------------------------------------");
	}

	@Test
	public void testPeekPeekFlatMapCollect() throws Throwable {
		log.info("testPeekPeekFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekPeekFlatMapCount() throws Throwable {
		log.info("testPeekPeekFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekFlatMapForEach() throws Throwable {
		log.info("testPeekPeekFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapCollect() throws Throwable {
		log.info("testPeekPeekMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekMap After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapCount() throws Throwable {
		log.info("testPeekPeekMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekMapCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapForEach() throws Throwable {
		log.info("testPeekPeekMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekPeekMap After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapPairCollect() throws Throwable {
		log.info("testPeekPeekMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekMapPair After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapPairCount() throws Throwable {
		log.info("testPeekPeekMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapPairForEach() throws Throwable {
		log.info("testPeekPeekMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekPeekMapPair After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekPeekMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				if (tuple2.v2 != null) {
					sum += ((List) tuple2.v2).size();
				}
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekPeekMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekPeekMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekPeekMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekPeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekPeekMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekPeekPeekCollect() throws Throwable {
		log.info("testPeekPeekPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekPeek After---------------------------------------");
	}

	@Test
	public void testPeekPeekPeekCount() throws Throwable {
		log.info("testPeekPeekPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekPeekCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekPeekForEach() throws Throwable {
		log.info("testPeekPeekPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekPeekPeek After---------------------------------------");
	}

	@Test
	public void testPeekPeekSampleCollect() throws Throwable {
		log.info("testPeekPeekSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).sample(46361)
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekSample After---------------------------------------");
	}

	@Test
	public void testPeekPeekSampleCount() throws Throwable {
		log.info("testPeekPeekSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).sample(46361)
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekSampleCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekSampleForEach() throws Throwable {
		log.info("testPeekPeekSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekPeekSample After---------------------------------------");
	}

	@Test
	public void testPeekPeekSortedCollect() throws Throwable {
		log.info("testPeekPeekSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekSorted After---------------------------------------");
	}

	@Test
	public void testPeekPeekSortedCount() throws Throwable {
		log.info("testPeekPeekSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekSortedCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekSortedForEach() throws Throwable {
		log.info("testPeekPeekSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekPeekSorted After---------------------------------------");
	}

	@Test
	public void testPeekSampleFilterCollect() throws Throwable {
		log.info("testPeekSampleFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekSampleFilter After---------------------------------------");
	}

	@Test
	public void testPeekSampleFilterCount() throws Throwable {
		log.info("testPeekSampleFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekSampleFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekSampleFilterForEach() throws Throwable {
		log.info("testPeekSampleFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekSampleFilter After---------------------------------------");
	}

	@Test
	public void testPeekSampleFlatMapCollect() throws Throwable {
		log.info("testPeekSampleFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekSampleFlatMapCount() throws Throwable {
		log.info("testPeekSampleFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekSampleFlatMapForEach() throws Throwable {
		log.info("testPeekSampleFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapCollect() throws Throwable {
		log.info("testPeekSampleMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleMap After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapCount() throws Throwable {
		log.info("testPeekSampleMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleMapCount After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapForEach() throws Throwable {
		log.info("testPeekSampleMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSampleMap After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapPairCollect() throws Throwable {
		log.info("testPeekSampleMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleMapPair After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapPairCount() throws Throwable {
		log.info("testPeekSampleMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapPairForEach() throws Throwable {
		log.info("testPeekSampleMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSampleMapPair After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekSampleMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekSampleMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekSampleMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekSampleMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekSampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekSampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekSampleMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekSamplePeekCollect() throws Throwable {
		log.info("testPeekSamplePeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).peek(val -> System.out.println(val))
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSamplePeek After---------------------------------------");
	}

	@Test
	public void testPeekSamplePeekCount() throws Throwable {
		log.info("testPeekSamplePeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).peek(val -> System.out.println(val))
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSamplePeekCount After---------------------------------------");
	}

	@Test
	public void testPeekSamplePeekForEach() throws Throwable {
		log.info("testPeekSamplePeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSamplePeek After---------------------------------------");
	}

	@Test
	public void testPeekSampleSampleCollect() throws Throwable {
		log.info("testPeekSampleSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).sample(46361).collect(toexecute,
				null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleSample After---------------------------------------");
	}

	@Test
	public void testPeekSampleSampleCount() throws Throwable {
		log.info("testPeekSampleSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleSampleCount After---------------------------------------");
	}

	@Test
	public void testPeekSampleSampleForEach() throws Throwable {
		log.info("testPeekSampleSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSampleSample After---------------------------------------");
	}

	@Test
	public void testPeekSampleSortedCollect() throws Throwable {
		log.info("testPeekSampleSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleSorted After---------------------------------------");
	}

	@Test
	public void testPeekSampleSortedCount() throws Throwable {
		log.info("testPeekSampleSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleSortedCount After---------------------------------------");
	}

	@Test
	public void testPeekSampleSortedForEach() throws Throwable {
		log.info("testPeekSampleSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361)
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSampleSorted After---------------------------------------");
	}

	@Test
	public void testPeekSortedFilterCollect() throws Throwable {
		log.info("testPeekSortedFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testPeekSortedFilter After---------------------------------------");
	}

	@Test
	public void testPeekSortedFilterCount() throws Throwable {
		log.info("testPeekSortedFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testPeekSortedFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedFilterForEach() throws Throwable {
		log.info("testPeekSortedFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekSortedFilter After---------------------------------------");
	}

	@Test
	public void testPeekSortedFlatMapCollect() throws Throwable {
		log.info("testPeekSortedFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekSortedFlatMapCount() throws Throwable {
		log.info("testPeekSortedFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedFlatMapForEach() throws Throwable {
		log.info("testPeekSortedFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapCollect() throws Throwable {
		log.info("testPeekSortedMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedMap After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapCount() throws Throwable {
		log.info("testPeekSortedMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedMapCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapForEach() throws Throwable {
		log.info("testPeekSortedMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSortedMap After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapPairCollect() throws Throwable {
		log.info("testPeekSortedMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedMapPair After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapPairCount() throws Throwable {
		log.info("testPeekSortedMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapPairForEach() throws Throwable {
		log.info("testPeekSortedMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSortedMapPair After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekSortedMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekSortedMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekSortedMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testPeekSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekSortedMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testPeekSortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekSortedMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testPeekSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekSortedPeekCollect() throws Throwable {
		log.info("testPeekSortedPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedPeek After---------------------------------------");
	}

	@Test
	public void testPeekSortedPeekCount() throws Throwable {
		log.info("testPeekSortedPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedPeekCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedPeekForEach() throws Throwable {
		log.info("testPeekSortedPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSortedPeek After---------------------------------------");
	}

	@Test
	public void testPeekSortedSampleCollect() throws Throwable {
		log.info("testPeekSortedSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedSample After---------------------------------------");
	}

	@Test
	public void testPeekSortedSampleCount() throws Throwable {
		log.info("testPeekSortedSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedSampleCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedSampleForEach() throws Throwable {
		log.info("testPeekSortedSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSortedSample After---------------------------------------");
	}

	@Test
	public void testPeekSortedSortedCollect() throws Throwable {
		log.info("testPeekSortedSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedSorted After---------------------------------------");
	}

	@Test
	public void testPeekSortedSortedCount() throws Throwable {
		log.info("testPeekSortedSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
					public int compare(java.lang.String value1, java.lang.String value2) {
						return value1.compareTo(value2);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSortedSortedCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedSortedForEach() throws Throwable {
		log.info("testPeekSortedSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSortedSorted After---------------------------------------");
	}

	@Test
	public void testSampleFilterFilterCollect() throws Throwable {
		log.info("testSampleFilterFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterFilter After---------------------------------------");
	}

	@Test
	public void testSampleFilterFilterCount() throws Throwable {
		log.info("testSampleFilterFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterFilterForEach() throws Throwable {
		log.info("testSampleFilterFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilterFilter After---------------------------------------");
	}

	@Test
	public void testSampleFilterFlatMapCollect() throws Throwable {
		log.info("testSampleFilterFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleFilterFlatMapCount() throws Throwable {
		log.info("testSampleFilterFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterFlatMapForEach() throws Throwable {
		log.info("testSampleFilterFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapCollect() throws Throwable {
		log.info("testSampleFilterMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterMap After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapCount() throws Throwable {
		log.info("testSampleFilterMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterMapCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapForEach() throws Throwable {
		log.info("testSampleFilterMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilterMap After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapPairCollect() throws Throwable {
		log.info("testSampleFilterMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterMapPair After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapPairCount() throws Throwable {
		log.info("testSampleFilterMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapPairForEach() throws Throwable {
		log.info("testSampleFilterMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilterMapPair After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleFilterMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleFilterMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleFilterMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testSampleFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testSampleFilterMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testSampleFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleFilterMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testSampleFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleFilterPeekCollect() throws Throwable {
		log.info("testSampleFilterPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterPeek After---------------------------------------");
	}

	@Test
	public void testSampleFilterPeekCount() throws Throwable {
		log.info("testSampleFilterPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterPeekForEach() throws Throwable {
		log.info("testSampleFilterPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilterPeek After---------------------------------------");
	}

	@Test
	public void testSampleFilterSampleCollect() throws Throwable {
		log.info("testSampleFilterSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterSample After---------------------------------------");
	}

	@Test
	public void testSampleFilterSampleCount() throws Throwable {
		log.info("testSampleFilterSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterSampleForEach() throws Throwable {
		log.info("testSampleFilterSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilterSample After---------------------------------------");
	}

	@Test
	public void testSampleFilterSortedCollect() throws Throwable {
		log.info("testSampleFilterSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterSorted After---------------------------------------");
	}

	@Test
	public void testSampleFilterSortedCount() throws Throwable {
		log.info("testSampleFilterSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterSortedForEach() throws Throwable {
		log.info("testSampleFilterSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilterSorted After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapFilterCollect() throws Throwable {
		log.info("testSampleFlatMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapFilterCount() throws Throwable {
		log.info("testSampleFlatMapFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapFilterForEach() throws Throwable {
		log.info("testSampleFlatMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapFlatMapCollect() throws Throwable {
		log.info("testSampleFlatMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapFlatMapCount() throws Throwable {
		log.info("testSampleFlatMapFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapFlatMapForEach() throws Throwable {
		log.info("testSampleFlatMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String value) {
				return Arrays.asList(value);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapCollect() throws Throwable {
		log.info("testSampleFlatMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapMap After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapCount() throws Throwable {
		log.info("testSampleFlatMapMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapForEach() throws Throwable {
		log.info("testSampleFlatMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleFlatMapMap After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapPairCollect() throws Throwable {
		log.info("testSampleFlatMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapPairCount() throws Throwable {
		log.info("testSampleFlatMapMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapPairForEach() throws Throwable {
		log.info("testSampleFlatMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testSampleFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleFlatMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleFlatMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleFlatMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSampleFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testSampleFlatMapMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSampleFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleFlatMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapPeekCollect() throws Throwable {
		log.info("testSampleFlatMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapPeekCount() throws Throwable {
		log.info("testSampleFlatMapPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapPeekForEach() throws Throwable {
		log.info("testSampleFlatMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapSampleCollect() throws Throwable {
		log.info("testSampleFlatMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapSample After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapSampleCount() throws Throwable {
		log.info("testSampleFlatMapSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapSampleForEach() throws Throwable {
		log.info("testSampleFlatMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleFlatMapSample After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapSortedCollect() throws Throwable {
		log.info("testSampleFlatMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapSortedCount() throws Throwable {
		log.info("testSampleFlatMapSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapSortedForEach() throws Throwable {
		log.info("testSampleFlatMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361)
				.flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String, java.lang.String>() {
					public java.util.List<java.lang.String> apply(java.lang.String value) {
						return Arrays.asList(value);
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String>() {
			public int compare(java.lang.String value1, java.lang.String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testSampleMapFilterCollect() throws Throwable {
		log.info("testSampleMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testSampleMapFilter After---------------------------------------");
	}

	@Test
	public void testSampleMapFilterCount() throws Throwable {
		log.info("testSampleMapFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSampleMapFilterCount After---------------------------------------");
	}

	@Test
	public void testSampleMapFilterForEach() throws Throwable {
		log.info("testSampleMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleMapFilter After---------------------------------------");
	}

	@Test
	public void testSampleMapFlatMapCollect() throws Throwable {
		log.info("testSampleMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapFlatMapCount() throws Throwable {
		log.info("testSampleMapFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSampleMapFlatMapForEach() throws Throwable {
		log.info("testSampleMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).flatMap(new com.github.mdc.stream.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public java.util.List<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapMapCollect() throws Throwable {
		log.info("testSampleMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapMap After---------------------------------------");
	}

	@Test
	public void testSampleMapMapCount() throws Throwable {
		log.info("testSampleMapMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapMapCount After---------------------------------------");
	}

	@Test
	public void testSampleMapMapForEach() throws Throwable {
		log.info("testSampleMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapMap After---------------------------------------");
	}

	@Test
	public void testSampleMapMapPairCollect() throws Throwable {
		log.info("testSampleMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapMapPairCount() throws Throwable {
		log.info("testSampleMapMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testSampleMapMapPairForEach() throws Throwable {
		log.info("testSampleMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSampleMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testSampleMapMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSampleMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSampleMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPeekCollect() throws Throwable {
		log.info("testSampleMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPeek After---------------------------------------");
	}

	@Test
	public void testSampleMapPeekCount() throws Throwable {
		log.info("testSampleMapPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPeekCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPeekForEach() throws Throwable {
		log.info("testSampleMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPeek After---------------------------------------");
	}

	@Test
	public void testSampleMapSampleCollect() throws Throwable {
		log.info("testSampleMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapSample After---------------------------------------");
	}

	@Test
	public void testSampleMapSampleCount() throws Throwable {
		log.info("testSampleMapSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapSampleCount After---------------------------------------");
	}

	@Test
	public void testSampleMapSampleForEach() throws Throwable {
		log.info("testSampleMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapSample After---------------------------------------");
	}

	@Test
	public void testSampleMapSortedCollect() throws Throwable {
		log.info("testSampleMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapSorted After---------------------------------------");
	}

	@Test
	public void testSampleMapSortedCount() throws Throwable {
		log.info("testSampleMapSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapSortedCount After---------------------------------------");
	}

	@Test
	public void testSampleMapSortedForEach() throws Throwable {
		log.info("testSampleMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new com.github.mdc.stream.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.mdc.stream.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapSorted After---------------------------------------");
	}

	@Test
	public void testSampleMapPairFilterCollect() throws Throwable {
		log.info("testSampleMapPairFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairFilter After---------------------------------------");
	}

	@Test
	public void testSampleMapPairFilterCount() throws Throwable {
		log.info("testSampleMapPairFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairFilterForEach() throws Throwable {
		log.info("testSampleMapPairFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairFilter After---------------------------------------");
	}

	@Test
	public void testSampleMapPairFlatMapCollect() throws Throwable {
		log.info("testSampleMapPairFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairFlatMapCount() throws Throwable {
		log.info("testSampleMapPairFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairFlatMapForEach() throws Throwable {
		log.info("testSampleMapPairFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.mdc.stream.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapCollect() throws Throwable {
		log.info("testSampleMapPairMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapCount() throws Throwable {
		log.info("testSampleMapPairMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapForEach() throws Throwable {
		log.info("testSampleMapPairMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapPairCollect() throws Throwable {
		log.info("testSampleMapPairMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapPairCount() throws Throwable {
		log.info("testSampleMapPairMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapPairForEach() throws Throwable {
		log.info("testSampleMapPairMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleMapPairMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleMapPairMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleMapPairMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info("testSampleMapPairMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleMapPairMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairPeekCollect() throws Throwable {
		log.info("testSampleMapPairPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairPeek After---------------------------------------");
	}

	@Test
	public void testSampleMapPairPeekCount() throws Throwable {
		log.info("testSampleMapPairPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairPeekForEach() throws Throwable {
		log.info("testSampleMapPairPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairPeek After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeyFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new com.github.mdc.stream.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.mdc.stream.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyFilterCount After---------------------------------------");
	}
}
