package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVRecord;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.mdc.stream.CsvStream;
import com.github.mdc.stream.MapPair;
import com.github.mdc.stream.MassiveDataPipeline;
import com.github.mdc.stream.NumPartitions;
import com.github.mdc.stream.NumPartitionsEachFile;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MassiveDataPipelineTest extends MassiveDataPipelineBaseTestClassCommon {

	boolean toexecute = true;
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCarsReduceByKey() throws Throwable {
		log.info("testCarsReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, cars, pipelineconfig);
		MassiveDataPipeline<String[]> map = (MassiveDataPipeline<String[]>) datastream.map(dat -> dat.split("\t"))
				.filter(dat -> !dat[0].equals("make"));
		MapPair<String, Long> mappairweight = map
				.mapToPair(dat -> new Tuple2<String, Long>(dat[0], Long.parseLong(dat[8])))
				.reduceByKey((a, b) -> (Long) a + (Long) b).coalesce(1, (a, b) -> (Long) a + (Long) b);
		MapPair<String, Long> mappaircount = map.mapToPair(dat -> new Tuple2<String, Long>(dat[0], 1l))
				.reduceByKey((a, b) -> (Long) a + (Long) b).coalesce(1, (a, b) -> (Long) a + (Long) b);
		List<List<Tuple2>> result = (List) mappairweight
				.join(mappaircount, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				assertEquals(((Tuple2) pair.v1).v1, ((Tuple2) pair.v2).v1);
			}
		}

		log.info("testCarsReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCollectMultiMaps() throws Throwable {
		log.info("testCollectMultiMaps Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String[]> map = (MassiveDataPipeline<String[]>) datastream.map(dat -> dat.split(","));

		List datacollected = (List) map.collect(toexecute, null);
		Assert.assertTrue(((List) datacollected.get(0)).size() == 46361);

		List<List<Tuple2<String, Long>>> count = (List) map.mapToPair(data -> Tuple.tuple(data[8], 1l))
				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		long sum = 0l;
		for (Tuple2 pair : count.get(0)) {
			sum += (Long) pair.v2;
		}
		log.info(sum);

		Assert.assertTrue(sum == 46361);
		log.info("testCollectMultiMaps After---------------------------------------");
	}

	

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamFilterCollect() throws Throwable {
		log.info("testCsvStreamFilterCollect Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		List<List<CSVRecord>> result = (List) datastream
				.filter((record) -> !record.get("ArrDelay").equals("ArrDelay") && !record.get("ArrDelay").equals("NA")
						&& record.get("Month").equals("4") && record.get("DayofMonth").equals("13"))
				.collect(toexecute, null);
		for (CSVRecord csvrec : result.get(0)) {
			log.info(csvrec);
			assertEquals(true, csvrec.get("Month").equals("4") && csvrec.get("DayofMonth").equals("13"));
		}

		log.info("testCsvStreamFilterCollect After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamFlatMapLong() throws Throwable {
		log.info("testCsvStreamFlatMapLong Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		List<List<Long>> results = (List) datastream
				.flatMapToLong(csvrec -> Arrays
						.asList(!csvrec.get("ArrDelay").equals("NA") && !csvrec.get("ArrDelay").equals("ArrDelay")
								? Long.parseLong(csvrec.get("ArrDelay"))
								: 0))
				.collect(toexecute, null);
		long sum = results.stream().flatMap(str -> str.stream()).mapToLong(val -> val).sum();
		assertEquals(-63278, sum);
		log.info("testCsvStreamFlatMapLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamFlatMapPair() throws Throwable {
		log.info("testCsvStreamFlatMapPair Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		List<List<Tuple2>> results = (List) datastream
				.flatMapToTuple2(csvrec -> Arrays.asList(Tuple.tuple(csvrec.get("ArrDelay"), csvrec.get("DayofMonth"))))
				.collect(toexecute, null);
		double sum = results.stream().flatMap(str -> str.stream())
				.filter(t2 -> !t2.v1.equals("ArrDelay") && !t2.v1.equals("NA"))
				.mapToDouble(t2 -> Double.parseDouble((String) t2.v1)).sum();
		assertEquals(-63278.0, sum, 2);
		log.info("testCsvStreamFlatMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamIntersection() throws Throwable {
		log.info("testCsvStreamIntersection Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		CsvStream<CSVRecord, CSVRecord> csvStream = (CsvStream) datastream;
		MassiveDataPipeline filter9 = csvStream.filter(
				record -> !record.get("ArrDelay").equals("ArrDelay") && Long.parseLong(record.get("Month")) > 9l);
		MassiveDataPipeline filter11 = csvStream.filter(
				record -> !record.get("ArrDelay").equals("ArrDelay") && Long.parseLong(record.get("Month")) > 8l);
		List<List<CSVRecord>> csvrecords = (List) filter9.intersection(filter11).collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, csvrec.get("Month").equals("10") || csvrec.get("Month").equals("11")
					|| csvrec.get("Month").equals("12"));

		}
		log.info("testCsvStreamIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamIntersection10_8UnionGt10() throws Throwable {
		log.info("testCsvStreamIntersection10_8UnionGt10 Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		CsvStream<CSVRecord, CSVRecord> csvStream = (CsvStream) datastream;
		MassiveDataPipeline filter10 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 10l);
		MassiveDataPipeline filter8 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 8l);
		MassiveDataPipeline filtergt10 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) > 10l);
		List<List<CSVRecord>> csvrecords = (List) filter10.intersection(filter8).union(filtergt10).collect(toexecute,
				null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, csvrec.get("Month").equals("11") || csvrec.get("Month").equals("12"));
		}
		log.info("testCsvStreamIntersection10_8UnionGt10 After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamIntersectionIntersection() throws Throwable {
		log.info("testCsvStreamIntersectionIntersection Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		CsvStream<CSVRecord, CSVRecord> csvStream = (CsvStream) datastream;
		MassiveDataPipeline filter9 = csvStream.filter(
				record -> !record.get("ArrDelay").equals("ArrDelay") && Long.parseLong(record.get("Month")) > 9l);
		MassiveDataPipeline filter8 = csvStream.filter(
				record -> !record.get("ArrDelay").equals("ArrDelay") && Long.parseLong(record.get("Month")) > 8l);
		MassiveDataPipeline filter11 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) > 11l);
		List<List<CSVRecord>> csvrecords = (List) filter9.intersection(filter8).intersection(filter11)
				.collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, csvrec.get("Month").equals("12"));
		}
		log.info("testCsvStreamIntersectionIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamMapPair() throws Throwable {
		log.info("testCsvStreamMapPair Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		List<List<Tuple2>> results = (List) datastream
				.mapToPair(csvrec -> Tuple.tuple(csvrec.get("ArrDelay"), csvrec.get("DayofMonth")))
				.collect(toexecute, null);
		double sum = results.stream().flatMap(str -> str.stream())
				.filter(t2 -> !t2.v1.equals("ArrDelay") && !t2.v1.equals("NA"))
				.mapToDouble(t2 -> Double.parseDouble((String) t2.v1)).sum();
		assertEquals(-63278.0, sum, 2);
		log.info("testCsvStreamMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamUnion() throws Throwable {
		log.info("testCsvStreamUnion Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig, airlineheader);
		CsvStream<CSVRecord, CSVRecord> csvStream = (CsvStream) datastream;
		MassiveDataPipeline filter9 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 9l);
		MassiveDataPipeline filter8 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 8l);
		List<List<CSVRecord>> csvrecords = (List) filter9.union(filter8).collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, csvrec.get("Month").equals("9") || csvrec.get("Month").equals("8"));
		}
		log.info("testCsvStreamUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamUnion10_8Intersectiongt8() throws Throwable {
		log.info("testCsvStreamUnion10_8Intersectiongt8 Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig, airlineheader);
		CsvStream<CSVRecord, CSVRecord> csvStream = (CsvStream) datastream;
		MassiveDataPipeline filter10 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 10l);
		MassiveDataPipeline filter8 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 8l);
		MassiveDataPipeline filtergt8 = csvStream.filter(
				record -> !record.get("ArrDelay").equals("ArrDelay") && Long.parseLong(record.get("Month")) > 8l);
		List<List<CSVRecord>> csvrecords = (List) filter10.union(filter8).intersection(filtergt8).collect(toexecute,
				null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, csvrec.get("Month").equals("10"));
		}
		log.info("testCsvStreamUnion10_8Intersectiongt8 After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamUnion10_8Union8_10() throws Throwable {
		log.info("testCsvStreamUnion10_8Union8_10 Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		CsvStream<CSVRecord, CSVRecord> csvStream = (CsvStream) datastream;
		MassiveDataPipeline filter10 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 10l);
		MassiveDataPipeline filter8 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 8l);
		MassiveDataPipeline filter10_2 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 10l);
		List<List<CSVRecord>> csvrecords = (List) filter10.union(filter8).union(filter10_2).collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, csvrec.get("Month").equals("10") || csvrec.get("Month").equals("8"));
		}
		log.info("testCsvStreamUnion10_8Union8_10 After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamUnion10_8Union8_12() throws Throwable {
		log.info("testCsvStreamUnion10_8Union8_12 Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig, airlineheader);
		CsvStream<CSVRecord, CSVRecord> csvStream = (CsvStream) datastream;
		MassiveDataPipeline filter10 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 10l);
		MassiveDataPipeline filter8 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 8l);
		MassiveDataPipeline filter12 = csvStream.filter(record -> !record.get("ArrDelay").equals("ArrDelay")
				&& Long.parseLong(record.get("Month")) == 12l);
		List<List<CSVRecord>> csvrecords = (List) filter10.union(filter8).union(filter12).collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, csvrec.get("Month").equals("10") || csvrec.get("Month").equals("8")
					|| csvrec.get("Month").equals("12"));
		}
		log.info("testCsvStreamUnion10_8Union8_12 After---------------------------------------");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testFilter() throws Throwable {
		log.info("testFilter Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List data = (List) datastream.filter(dat -> dat.split(",")[2].equals("21")).collect(toexecute, null);
		log.info(data);
		Assert.assertTrue(data.size() > 0);

		log.info("testFilter After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFilterFilterFilter() throws Throwable {
		log.info("testFilterFilterFilter Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List> filterfilterresult = (List) datastream.filter(dat -> dat.split(",")[2].equals("1"))
				.filter(dat -> dat.split(",")[3].equals("1")).filter(dat -> dat.split(",")[1].equals("1"))
				.collect(true, null);
		assertTrue(filterfilterresult.get(0).size() > 0);
		log.info("testFilterFilterFilter After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testFilterMultipleSortFields() throws Throwable {
		log.info("testFilterMultipleSortFields Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline map = (MassiveDataPipeline) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !val[14].equals("ArrDelay");
		}).sorted((val1, val2) -> {
			String[] vals1 = ((String) val1).split(",");
			String[] vals2 = ((String) val2).split(",");
			Long vall1 = Long.parseLong(vals1[1]);
			Long vall2 = Long.parseLong(vals2[1]);
			int compresult = vall1.compareTo(vall2);
			if (compresult == 0) {
				vall1 = Long.parseLong(vals1[2]);
				vall2 = Long.parseLong(vals2[2]);
				compresult = vall1.compareTo(vall2);
				if (compresult == 0) {
					vall1 = Long.parseLong(vals1[3]);
					vall2 = Long.parseLong(vals2[3]);
					return vall1.compareTo(vall2);
				}
				return compresult;
			}
			return compresult;
		});
		List<List<String>> values = (List) map.collect(toexecute, null);
		for (List<String> vals : values) {
			for (String valarr : vals) {
				log.info(valarr);
			}
		}
		assertTrue(values.get(0).size() == 46360);
		log.info("testFilterMultipleSortFields After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFilterPeek() throws Throwable {
		log.info("testFilterPeek Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> values = (List) datastream.filter(value -> value.split(",")[1].equals("10"))
				.peek(System.out::println).peek(valuearr -> System.out.println(valuearr)).collect(toexecute, null);
		assertTrue(values.get(0).size() > 0);
		log.info("testFilterPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testFilterSorted() throws Throwable {
		log.info("testFilterSorted Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline map = (MassiveDataPipeline) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !val[14].equals("ArrDelay");
		}).sorted((val1, val2) -> {
			String[] vals1 = ((String) val1).split(",");
			String[] vals2 = ((String) val2).split(",");
			Long vall1 = Long.parseLong(vals1[2]);
			Long vall2 = Long.parseLong(vals2[2]);
			return vall1.compareTo(vall2);
		});
		List<List<String>> values = (List) map.collect(toexecute, null);
		for (List<String> vals : values) {
			for (String valarr : vals) {
				log.info(valarr);
			}
		}
		assertTrue(values.get(0).size() == 46360);
		log.info("testFilterSorted After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testFlatMap() throws Throwable {
		log.info("testFlatMap Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List> words = (List) datastream.flatMap(str -> Arrays.asList(str.split(" "))).collect(toexecute, null);
		for (Object word : words) {
			log.info(word);
		}
		assertTrue(words.get(0).size() > 0);
		log.info("testFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapFlatMapFlatMapFlatMap() throws Throwable {
		log.info("testFlatMapFlatMapFlatMapFlatMap Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMap(value -> {
			String values[] = value.split(",");
			return Arrays.asList(
					Long.parseLong((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).flatMap(value -> Arrays.asList(Double.valueOf(value))).flatMap(value -> Arrays.asList(value.longValue()))
				.flatMap(value -> Arrays.asList(Double.valueOf(value))).collect(toexecute, null);

		assertEquals(-63278.0,
				longs.stream().flatMap(stream -> stream.stream()).mapToDouble(val -> val.doubleValue()).sum(), 2);
		log.info("testFlatMapFlatMapFlatMapFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapFlatMapToDoubleFlatMapFlatMapToLong() throws Throwable {
		log.info("testFlatMapFlatMapToDoubleFlatMapFlatMapToLong Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMap(value -> {
			String values[] = value.split(",");
			return (List<Long>) Arrays.asList(
					Long.parseLong((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).flatMapToDouble(value -> Arrays.asList(Double.valueOf(value))).flatMap(value -> Arrays.asList(value.longValue()))
				.flatMapToLong(value -> Arrays.asList(value)).collect(toexecute, null);

		assertEquals(-63278, longs.stream().flatMap(stream -> stream.stream()).mapToLong(val -> val.longValue()).sum());
		log.info("testFlatMapFlatMapToDoubleFlatMapFlatMapToLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapPeek() throws Throwable {
		log.info("testFlatMapPeek Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> values = (List) datastream.flatMap(value -> Arrays.asList(value.split(",")))
				.peek(System.out::println).collect(toexecute, null);
		assertTrue(values.get(0).size() > 0);
		log.info("testFlatMapPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToDouble() throws Throwable {
		log.info("testFlatMapToDouble Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMapToDouble(value -> {
			String values[] = value.split(",");
			return Arrays.asList(Double
					.parseDouble((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).collect(toexecute, null);

		assertEquals(-63278,
				longs.stream().flatMap(stream -> stream.stream()).mapToLong(value -> value.longValue()).sum());
		log.info("testFlatMapToDouble After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToDoubleFlatMap() throws Throwable {
		log.info("testFlatMapToDoubleFlatMap Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMapToDouble(value -> {
			String values[] = value.split(",");
			return Arrays.asList(Double
					.parseDouble((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).flatMap(value -> Arrays.asList(value)).collect(toexecute, null);

		assertEquals(-63278,
				longs.stream().flatMap(stream -> stream.stream()).mapToLong(value -> value.longValue()).sum());
		log.info("testFlatMapToDoubleFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLong() throws Throwable {
		log.info("testFlatMapToLong Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays.asList(
					Long.parseLong((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).collect(toexecute, null);

		assertEquals(-63278,
				longs.stream().flatMap(stream -> stream.stream()).mapToLong(list -> list.longValue()).sum());
		log.info("testFlatMapToLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMap() throws Throwable {
		log.info("testFlatMapToLongFlatMap Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays.asList(
					Long.parseLong((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).flatMap(value -> Arrays.asList(value)).collect(toexecute, null);

		assertEquals(-63278,
				longs.stream().flatMap(stream -> stream.stream()).mapToLong(value -> value.longValue()).sum());
		log.info("testFlatMapToLongFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMapFlatMapFlatMap() throws Throwable {
		log.info("testFlatMapToLongFlatMapFlatMapFlatMap Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays.asList(
					Long.parseLong((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).flatMap(value -> Arrays.asList(value)).flatMap(value -> Arrays.asList(value))
				.flatMap(value -> Arrays.asList(value)).collect(toexecute, null);

		assertEquals(-63278, longs.stream().flatMap(stream -> stream.stream()).mapToLong(val -> val.longValue()).sum());
		log.info("testFlatMapToLongFlatMapFlatMapFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMapToDouble() throws Throwable {
		log.info("testFlatMapToLongFlatMapToDouble Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays.asList(
					Long.parseLong((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).flatMapToDouble(value -> Arrays.asList(Double.valueOf(value))).collect(toexecute, null);

		assertEquals(-63278.0,
				longs.stream().flatMap(stream -> stream.stream()).mapToDouble(val -> val.doubleValue()).sum(), 2);
		log.info("testFlatMapToLongFlatMapToDouble After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMapToDoubleFlatMapToLongFlatMapToDouble() throws Throwable {
		log.info(
				"testFlatMapToLongFlatMapToDoubleFlatMapToLongFlatMapToDouble Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays.asList(
					Long.parseLong((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).flatMapToDouble(value -> Arrays.asList(Double.valueOf(value)))
				.flatMapToLong(value -> Arrays.asList(value.longValue()))
				.flatMapToDouble(value -> Arrays.asList(Double.valueOf(value))).collect(toexecute, null);

		assertEquals(-63278.0,
				longs.stream().flatMap(stream -> stream.stream()).mapToDouble(val -> val.doubleValue()).sum(), 2);
		log.info(
				"testFlatMapToLongFlatMapToDoubleFlatMapToLongFlatMapToDouble After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMapToLong() throws Throwable {
		log.info("testFlatMapToLongFlatMapToLong Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays.asList(
					Long.parseLong((!values[14].equals("NA") && !values[14].equals("ArrDelay")) ? values[14] : "0"));
		}).flatMapToLong(value -> Arrays.asList(value)).collect(toexecute, null);

		assertEquals(-63278, longs.stream().flatMap(stream -> stream.stream()).mapToLong(val -> val.longValue()).sum());
		log.info("testFlatMapToLongFlatMapToLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGroupByKey() throws Throwable {
		log.info("testGroupByKey Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List output = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).groupByKey().collect(toexecute, null);
		output.stream().forEach(log::info);
		log.info(output.size());

		log.info("testGroupByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersection() throws Throwable {
		log.info("testIntersection Before---------------------------------------");
		pipelineconfig.setBlocksize("1");
		MassiveDataPipeline dataverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline dataveryverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> datas = (List) dataverysmall.intersection(dataveryverysmall).collect(true, null);
		Assert.assertEquals(46361, datas.get(0).size());
		log.info("testIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionFilterIntersection() throws Throwable {
		log.info("testIntersectionFilterIntersection Before---------------------------------------");
		MassiveDataPipeline<String> datasmall = MassiveDataPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig)
				.filter(dat -> dat.split(",")[1].equals("10") || dat.split(",")[1].equals("11"));
		MassiveDataPipeline<String> dataverysmall1987 = MassiveDataPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig)
				.filter(dat -> dat.split(",")[1].equals("10") || dat.split(",")[1].equals("11"));
		List<List<String>> datas = (List) datasmall.intersection(dataverysmall1987).collect(true, null);
		assertEquals(29, datas.get(0).size());
		log.info("testIntersectionFilterIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionIntersection() throws Throwable {
		log.info("testIntersectionIntersection Before---------------------------------------");
		MassiveDataPipeline datasmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MassiveDataPipeline dataverysmall1987 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline dataverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> datas = (List) datasmall.intersection(dataverysmall1987).intersection(dataverysmall)
				.collect(true, null);
		Assert.assertEquals(46361, datas.get(0).size());
		log.info("testIntersectionIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionIntersectionReduceByKey() throws Throwable {
		log.info("testIntersectionIntersectionReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> datasmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String> dataverysmall1987 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String> dataverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = (List) datasmall.intersection(dataverysmall1987).intersection(dataverysmall)
				.map(dat -> dat.split(",")).filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);

		assertEquals(-63278l, ((Tuple2) datas.get(0).get(0)).v2);
		log.info("testIntersectionIntersectionReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionMapIntersection() throws Throwable {
		log.info("testIntersectionMapIntersection Before---------------------------------------");
		MassiveDataPipeline<String> datasmall = MassiveDataPipeline
				.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig).map(dat -> dat);
		MassiveDataPipeline<String> dataverysmall1987 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> datas = (List) datasmall.intersection(dataverysmall1987).collect(true, null);
		datas.get(0).stream().forEach(log::info);
		Assert.assertEquals(46361, datas.get(0).size());
		log.info("testIntersectionMapIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionMapPairIntersection() throws Throwable {
		log.info("testIntersectionMapPairIntersection Before---------------------------------------");
		MassiveDataPipeline<String> mds = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinepairjoin,
				pipelineconfig);
		MapPair<Tuple, Object> dataverysmall = mds.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).mapToPair(dat -> {
					return (Tuple2) Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				});
		MapPair<Tuple, Object> datasmall = mds.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> (Tuple2) Tuple.tuple(dat[8], Long.parseLong(dat[14])));
		List<List<Tuple2>> datas = (List) dataverysmall.intersection(datasmall).collect(true, null);
		assertEquals(25, datas.get(0).size());
		log.info("testIntersectionMapPairIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionReduceByKey() throws Throwable {
		log.info("testIntersectionReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> dataverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String> dataveryverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = (List) dataverysmall.intersection(dataveryverysmall).map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);
		assertEquals(-63278l, ((Tuple2) datas.get(0).get(0)).v2);
		log.info("testIntersectionReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoin() throws Throwable {
		log.info("testJoin Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		MapPair<String, Long> mappair2 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		MapPair<String, Long> mappair3 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 - (Long) dat2).coalesce(1, (dat1, dat2) -> (Long) dat1 - (Long) dat2);

		MapPair<String, Long> mappair4 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 - (Long) dat2).coalesce(1, (dat1, dat2) -> (Long) dat1 - (Long) dat2);

		MapPair<String, Long> mappair5 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2).coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		List<List> result = (List) mappair1
				.join(mappair2, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.join(mappair3, (tuple1, tuple2) -> (((Tuple2) ((Tuple2) tuple1).v1).v1).equals(((Tuple2) tuple2).v1))
				.join(mappair4,
						(tuple1, tuple2) -> ((Tuple2) (((Tuple2) ((Tuple2) tuple1).v1).v1)).v1
								.equals(((Tuple2) tuple2).v1))
				.join(mappair5, (tuple1, tuple2) -> ((Tuple2) ((Tuple2) (((Tuple2) ((Tuple2) tuple1).v1).v1)).v1).v1
						.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				assertEquals(((Tuple2) ((Tuple2) ((Tuple2) ((Tuple2) ((Tuple2) pair.v1)).v1).v1).v1).v1,
						((Tuple2) pair.v2).v1);
			}
		}
		log.info("testJoin After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoinCommonMapMultipleReduce() throws Throwable {
		log.info("testJoinCommonMapMultipleReduce Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		MassiveDataPipeline<String> datastream1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);

		MapPair<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> result = (List) airlinesamples
				.join(carriers, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				assertEquals(((Tuple2) pair.v1).v1, ((Tuple2) pair.v2).v1);
			}
		}

		log.info("testJoinCommonMapMultipleReduce After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoinCommonMapMultipleReduceLeftOuterJoin() throws Throwable {
		log.info("testJoinCommonMapMultipleReduceLeftOuterJoin Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair airlinesamples = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1, (dat1, dat2) -> dat1 + dat2);

		MassiveDataPipeline<String> datastream1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);

		MapPair carriers = (MapPair) datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> result = (List) airlinesamples
				.leftOuterjoin(carriers, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		int sumv1 = 0;
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				if (((Tuple2) pair.v2) != null) {
					assertEquals(((Tuple2) pair.v1).v1, ((Tuple2) pair.v2).v1);
				}
				sumv1 += (Long) ((Tuple2) pair.v1).v2;
			}
		}
		assertEquals(-63278l, sumv1);

		log.info("testJoinCommonMapMultipleReduceLeftOuterJoin After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoinCommonMapMultipleReduceRightOuterJoin() throws Throwable {
		log.info("testJoinCommonMapMultipleReduceRightOuterJoin Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair airlinesample = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		MassiveDataPipeline<String> datastream1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);

		MapPair carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List<Tuple2>> result = (List) carriers
				.rightOuterjoin(airlinesample, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		int sumv1 = 0;
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				if (((Tuple2) pair.v1) != null) {
					assertEquals(((Tuple2) pair.v1).v1, ((Tuple2) pair.v2).v1);
				}
				sumv1 += (Long) ((Tuple2) pair.v2).v2;
			}
		}
		assertEquals(-63278l, sumv1);

		log.info("testJoinCommonMapMultipleReduceRightOuterJoin After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testJoinPeek() throws Throwable {
		log.info("testJoinPeek Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).peek(System.out::println)
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2).peek(System.out::println);

		MapPair mappair2 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 - (Long) dat2).coalesce(1, (dat1, dat2) -> (Long) dat1 - (Long) dat2)
				.peek(System.out::println);
		MassiveDataPipeline<String> carriersstream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);
		MapPair mappair3 = (MapPair) carriersstream.map(value -> value.split(","))
				.mapToPair(valuearr -> new Tuple2(valuearr[0].substring(1, valuearr[0].length() - 1),
						valuearr[1].substring(1, valuearr[1].length() - 1)))
				.peek(System.out::println);
		;
		List<List<Tuple2>> vals = (List) mappair1
				.join(mappair2, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.join(mappair3, (tuple1, tuple2) -> ((Tuple2) ((Tuple2) tuple1).v1).v1.equals(((Tuple2) tuple2).v1))
				.peek(System.out::println).peek(System.out::println).collect(toexecute, null);
		for (Tuple2 pair : vals.get(0)) {
			log.info(pair.v1() + " " + pair.v2());
			assertTrue(((Tuple2) ((Tuple2) pair.v1).v1).v1.equals((((Tuple2) ((Tuple2) pair).v2).v1)));
		}
		log.info("testJoinPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapCsvStream() throws Throwable {
		log.info("testMapCsvStream Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinepairjoin,
				pipelineconfig,airlineheader);
		java.util.List<List<Tuple2>> listreducebykey = (List) datastream.filter(
				dat -> dat != null && !dat.get("ArrDelay").equals("ArrDelay") && !dat.get("ArrDelay").equals("NA"))
				.map(dat -> Tuple.tuple(dat.get(8), Long.parseLong(dat.get(14)))).collect(toexecute, null);
		for (Tuple2 tuple2 : listreducebykey.get(0)) {
			log.info(tuple2);
			assertEquals("PS", tuple2.v1);
			assertEquals(true, (Long) tuple2.v2 >= -7);
		}

		log.info("testMapCsvStream After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testMapFilterSorted() throws Throwable {
		log.info("testMapFilterSorted Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline map = (MassiveDataPipeline) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay")).sorted((val1, val2) -> {
					Long vall1 = Long.parseLong(((String[]) val1)[2]);
					Long vall2 = Long.parseLong(((String[]) val2)[2]);
					return vall1.compareTo(vall2);
				});
		List<List<String[]>> values = (List) map.collect(toexecute, null);
		for (List<String[]> vals : values) {
			for (String[] valarr : vals) {
				log.info(valarr[0] + " " + valarr[1] + " " + valarr[2]);
			}
		}
		assertTrue(values.get(0).size() == 46360);
		log.info("testMapFilterSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapMappair() throws Throwable {
		log.info("testMapMappairFilter Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> mappairresult = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !(dat[14].equals("ArrDelay") || dat[14].equals("NA")))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).collect(toexecute, null);
		List<List> arrDelayNA = (List<List>) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat[14].equals("ArrDelay") || dat[14].equals("NA")).collect(toexecute, null);
		long sum = 0;
		int totalValueCount = 0;
		for (List<Tuple2> tuples : mappairresult) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
			totalValueCount += tuples.size();
		}
		int arrDelayNACount = 0;
		for (List arrDelay : arrDelayNA) {
			log.info(arrDelayNA);
			arrDelayNACount += arrDelay.size();
		}
		log.info(sum);
		log.info(arrDelayNACount);
		log.info(totalValueCount);
		assertTrue(-63278l == sum);
		assertEquals(46361 - arrDelayNACount, totalValueCount);
		log.info("testMapMappair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapMappairFilter() throws Throwable {
		log.info("testMapMappairFilter Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		java.util.List<List<Tuple2>> mappairresult = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.filter((Tuple2 pair) -> pair != null && (Long) ((Tuple2) pair).v2 > Long.MIN_VALUE)
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> tuples : mappairresult) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		log.info(sum);
		Assert.assertTrue(sum == -63278);

		log.info("testMapMappairFilter After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapMappairJoinPair() throws Throwable {
		log.info("testMapMappairJoinPair Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig);
		MapPair<String, Long> mappairfirst = datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((a, b) -> (Long) a + (Long) b).coalesce(1, (a, b) -> (Long) a + (Long) b);

		MapPair<String, Long> mappairsecond = datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[12].equals("CRSElapsedTime") && !dat[12].equals("NA"))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[12])))
				.reduceByKey((a, b) -> (Long) a + (Long) b).coalesce(1, (a, b) -> (Long) a + (Long) b);
		List<List<Tuple2>> mappairresult = (List) mappairfirst
				.join(mappairsecond, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2> tuples : mappairresult) {
			for (Tuple2 pair : tuples) {
				sum += (Long) ((Tuple2) pair.v1).v2;
			}
		}
		log.info(sum);
		assertTrue(6434 == sum);

		log.info("testMapMappairJoinPair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapOfTupleOutput() throws Throwable {
		log.info("testMapOfTupleOutput Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinepairjoin,
				pipelineconfig);
		java.util.List<List<Tuple2>> listreducebykey = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.map(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).collect(toexecute, null);
		for (Tuple2 tuple2 : listreducebykey.get(0)) {
			log.info(tuple2);
			assertEquals("PS", tuple2.v1);
			assertEquals(true, (Long) tuple2.v2 >= -7);
		}

		log.info("testMapOfTupleOutput After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testMapPairCollect() throws Throwable {
		log.info("testMapPairCollect Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> result = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.map(dat -> Integer.parseInt(dat[14])).mapToPair(data -> new Tuple2(data, data))
				.collect(toexecute, null);
		int sumv1 = 0, sumv2 = 0;
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				sumv1 += (Integer) pair.v1;
				sumv2 += (Integer) pair.v2;
			}
		}
		assertEquals(sumv2, sumv1);
		assertEquals(-63278l, sumv1);
		assertEquals(-63278l, sumv2);

		log.info("testMapPairCollect After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMapPeek() throws Throwable {
		log.info("testMapPeek Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String[]>> values = (List<List<String[]>>) datastream.map(value -> value.split(","))
				.peek(System.out::println).peek(valuearr -> System.out.println(((String[]) valuearr)[1]))
				.collect(toexecute, null);
		assertTrue(values.get(0).size() == 46361);
		log.info("testMapPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPair() throws Throwable {
		log.info("testMapPair Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> mapPair6List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return Tuple.tuple(datarr[0], datarr[3]);
		}).collect(toexecute, null);
		mapPair6List.stream().flatMap(stream -> stream.stream())
				.forEach(tuple2 -> assertEquals(true,
						tuple2.v2.equals("1") || tuple2.v2.equals("2") || tuple2.v2.equals("3") || tuple2.v2.equals("4")
								|| tuple2.v2.equals("5") || tuple2.v2.equals("6") || tuple2.v2.equals("7")
								|| tuple2.v2.equals("DayOfWeek")));

		log.info("testMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairFilter() throws Throwable {
		log.info("testMapPairFilter Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> mapPair6List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return (Tuple2) Tuple.tuple(datarr[0], datarr[2]);
		}).filter(tup -> {
			Tuple2 tup2 = (Tuple2) tup;
			return !tup2.v2.equals("DayofMonth") && Integer.parseInt((String) tup2.v2) >= 28;
		}).collect(toexecute, null);
		mapPair6List.stream().flatMap(stream -> stream.stream()).forEach(tup2 -> assertEquals(true,
				tup2.v2.equals("28") || tup2.v2.equals("29") || tup2.v2.equals("30") || tup2.v2.equals("31")));
		log.info("testMapPairFilter After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testMapPairFilterContains() throws Throwable {
		log.info("testMapPairFilterContains Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> datas = (List) datastream.mapToPair(data -> new Tuple2<String, String>(data, data))
				.filter(pair -> ((String) pair.v2).contains(",21,")).collect(toexecute, null);
		for (List<Tuple2> tuples : datas) {
			for (Tuple2 pair : tuples) {
				assertEquals(true, ((String) pair.v2).contains(",21,"));
			}
		}

		log.info("testMapPairFilterContains After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairFlatMap() throws Throwable {
		log.info("testMapPairFlatMap Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> mapPair2List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return (Tuple2) Tuple.tuple(datarr[0], datarr[3]);
		}).flatMap(tup2 -> Arrays.asList(tup2)).collect(toexecute, null);
		mapPair2List.stream().flatMap(stream -> stream.stream()).forEach(System.out::println);
		log.info("testMapPairFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairFlatMapToLong() throws Throwable {
		log.info("testMapPairFlatMapToLong Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapPair6List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return (Tuple2) Tuple.tuple(datarr[0], datarr[14]);
		}).filter(tup2 -> !((Tuple2) tup2).v2.equals("NA") && !((Tuple2) tup2).v2.equals("ArrDelay"))
				.flatMapToLong(tup2 -> Arrays.asList(Long.parseLong((String) ((Tuple2) tup2).v2)))
				.collect(toexecute, null);
		assertEquals(-63278,
				mapPair6List.stream().flatMap(stream -> stream.stream()).mapToLong(list -> list.longValue()).sum());
		log.info("testMapPairFlatMapToLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairMap() throws Throwable {
		log.info("testMapPairMap Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> mapPair2List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return (Tuple2) Tuple.tuple(datarr[0], datarr[3]);
		}).map(tup2 -> {
			Tuple2 tuple = (Tuple2) tup2;
			return Tuple.tuple(tuple.v1, tuple.v2);
		}).collect(toexecute, null);
		mapPair2List.stream().flatMap(stream -> stream.stream())
				.forEach(tup2 -> assertEquals(true,
						tup2.v2.equals("1") || tup2.v2.equals("2") || tup2.v2.equals("3") || tup2.v2.equals("4")
								|| tup2.v2.equals("5") || tup2.v2.equals("6") || tup2.v2.equals("7")
								|| tup2.v2.equals("DayOfWeek")));
		log.info("testMapPairMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairPeek() throws Throwable {
		log.info("testMapPairPeek Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> vals = (List) datastream.mapToPair(value -> {
			String[] values = value.split(",");
			return Tuple.tuple(values[8], values[14]);
		}).peek(tuple2->System.out.println(tuple2)).collect(toexecute, null);
		assertTrue(vals.get(0).size() == 46361);
		log.info("testMapPairPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testMapPairSorted() throws Throwable {
		log.info("testMapPairSorted Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair mappair = (MapPair) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !val[14].equals("ArrDelay") && !val[14].equals("NA");
		}).mapToPair(val -> {
			String[] valarr = val.split(",");
			return Tuple.tuple(valarr[8], Integer.parseInt(valarr[14]));
		}).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			Integer vall1 = (Integer) tup1.v2;
			Integer vall2 = (Integer) tup2.v2;
			return vall1.compareTo(vall2);
		});
		List<List<Tuple2>> values = (List) mappair.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> vals : values) {
			for (Tuple2 valarr : vals) {
				sum += (Integer) valarr.v2;
				log.info(valarr);
			}
		}
		assertTrue(sum == -63278);
		log.info("testMapPairSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testMapPairSortedPeek() throws Throwable {
		log.info("testMapPairSortedPeek Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair mappair = (MapPair) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !val[14].equals("ArrDelay") && !val[14].equals("NA");
		}).mapToPair(val -> {
			String[] valarr = val.split(",");
			return (Tuple2) Tuple.tuple(valarr[8], Integer.parseInt(valarr[14]));
		}).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			Integer vall1 = (Integer) tup1.v2;
			Integer vall2 = (Integer) tup2.v2;
			return vall1.compareTo(vall2);
		});
		List<List<Tuple2>> values = (List) mappair.peek(System.out::println).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> vals : values) {
			for (Tuple2 valarr : vals) {
				sum += (Integer) valarr.v2;
				log.info(valarr);
			}
		}
		assertTrue(sum == -63278);
		log.info("testMapPairSortedPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMassiveDataPipelinePeek() throws Throwable {
		log.info("testMassiveDataPipelinePeek Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> vals = (List) datastream.peek(System.out::println).peek(System.out::println)
				.collect(toexecute, null);
		assertTrue(vals.get(0).size() == 46361);
		log.info("testMassiveDataPipelinePeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMultipleDAG() throws Throwable {
		log.info("testMultipleDAG Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String[]> map = datastream.map(dat -> dat.split(","));
		MassiveDataPipeline<String> map1 = map.map(datas -> datas[0] + "" + datas[14]);
		MassiveDataPipeline<String> map2 = map.map(datas -> datas[1] + "" + datas[14]);
		MassiveDataPipeline<String> map3 = map.map(datas -> datas[2] + "" + datas[14]);
		MassiveDataPipeline<String> map4 = map.map(datas -> datas[3] + datas[14]);
		List<List> lst1 = (List<List>) map1.collect(toexecute, null);
		List<List> lst2 = (List<List>) map2.collect(toexecute, null);
		List<List> lst3 = (List<List>) map3.collect(toexecute, null);
		List<List> lst4 = (List<List>) map4.collect(toexecute, null);
		assertEquals(lst1.get(0).size(), lst2.get(0).size());
		assertEquals(lst2.get(0).size(), lst3.get(0).size());
		assertEquals(lst3.get(0).size(), lst4.get(0).size());
		log.info("testMultipleDAG After---------------------------------------");
	}

	@SuppressWarnings({ "unused" })
	@Test
	public void testMultipleSplits() throws Throwable {
		log.info("testMultipleSplits Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String[]> map = datastream.map(dat -> dat.split(","));
		MassiveDataPipeline<String[]> filt = map.filter(dat -> !dat[14].equals("ArrDelay"));
		MassiveDataPipeline<String> mapfilt = map.map(datas -> datas[0] + "" + datas[14])
				.filter(dat -> !dat.equals("198723"));
		MassiveDataPipeline<String[]> filt1 = map.filter(dat -> !dat[14].equals("ArrDelay"));
		MassiveDataPipeline<String> mapfilt1 = map.map(datas -> datas[0] + "" + datas[14])
				.filter(dat -> !dat.equals("198723"));
		log.info(filt.collect(toexecute, null));
		log.info(mapfilt.collect(toexecute, null));

		log.info("testMultipleSplits After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testPartitioningEachFileReduceByKey() throws Throwable {
		log.info("testPartitioningEachFileReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath,
				airlinemultiplefilesfolder, pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, new NumPartitionsEachFile(5));
		int sum = 0;
		for (List<Tuple2> tuples : listreducebykey) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		assertEquals(-62663l, sum);
		log.info("testPartitioningEachFileReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testPartitioningReduceByKey() throws Throwable {
		log.info("testPartitioningReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute, new NumPartitions(4));
		int sum = 0;
		for (List<Tuple2> tuples : listreducebykey) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		assertEquals(-63278l, sum);

		log.info("testPartitioningReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKey() throws Throwable {
		log.info("testReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2> tuples : redByKeyList) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		log.info(sum);
		assertTrue(-63278l == sum);

		log.info("testReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyBiCycleCrashSpeedLimit() throws Throwable {
		log.info("testReduceByKeyBiCycleCrashSpeedLimit Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, bicyclecrash,
				pipelineconfig);
		List<List> crashcounts = (List) datastream.map(dat -> dat.split(";")).filter(dat -> !dat[9].equals("bike_sex"))
				.mapToPair(dat -> Tuple.tuple(dat[9] + "-" + dat[6] + "-" + dat[44], 1l))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);

		for (Object crashcount : crashcounts) {
			log.info(crashcount);
		}
		assertTrue(crashcounts.get(0).size() > 0);

		log.info("testReduceByKeyBiCycleCrashSpeedLimit After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCarrierDayOfMonthArrDelay() throws Throwable {
		log.info("testReduceByKeyCarrierDayOfMonthArrDelay Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> carrierDayOfmonthArrDelay = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8] + "-" + dat[2], Long.parseLong(dat[14])))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		long sumv1 = 0;
		for (List<Tuple2> tuples : carrierDayOfmonthArrDelay) {
			for (Tuple2 pair : tuples) {
				sumv1 += (Long) pair.v2;
			}
		}
		log.info(sumv1);
		assertTrue(-63278l == sumv1);
		log.info("testReduceByKeyCarrierDayOfMonthArrDelay After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCarrierFlightNumDistance() throws Throwable {
		log.info("testReduceByKeyCarrierFlightNumDistance Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig);
		List<List> carrierFlightNumDistance = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[18].equals("Distance") && !dat[18].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8] + "-" + dat[9], Long.parseLong(dat[18])))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);

		for (Object carrierFlightNum : carrierFlightNumDistance) {
			log.info(carrierFlightNum);
		}
		assertTrue(carrierFlightNumDistance.get(0).size() > 0);

		log.info("testReduceByKeyCarrierFlightNumDistance After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCarrierFlightNumOriginDistance() throws Throwable {
		log.info("testReduceByKeyCarrierFlightNumOriginDistance Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List> distancecount = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[18].equals("Distance") && !dat[18].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8] + "-" + dat[9] + "-" + dat[16], Long.parseLong(dat[18])))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);

		for (Object distance : distancecount) {
			log.info(distance);
		}
		assertTrue(distancecount.get(0).size() > 0);

		log.info("testReduceByKeyCarrierFlightNumOriginDistance After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCarrierMonthArrivalDelay() throws Throwable {
		log.info("testReduceByKeyCarrierMonthArrivalDelay Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig);
		List<List<Tuple2>> carrierMonthArrDelay = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8] + "-" + dat[1], Long.parseLong(dat[14])))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);

		long sumv1 = 0;
		for (List<Tuple2> tuples : carrierMonthArrDelay) {
			for (Tuple2 pair : tuples) {
				sumv1 += (Long) pair.v2;
			}
		}
		log.info(sumv1);
		assertTrue(6434 == sumv1);

		log.info("testReduceByKeyCarrierMonthArrivalDelay After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCsvStream() throws Throwable {
		log.info("testReduceByKeyCsvStream Before---------------------------------------");
		CsvStream<CSVRecord, CSVRecord> datastream = MassiveDataPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig,airlineheader);
		java.util.List<List<Tuple2>> listreducebykey = (List) datastream.filter(
				dat -> dat != null && !dat.get("ArrDelay").equals("ArrDelay") && !dat.get("ArrDelay").equals("NA"))
				.mapToPair(dat -> {
					return Tuple.tuple(dat.get(8), Long.parseLong(dat.get(14)));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);
		for (Tuple2 tuple2 : listreducebykey.get(0)) {
			log.info(tuple2);
			assertEquals(-63278l, ((Long) tuple2.v2).longValue());
		}

		log.info("testReduceByKeyCsvStream After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyPopulation() throws Throwable {
		log.info("testReduceByKeyPopulation Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, population,
				pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[2].equals("Abortion_rate")).mapToPair(dat -> {
					return new Tuple2<String, Double>(dat[1], Double.parseDouble(dat[2]));
				}).reduceByKey((pair1, pair2) -> (Double) pair1 + (Double) pair2)
				.coalesce(1, (pair1, pair2) -> (Double) pair1 + (Double) pair2).collect(toexecute, null);
		double sum = 0;
		for (Tuple2 pair : listreducebykey.get(0)) {
			log.info(pair.v1 + " " + pair.v2);
			sum += (double) pair.v2;
		}
		log.info(sum);

		log.info("testReduceByKeyPopulation After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyVerySmall() throws Throwable {
		log.info("testReduceByKeyVerySmall Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig);
		List<List<Tuple2>> carrierArrDelay = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute, null);
		long sumv1 = 0;
		for (List<Tuple2> tuples : carrierArrDelay) {
			for (Tuple2 pair : tuples) {
				sumv1 += (Long) pair.v2;
			}
		}
		log.info(sumv1);
		assertTrue(6434 == sumv1);

		log.info("testReduceByKeyVerySmall After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSample() throws Throwable {
		log.info("testSample Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		java.util.List<List<Tuple2>> listreducebykey = (List) datastream.sample(46361).map(datas -> datas.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> tuples : listreducebykey) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		assertEquals(-63278l, sum);

		log.info("testSample After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSampleSample() throws Throwable {
		log.info("testSampleSample Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.sample(46361).map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> tuples : listreducebykey) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		assertEquals(-63278l, sum);

		log.info("testSampleSample After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSingleMappairMultipleReduceByKey() throws Throwable {
		log.info("testSingleMappairMultipleReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> mappair2 = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,(dat1, dat2) -> dat1 + dat2);

		MapPair<String, Long> mappair3 = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 - dat2).coalesce(1,
				(dat1, dat2) -> dat1 - dat2);

		MapPair<String, Long> mappair4 = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 - dat2 + dat1).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		MapPair<String, Long> mappair5 = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat2 - dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		List<List> result = (List) mappair2
				.join(mappair3, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.join(mappair4, (tuple1, tuple2) -> (((Tuple2) ((Tuple2) tuple1).v1).v1).equals(((Tuple2) tuple2).v1))
				.join(mappair5, (tuple1, tuple2) -> ((Tuple2) (((Tuple2) ((Tuple2) tuple1).v1).v1)).v1
						.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				assertEquals(((Tuple2) ((Tuple2) ((Tuple2) ((Tuple2) pair.v1)).v1).v1).v1, ((Tuple2) pair.v2).v1);
			}
		}

		log.info("testSingleMappairMultipleReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testStreamSampleSampleSample() throws Throwable {
		log.info("testStreamSampleSampleSample Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		java.util.List<List<Tuple2>> listreducebykey = (List) datastream.sample(10000).map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).sample(1000)
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		int sum = 0;
		for (Tuple2 pair : listreducebykey.get(0)) {
			log.info(pair.v1 + " " + pair.v2);
			sum += (Long) pair.v2;
		}
		log.info(sum);
		assertEquals(-1308, sum);
		log.info("testStreamSampleSampleSample After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnion() throws Throwable {
		log.info("testUnion Before---------------------------------------");
		MassiveDataPipeline dataverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline dataveryverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> datas = (List) dataverysmall.union(dataveryverysmall).collect(true, null);
		assertEquals(46361, datas.get(0).size());
		log.info("testUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionFilterUnion() throws Throwable {
		log.info("testUnionFilterUnion Before---------------------------------------");
		MassiveDataPipeline<String> dataverysmall = MassiveDataPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig)
				.filter(dat -> dat.split(",")[2].equals("10") || dat.split(",")[2].equals("11"));
		MassiveDataPipeline<String> dataveryverysmall1989 = MassiveDataPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig)
				.filter(dat -> dat.split(",")[2].equals("12") || dat.split(",")[2].equals("13"));
		List<List<String>> datas = (List) dataverysmall.union(dataveryverysmall1989).collect(true, null);
		Assert.assertEquals(4, datas.get(0).size());
		for (String data : datas.get(0)) {
			String value = data.split(",")[2];
			Assert.assertEquals(true,
					value.equals("10") || value.equals("11") || value.equals("12") || value.equals("13"));
		}
		log.info("testUnionFilterUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionMapPairUnion() throws Throwable {
		log.info("testUnionFilterMapPairUnion Before---------------------------------------");
		MapPair<Tuple, Object> dataverysmall = MassiveDataPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig).map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).mapToPair(dat -> {
					return (Tuple2) Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				});
		MapPair<Tuple, Object> dataveryverysmall1989 = MassiveDataPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig).map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).mapToPair(dat -> {
					return (Tuple2) Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				});
		List<List<Tuple>> datas = (List) dataverysmall.union(dataveryverysmall1989).collect(true, null);
		Assert.assertEquals(25, datas.get(0).size());
		log.info("testUnionFilterMapPairUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionMapUnion() throws Throwable {
		log.info("testUnionMapUnion Before---------------------------------------");
		MassiveDataPipeline<String> unionstream1 = MassiveDataPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig).map(dat -> dat);
		MassiveDataPipeline<String> unionstream2 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinepairjoin,
				pipelineconfig);
		List<List<String>> datas = (List) unionstream1.union(unionstream2).collect(true, null);
		Assert.assertEquals(30, datas.get(0).size());
		log.info("testUnionMapUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionReduceByKey() throws Throwable {
		log.info("testUnionReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> dataverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String> dataveryverysmall = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = (List) dataverysmall.union(dataveryverysmall).map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);
		assertEquals(-63278l, ((Tuple2) datas.get(0).get(0)).v2);
		log.info("testUnionReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionUnion() throws Throwable {
		log.info("testUnionUnion Before---------------------------------------");
		MassiveDataPipeline unionstream1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline unionstream2 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline unionstream3 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> datas = (List) unionstream1.union(unionstream2).union(unionstream3).collect(true, null);
		assertEquals(46361, datas.get(0).size());
		log.info("testUnionUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionUnionReduceByKey() throws Throwable {
		log.info("testUnionUnionReduceByKey Before---------------------------------------");
		MassiveDataPipeline<String> unionstream1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String> unionstream2 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String> unionstream3 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = (List) unionstream1.union(unionstream2).union(unionstream3).map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);
		assertEquals(-63278l, ((Tuple2) datas.get(0).get(0)).v2);
		log.info("testUnionUnionReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCount() throws Throwable {
		log.info("testWordCount Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")))
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l)).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute, null);
		wordscount.stream().forEach(log::info);

		log.info("testWordCount After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCountCountAndWordSorted() throws Throwable {
		log.info("testWordCountCountAndWordSorted Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")))
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l)).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
					Tuple2 tup1 = (Tuple2) val1;
					Tuple2 tup2 = (Tuple2) val2;
					int compres = ((Long) tup1.v2).compareTo(((Long) tup2.v2));
					if (compres == 0) {
						return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
					}
					return compres;
				}).collect(toexecute, null);
		wordscount.stream().flatMap(stream -> stream.stream()).forEach(log::info);

		log.info("testWordCountCountAndWordSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCountCountSorted() throws Throwable {
		log.info("testWordCountCountSorted Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")))
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l)).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
					Tuple2 tup1 = (Tuple2) val1;
					Tuple2 tup2 = (Tuple2) val2;
					return ((Long) tup1.v2).compareTo(((Long) tup2.v2));
				}).collect(toexecute, null);
		wordscount.stream().flatMap(stream -> stream.stream()).forEach(log::info);

		log.info("testWordCountCountSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCountFlatMapToTuple() throws Throwable {
		log.info("testWordCountFlatMapToTuple Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List wordscount = (List) datastream.flatMapToTuple2(str -> {
			String[] strarr = str.split(" ");
			List<Tuple2<String, Long>> pairs = (List) Arrays.asList(strarr).parallelStream().map(stra -> {
				return (Tuple2<String, Long>) Tuple.tuple(stra, 1l);
			}).collect(Collectors.toCollection(Vector::new));
			return pairs;
		}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute,
				null);
		wordscount.stream().forEach(log::info);

		log.info("testWordCountFlatMapToTuple After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCountWordSorted() throws Throwable {
		log.info("testWordCountWordSorted Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")))
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l)).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
					Tuple2 tup1 = (Tuple2) val1;
					Tuple2 tup2 = (Tuple2) val2;
					return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
				}).collect(toexecute, null);
		int numberofwords = 0;
		int numberofdistinctwords = 0;
		for (List<Tuple2> vals : wordscount) {
			numberofdistinctwords += vals.size();
			for (Tuple2 valarr : vals) {
				numberofwords += (Long) valarr.v2;
				log.info(valarr);
			}
		}
		assertEquals(201, numberofwords);
		assertEquals(121, numberofdistinctwords);
		log.info("testWordCountWordSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testDistinct() throws Throwable {
		log.info("testDistinct Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" "))).distinct()
				.collect(toexecute, null);
		for (List<String> vals : wordscount) {
			for (String distinctwords : vals)
				log.info(distinctwords);
		}
		assertEquals(121, wordscount.get(0).size());
		log.info("testDistinct After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapToInt() throws Throwable {
		log.info("testMapToInt Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<AtomicInteger>> intvalues = (List) datastream.map(str -> str.split(","))
				.filter(str -> !str[14].equals("ArrDelay") && !str[14].equals("NA"))
				.mapToInt(str -> Integer.parseInt(str[14])).<AtomicInteger> collect(toexecute,
						() -> new AtomicInteger(),
						(AtomicInteger a, int b) -> ((AtomicInteger) a).set(((AtomicInteger) a).get() + b),
						(AtomicInteger a, AtomicInteger b) -> a.set(a.get() + b.get()));
		for (List<AtomicInteger> vals : intvalues) {
			for (AtomicInteger intvalue : vals)
				log.info(intvalue);
		}
		assertEquals(-63278, intvalues.get(0).get(0).get());
		log.info("testMapToInt After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapToIntDistinct() throws Throwable {
		log.info("testMapToIntDistinct Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<AtomicInteger>> intvalues = (List) datastream.map(str -> str.split(","))
				.filter(str -> !str[14].equals("ArrDelay") && !str[14].equals("NA"))
				.mapToInt(str -> Integer.parseInt(str[14])).distinct().<AtomicInteger> collect(toexecute,
						() -> new AtomicInteger(),
						(AtomicInteger a, int b) -> ((AtomicInteger) a).set(((AtomicInteger) a).get() + b),
						(AtomicInteger a, AtomicInteger b) -> a.set(a.get() + b.get()));
		for (List<AtomicInteger> vals : intvalues) {
			for (AtomicInteger intvalue : vals)
				log.info(intvalue);
		}
		assertEquals(26962, intvalues.get(0).get(0).get());
		log.info("testMapToIntDistinct After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapToIntMap() throws Throwable {
		log.info("testMapToIntMap Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<AtomicInteger>> intvalues = (List) datastream.map(str -> str.split(","))
				.filter(str -> !str[14].equals("ArrDelay") && !str[14].equals("NA"))
				.mapToInt(str -> Integer.parseInt(str[14])).map(new IntUnaryOperator() {

					@Override
					public int applyAsInt(int operand) {
						return operand + 100;
					}

				}).<AtomicInteger> collect(toexecute, () -> new AtomicInteger(),
						(AtomicInteger a, int b) -> ((AtomicInteger) a).set(((AtomicInteger) a).get() + b),
						(AtomicInteger a, AtomicInteger b) -> a.set(a.get() + b.get()));
		for (List<AtomicInteger> vals : intvalues) {
			for (AtomicInteger intvalue : vals)
				log.info(intvalue);
		}
		assertEquals(4532422, intvalues.get(0).get(0).get());
		log.info("testMapToIntMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCountByKey() throws Throwable {
		log.info("testCountByKey Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !str[14].equals("ArrDelay")).mapToPair(str -> Tuple.tuple(str[1], str[14])).countByKey()
				.collect(toexecute, new NumPartitions(3));
		long sum = 0;
		for (List<Tuple> tuples : tupleslist) {
			for (Tuple tuple : tuples) {
				log.info(tuple);
				sum += (long) ((Tuple2) tuple).v2;
			}
			log.info("");
		}
		assertEquals(46360l, sum);
		log.info("testCountByKey After---------------------------------------");
	}

	

}
