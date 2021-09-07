package com.github.mdc.stream.sql.examples;

import java.io.Serializable;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.MDCConstants.STORAGE;
import com.github.mdc.stream.Pipeline;
import com.github.mdc.stream.sql.MDPSql;
import com.github.mdc.stream.sql.MDPSqlBuilder;

public class SqlUniqueCarrierSumCountArrDelaySADisk implements Serializable, Pipeline {
	private static final long serialVersionUID = -7001849661976107123L;
	private Logger log = Logger.getLogger(SqlUniqueCarrierSumCountArrDelaySADisk.class);
	String[] airlineheader = new String[] { "AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay" };
	String[] carrierheader = { "Code", "Description" };
	static SqlTypeName[] airsqltype = {SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR
			,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR};
	static SqlTypeName[] carriersqltype = {SqlTypeName.VARCHAR,SqlTypeName.VARCHAR};
	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setMaxmem("1024");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setBlocksize(args[2]);
		pipelineconfig.setBatchsize(args[3]);
		pipelineconfig.setStorage(STORAGE.DISK);
		pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
		testSql(args, pipelineconfig);
	}

	@SuppressWarnings({ "unchecked" })
	public void testSql(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("SqlUniqueCarrierSumCountArrDelaySADisk.testSql Before---------------------------------------");
		String statement = "SELECT UniqueCarrier,sum(ArrDelay),count(ArrDelay) "
				+ "FROM airline where ArrDelay<>'NA' and ArrDelay<>'ArrDelay' group by UniqueCarrier";
		MDPSql mdpsql = MDPSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airsqltype)
				.setHdfs(args[0])
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<CSVRecord>> records = (List<List<CSVRecord>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<CSVRecord> recs : records) {
			for (CSVRecord rec : recs) {
				log.info(rec);
				sum += Long.valueOf(rec.get(1));
			}
		}
		log.info("Sum = "+sum);
		log.info("SqlUniqueCarrierSumCountArrDelaySADisk.testSql After---------------------------------------");
	}
}
