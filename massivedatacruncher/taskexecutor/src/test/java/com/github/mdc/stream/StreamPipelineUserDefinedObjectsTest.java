package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.github.mdc.stream.StreamPipeline;

public class StreamPipelineUserDefinedObjectsTest extends StreamPipelineBaseTestCommon {
	boolean toexecute = true;
	int sum = 0;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapCollect() throws Throwable {
		log.info("testMapCollect Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Map>> data = (List<List<Map>>) datapipeline.map(value->value.split(","))
				.map(val->{
					Map<String,String> map= new HashMap<>();
					int index=0;
					for(String header:airlineheader) {
						map.put(header, val[index]);
						index++;
					}
					return map;
					
				}).collect(toexecute, null);
		int sum = 0;
		for (List<Map> partitioneddata : data) {
			for(Map valuemap:partitioneddata) {
				log.info(valuemap);
			}
			sum+=partitioneddata.size();
		}
		assertEquals(46361, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapCollect After---------------------------------------");
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapContainerExecuteCollect() throws Throwable {
		log.info("testMapContainerExecuteCollect Before---------------------------------------");
		String localmode = pipelineconfig.getLocal();
		pipelineconfig.setLocal("false");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Map>> data = (List<List<Map>>) datapipeline.map(value->value.split(","))
				.map(val->{
					Map<String,String> map= new HashMap<>();
					int index=0;
					for(String header:airlineheader) {
						map.put(header, val[index]);
						index++;
					}
					return map;
					
				}).collect(toexecute, null);
		int sum = 0;
		for (List<Map> partitioneddata : data) {
			for(Map valuemap:partitioneddata) {
				log.info(valuemap);
			}
			sum+=partitioneddata.size();
		}
		assertEquals(46361, sum);
		pipelineconfig.setLocal(localmode);
		log.info("testMapContainerExecuteCollect After---------------------------------------");
	}
}
