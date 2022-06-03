package com.github.mdc.stream.sql;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.commons.csv.CSVRecord;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.MapPair;
import com.github.mdc.stream.StreamPipeline;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.functions.BiPredicateSerializable;
import com.github.mdc.stream.functions.CoalesceFunction;
import com.github.mdc.stream.functions.JoinPredicate;
import com.github.mdc.stream.functions.LeftOuterJoinPredicate;
import com.github.mdc.stream.functions.MapFunction;
import com.github.mdc.stream.functions.MapToPairFunction;
import com.github.mdc.stream.functions.PredicateSerializable;
import com.github.mdc.stream.functions.ReduceByKeyFunction;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;

public class StreamPipelineSqlBuilder {
	String sql;
	String hdfs;
	transient PipelineConfig pc;
	ConcurrentMap<String, String> tablefoldermap = new ConcurrentHashMap<>();
	ConcurrentMap<String, String[]> tablecolumnsmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, SqlTypeName[]> tablecolumntypesmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, Map<String, String>> tablecolumnindexsmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, String> globalindexcolumnsmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, String> globalindextablesmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, ConcurrentMap<String, String>> globaltableindextablesmap = new ConcurrentHashMap<>();
	transient ConcurrentMap<EnumerableHashJoin, ConcurrentMap<String, String>> hashjoinglobalindextablesmap = new ConcurrentHashMap<>();
	transient ConcurrentMap<EnumerableProject, List<OperandSqlFunction>> enuprojcolumnmap = new ConcurrentHashMap<>();
	transient ConcurrentMap<EnumerableAggregate, List<OperandSqlFunction>> enuaggcolumnmap = new ConcurrentHashMap<>();
	Set<String> tablescancolumns = new LinkedHashSet<>();
	transient ConcurrentMap<EnumerableFilter,Set<String>> filteredtablescancolumns = new ConcurrentHashMap<>();
	transient ConcurrentMap<RelNode,RelNode> childparentrelnode = new ConcurrentHashMap<>();
	
	private StreamPipelineSqlBuilder() {

	}

	public static StreamPipelineSqlBuilder newBuilder() {
		return new StreamPipelineSqlBuilder();
	}

	public StreamPipelineSqlBuilder add(String folder, String tablename, String[] columns, SqlTypeName[] sqltypes) {
		tablefoldermap.put(tablename, folder);
		tablecolumnsmap.put(tablename, columns);
		tablecolumntypesmap.put(tablename, sqltypes);
		return this;
	}

	public StreamPipelineSqlBuilder setHdfs(String hdfs) {
		this.hdfs = hdfs;
		return this;
	}

	public StreamPipelineSqlBuilder setPipelineConfig(PipelineConfig pc) {
		this.pc = pc;
		return this;
	}

	public StreamPipelineSqlBuilder setSql(String sql) {
		this.sql = sql;
		return this;
	}

	public StreamPipelineSql build() throws Exception {
		Set<String> tablesfromconfig = tablecolumnsmap.keySet();
		SimpleSchema.Builder builder = SimpleSchema.newBuilder("mdc-" + System.currentTimeMillis());
		for (String table : tablesfromconfig) {
			builder.addTable(getSimpleTable(table, tablecolumnsmap.get(table), tablecolumntypesmap.get(table)));
		}
		SimpleSchema schema = builder.build();
		Optimizer optimizer = Optimizer.create(schema);
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode validatedSqlTree = optimizer.validate(sqlTree);
		RelNode relTree = optimizer.convert(validatedSqlTree);
		print("AFTER CONVERSION", relTree);
		RuleSet rules = RuleSets.ofList(CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC,
				CoreRules.FILTER_CALC_MERGE,
				CoreRules.AGGREGATE_PROJECT_MERGE,
				CoreRules.PROJECT_MERGE, CoreRules.FILTER_INTO_JOIN, EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
				EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_FILTER_RULE,
				EnumerableRules.ENUMERABLE_CALC_RULE, EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
				EnumerableRules.ENUMERABLE_JOIN_RULE,
				EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE);

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		print("Tree", optimizerRelTree);
		var stack = new Stack<Object>();
		traverseRelNode(optimizerRelTree,optimizerRelTree.getInputs(), stack);
		processNode(optimizerRelTree, stack);
		Object stacktop = stack.pop();
		return new StreamPipelineSql(stacktop);
	}

	int globalcolumnindex = 0;
	boolean isjoin = false;

	public void traverseRelNode(RelNode optimizerRelTree,List<RelNode> nodes, Stack<Object> stack) throws PipelineException {
		getTableScanColumns(optimizerRelTree);
		for (RelNode node : nodes) {
			childparentrelnode.put(node, optimizerRelTree);
			traverseRelNode(node,node.getInputs(), stack);
			processNode(node, stack);
		}
	}

	@SuppressWarnings("unchecked")
	public void getTableScanColumns(RelNode node) {
		if(node instanceof EnumerableProject ep) {
			ep.getProjects().stream().forEach(rexnode->rexnode.accept(new EnumerableProjectFilterHashJoinRexVisitor(tablescancolumns)));
		}else if(node instanceof EnumerableFilter ef) {
			filteredtablescancolumns.put(ef, new LinkedHashSet<String>());
			ef.getCondition().accept(new EnumerableProjectFilterHashJoinRexVisitor(filteredtablescancolumns.get(ef)));
		}else if (node instanceof EnumerableHashJoin ehj) {
			ehj.getCondition().accept(new EnumerableProjectFilterHashJoinRexVisitor(tablescancolumns));
		}else if (node instanceof EnumerableAggregate ea) {
			List<Integer> agggroup = new ArrayList<>(ea.getGroupSet().asList());
			agggroup.stream().forEach(key->tablescancolumns.add("$"+key));
		}
	}
	
	public MapFunction<CSVRecord, Map> tableScanRequiredColumns(String[] columns) {
		return (Serializable & MapFunction<CSVRecord, Map>) (CSVRecord csvrecord) -> {
			Map values = new LinkedHashMap<>();
			for (String column : columns) {
				values.put(column,csvrecord.get(column));
			}
			return values;
		};

	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void processNode(RelNode node, Stack<Object> stack) throws PipelineException {
		if (node instanceof EnumerableTableScan ets) {
			RelOptTableImpl rtimpl = (RelOptTableImpl) ets.getTable();
			StreamPipeline mdp = StreamPipeline.newCsvStreamHDFS(hdfs,
					tablefoldermap.get(rtimpl.getQualifiedName().get(1)), this.pc,
					tablecolumnsmap.get(rtimpl.getQualifiedName().get(1)));
			
			Map<String, String> indexcolumn = tablecolumnindexsmap.get(rtimpl.getQualifiedName().get(1));
			if (Objects.isNull(tablecolumnindexsmap.get(rtimpl.getQualifiedName().get(1)))) {
				indexcolumn = new ConcurrentHashMap<>();
				tablecolumnindexsmap.put(rtimpl.getQualifiedName().get(1), indexcolumn);
			}
			int tablespecindex = 0;
			List<String> tscolumnsreq = new ArrayList<>();
			ConcurrentMap<String, String> tableindextablesmap = new ConcurrentHashMap<>();
			RelNode parentnode = childparentrelnode.get(ets);
			Set<String> tablescancolumns=null;
			if(!Objects.isNull(parentnode) && parentnode instanceof EnumerableFilter ef) {
				tablescancolumns = filteredtablescancolumns.get(ef);
			}
			for (RelDataTypeField field : rtimpl.getRowType().getFieldList()) {
				indexcolumn.put("$" + tablespecindex, field.getName());
				globalindexcolumnsmap.put("$" + globalcolumnindex, field.getName());
				globalindextablesmap.put("$" + globalcolumnindex, rtimpl.getQualifiedName().get(1));
				tableindextablesmap.put("$" + globalcolumnindex, rtimpl.getQualifiedName().get(1));
				if(!Objects.isNull(parentnode) && parentnode instanceof EnumerableFilter && tablescancolumns!=null && tablescancolumns.contains("$" + tablespecindex)){
					tscolumnsreq.add(field.getName());
				}
				if(this.tablescancolumns.contains("$" + globalcolumnindex)){
					tscolumnsreq.add(field.getName());
				}
				globalcolumnindex++;
				tablespecindex++;
			}
			stack.push(mdp.map(tableScanRequiredColumns(tscolumnsreq.toArray(new String[tscolumnsreq.size()]))));
			globaltableindextablesmap.put(rtimpl.getQualifiedName().get(1), tableindextablesmap);
		} else if (node instanceof EnumerableHashJoin ehj) {

			EnumerableTableScan etsleft = null, etsright = null;
			EnumerableHashJoin ehjlef = null, ehjrig = null;
			Object left = ehj.getLeft();
			Object right = ehj.getRight();
			if (left instanceof EnumerableHashJoin ehjleft) {
				ConcurrentMap<String, String> globalindexhashjoinmap = new ConcurrentHashMap<>();
				getAllColumnsEnumerableHashJoin(ehjleft, globalindexhashjoinmap);
				hashjoinglobalindextablesmap.put(ehjleft, globalindexhashjoinmap);
				ehjlef = ehjleft;
			}
			if (right instanceof EnumerableHashJoin ehjright) {
				ConcurrentMap<String, String> globalindexhashjoinmap = new ConcurrentHashMap<>();
				getAllColumnsEnumerableHashJoin(ehjright, globalindexhashjoinmap);
				hashjoinglobalindextablesmap.put(ehjright, globalindexhashjoinmap);
				ehjrig = ehjright;
			}
			if (left instanceof EnumerableFilter ef) {
				etsleft = (EnumerableTableScan) ef.getInput();
			} else if (left instanceof EnumerableTableScan ets) {
				etsleft = ets;
			}
			if (right instanceof EnumerableFilter ef) {
				etsright = (EnumerableTableScan) ef.getInput();
			} else if (right instanceof EnumerableTableScan ets) {
				etsright = ets;
			}
			HashJoinRexVisitor hjrv = null;
			StreamPipeline<Map> mdp2 = (StreamPipeline) stack.pop();
			StreamPipeline<Map> mdp1 = (StreamPipeline) stack.pop();
			if (!Objects.isNull(ehjlef) && !Objects.isNull(ehjrig)) {
				hjrv = new HashJoinRexVisitor(stack, ehjlef, ehjrig, ehj.getJoinType().name().toLowerCase());
			} else if (!Objects.isNull(etsleft) && !Objects.isNull(ehjrig)) {
				hjrv = new HashJoinRexVisitor(stack, etsleft, ehjrig, ehj.getJoinType().name().toLowerCase());
			} else if (!Objects.isNull(ehjlef) && !Objects.isNull(etsright)) {
				hjrv = new HashJoinRexVisitor(stack, ehjlef, etsright, ehj.getJoinType().name().toLowerCase());
			} else {
				hjrv = new HashJoinRexVisitor(stack, etsleft, etsright, ehj.getJoinType().name().toLowerCase());
			}
			ehj.getCondition().accept(hjrv);
			var joinmergefunction = new MapFunction<Tuple2<Map,Map>,Map>() {

				@Override
				public Map apply(Tuple2<Map, Map> val) {
					val.v1.putAll(val.v2);
					return val.v1;
				}};
			if (ehj.getJoinType().name().equalsIgnoreCase("inner")) {
				stack.push(mdp1.join(mdp2, (JoinPredicate<Map, Map>) stack.pop()).map(joinmergefunction));
			} else if (ehj.getJoinType().name().equalsIgnoreCase("left")) {
				stack.push(mdp1.leftOuterjoin(mdp2, (LeftOuterJoinPredicate<Map, Map>) stack.pop()).map(joinmergefunction));
			} else if (ehj.getJoinType().name().equalsIgnoreCase("right")) {
				stack.push(mdp1.rightOuterjoin(mdp2, (RightOuterJoinPredicate<Map, Map>) stack.pop()).map(joinmergefunction));
			}

			isjoin = true;
		} else if (node instanceof EnumerableProject ep) {
			Object mdpmp = stack.pop();			
			if(mdpmp instanceof StreamPipeline mdp) {
				if(ep.getInputs().get(0) instanceof EnumerableAggregate) {
					List<OperandSqlFunction> columns = enuaggcolumnmap.get(ep.getInputs().get(0));
					List<OperandSqlFunction> reposcolumns = repositionColumns(columns,ep.getProjects());
					OperandSqlFunction[] columnarray = reposcolumns.toArray(new OperandSqlFunction[reposcolumns.size()]);
					stack.push(mdp.map(tuple2ToProjection(columnarray)));
					enuprojcolumnmap.put(ep, reposcolumns);
				}
				else {
					List<OperandSqlFunction> columns = convertIndexToColumns(ep.getProjects());
					OperandSqlFunction[] columnarray = columns.toArray(new OperandSqlFunction[columns.size()]);
					stack.push(mdp.map(tuple2ToProjection(columnarray)));
					enuprojcolumnmap.put(ep, columns);
				}
			}else if(mdpmp instanceof MapPair mp) {
				if(ep.getInputs().get(0) instanceof EnumerableAggregate ea) {
					List<OperandSqlFunction> columns = enuaggcolumnmap.get(ea);
					List<OperandSqlFunction> reposcolumns = repositionColumns(columns,ep.getProjects());
					List<String> finalcolumns= reposcolumns.stream().map(osf->osf.column).collect(Collectors.toList());
					stack.push(mp.map(tableScanRequiredColumns(finalcolumns.toArray(new String[finalcolumns.size()]))));
				}
			}
		} else if (node instanceof EnumerableFilter ef) {
			EnumerableTableScan ets = (EnumerableTableScan) ef.getInput();
			StreamPipeline mdp = (StreamPipeline) stack.pop();
			ef.getCondition().accept(new FilteredRexVisitor(stack, mdp, ets));
			stack.push(mdp.filter((PredicateSerializable) stack.pop()));
		} else if (node instanceof EnumerableAggregate ea) {
			Object agginput =  ea.getInput();
			List<AggregateCall> aggcalls = ea.getAggCallList();
			StreamPipeline<Map> mdp = (StreamPipeline) stack.pop();
			if(agginput instanceof EnumerableProject ep) {
				List<Integer> agggroup = new ArrayList<>(ea.getGroupSet().asList());
				List<OperandSqlFunction> osfs = getAggregateColumns(enuprojcolumnmap.get(ep),agggroup);
				enuaggcolumnmap.put(ea, osfs);
				if(aggcalls.size()<2) {
					for(AggregateCall aggcall:aggcalls) {
						String aggfunc = aggcall.getAggregation().getName();
						if(aggfunc.equals("SUM")) {
							MapToPairFunction<Map, Tuple2<Map, Long>> mappair = !agggroup.isEmpty()?aggregateCallSum(enuprojcolumnmap.get(ep),agggroup,
									aggcall):null;
							MapPair<Map,Long> mpcrml = !Objects.isNull(mappair)?mdp.mapToPair(mappair):null;
							OperandSqlFunction osf = new OperandSqlFunction("sum("+enuprojcolumnmap.get(ep).get(aggcall.getArgList().get(0)).column+")","Long");
							osfs.add(osf);
							if(!Objects.isNull(mpcrml)) {
								stack.push(mpcrml.reduceByKey((Serializable & ReduceByKeyFunction<Long>)(a, b) -> 
								a + b)
								.coalesce(1, (Serializable & CoalesceFunction<Long>) (a, b) -> 
								a + b).map(convertToMap(enuprojcolumnmap.get(ep),agggroup,"sum("+enuprojcolumnmap.get(ep).get(aggcall.getArgList().get(0)).column+")")));
							}else {
								String column = enuprojcolumnmap.get(ep).get(aggcall.getArgList().get(0)).column;
								stack.push(mdp.map((Serializable&MapFunction<Map,Long>)map->Long.valueOf((String)map.get(column))).reduce((a,b)->a+b));
							}
						}else if(aggfunc.equals("COUNT")) {
							MapToPairFunction<Map, Tuple2<Map, Long>> mappair = aggregateCallCount(enuprojcolumnmap.get(ep),agggroup);
							MapPair<Map,Long> mpcrml = !Objects.isNull(mappair)?mdp.mapToPair(mappair):null;
							OperandSqlFunction osf = new OperandSqlFunction("count()","Long");
							osfs.add(osf);
							stack.push(mpcrml.countByKey().coalesce(1, (a,b)->a+b).map(convertToMap(enuprojcolumnmap.get(ep),agggroup,"count()")));
						}
					}					
				}else {
					List<String> aggfuncs  = new ArrayList<>();
					Map<String,AggregateCall> aggfunaggcallmap = new ConcurrentHashMap<>();
					for(AggregateCall aggcall:aggcalls) {
						String aggfunc = aggcall.getAggregation().getName();
						aggfuncs.add(aggfunc);
						aggfunaggcallmap.put(aggfunc, aggcall);
						if(aggfunc.equals("SUM")) {
							OperandSqlFunction osf = new OperandSqlFunction("sum("+enuprojcolumnmap.get(ep).get(aggcall.getArgList().get(0)).column+")","Long");
							osfs.add(osf);
						}else if(aggfuncs.contains("COUNT")) {
							OperandSqlFunction osf = new OperandSqlFunction("count()","Long");
							osfs.add(osf);
						}
					}
					if(aggfuncs.contains("COUNT")&&aggfuncs.contains("SUM")){
						MapToPairFunction<Map, Tuple2<Map, Long>> mappair = !agggroup.isEmpty()?aggregateCallSum(enuprojcolumnmap.get(ep),agggroup,
								aggfunaggcallmap.get("SUM")):null;
						MapPair<Map,Long> mpcrml = !Objects.isNull(mappair)?mdp.mapToPair(mappair):null;
						stack.push(mpcrml.mapValues(mv->new Tuple2<Long,Long>(mv,1l))
								.reduceByValues((tuple1,tuple2)->new Tuple2<Long,Long>(tuple1.v1+tuple2.v1,tuple1.v2+tuple2.v2))
								.coalesce(1, (tuple1,tuple2)->new Tuple2<Long,Long>(tuple1.v1+tuple2.v1,tuple1.v2+tuple2.v2)).map(convertTuple2ToMap(osfs)));
					}
				}
			}else if(agginput instanceof EnumerableFilter ef) {
				List<Integer> agggroup = new ArrayList<>(ea.getGroupSet().asList());
				List<Integer> indexagggroupreform = new ArrayList<>();
				for(int index = 0;index<agggroup.size();index++) {
					indexagggroupreform.add(index);
				}
				List<OperandSqlFunction> columns = agggroup.stream().map(key->globalindexcolumnsmap.get("$"+key)).map(column->new OperandSqlFunction(column,"String")).collect(Collectors.toList());
				for(AggregateCall aggcall:aggcalls) {
					String aggfunc = aggcall.getAggregation().getName();
					MapToPairFunction<Map, Tuple2<Map, Long>> mappair = !agggroup.isEmpty()?aggregateCallCount(columns,indexagggroupreform):null;
					MapPair<Map,Long> mpcrml = !Objects.isNull(mappair)?mdp.mapToPair(mappair):null;
					if(aggfunc.equals("COUNT") && Objects.isNull(mpcrml)) {
						OperandSqlFunction osf = new OperandSqlFunction("count()","Long");
						stack.push(mdp.map(csvrec->1l).reduce((a,b)->a+b));
					}else if(aggfunc.equals("COUNT") && !Objects.isNull(mpcrml)) {
						OperandSqlFunction osf = new OperandSqlFunction("count()","Long");
						stack.push(mpcrml.countByKey().coalesce(1, (a,b)->a+b).map(convertToMap(columns,indexagggroupreform,"count()")));
					}
				}
			}else if(agginput instanceof EnumerableHashJoin ehj) {
				List<OperandSqlFunction> osfs = new ArrayList<>();
				List<Integer> agggroup = new ArrayList<>(ea.getGroupSet().asList());
				List<RelDataTypeField> fields = ehj.getRowType().getFieldList();
				List<Integer> indexes = new ArrayList<>();
				int count = 0;
				for(Integer index:agggroup) {
					OperandSqlFunction osf = new OperandSqlFunction(fields.get(index).getName(),fields.get(index).getType().getSqlTypeName().getName());
					osfs.add(osf);
					indexes.add(count++);
				}
				for(AggregateCall aggcall:aggcalls) {
					String aggfunc = aggcall.getAggregation().getName();
					MapToPairFunction<Map, Tuple2<Map, Long>> mappair = aggregateCallCount(osfs,indexes);
					MapPair<Map,Long> mpcrml = mdp.mapToPair(mappair);
					if(aggfunc.equals("COUNT")) {
						OperandSqlFunction osf = new OperandSqlFunction("count()","Long");
						stack.push(mpcrml.countByKey().coalesce(1, (a,b)->a+b).map(convertToMap(osfs,indexes,"count()")));
					}
				}
			}
		}
	}
	
	public List<OperandSqlFunction> getAggregateColumns(List<OperandSqlFunction> osfs,List<Integer> agggroup){
		return agggroup.stream().map(val->osfs.get(val)).collect(Collectors.toList());
		
	}
	
	public List<OperandSqlFunction> repositionColumns(List<OperandSqlFunction> columns,List<RexNode> indexes) {
		List<OperandSqlFunction> reposcolumns = indexes.stream().map(rexnode -> {
			if (rexnode instanceof RexCall rc) {
				String columnindex = ((RexInputRef) rc.getOperands().get(0)).getName();
				OperandSqlFunction osf =
						columns.get(Integer.valueOf((String)columnindex.substring(1)));
				return osf;
			}
			String columnindex = (((RexInputRef) rexnode).getName());
			OperandSqlFunction osf = 
					columns.get(Integer.valueOf((String)columnindex.substring(1)));
			return osf;
		}).collect(Collectors.toList());
		return reposcolumns;
	}
	
	@SuppressWarnings("unchecked")
	public MapFunction<Tuple2<Map, Tuple2<Long,Long>>, Map> convertTuple2ToMap(List<OperandSqlFunction> osfs) {
		return (Serializable &  MapFunction<Tuple2<Map, Tuple2<Long,Long>>, Map>)(Tuple2<Map, Tuple2<Long,Long>> rec)->{
			Map inputmap = rec.v1;
			Map outputmap = new LinkedHashMap<>();
			for(OperandSqlFunction osf:osfs) {
					if(osf.column.startsWith("sum")) {
						outputmap.put(osf.column, rec.v2.v1);
					}else if(osf.column.startsWith("count")){
						outputmap.put(osf.column, rec.v2.v2);
					}else {
						outputmap.put(osf.column, inputmap.get(osf.column));
					}
			}
			return outputmap;
		};
	}
	
	public MapFunction<Tuple2<Map, Long>, Map> convertToMap(List<OperandSqlFunction> osf, List<Integer> agggroup, String extracolumn) {
		return (Serializable &  MapFunction<Tuple2<Map, Long>, Map>)(Tuple2<Map, Long> rec)->{
			Map inputmap = rec.v1;
			inputmap.put(extracolumn,rec.v2);
			return inputmap;
		};
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MapToPairFunction<Map, Tuple2<Map, Long>> aggregateCallSum(List<OperandSqlFunction> columns,List<Integer> agggroup, AggregateCall aggcall) {
		MapToPairFunction<Map, Tuple2<Map, Long>> pairfunc = (Serializable & MapToPairFunction<Map, Tuple2<Map, Long>>) (map) -> {
			try {
				for(Integer index:agggroup) {
					map.put(columns.get(index).column,cast(map.get(columns.get(index).column),columns.get(index).sqltype));
				}
				Integer aggindex = aggcall.getArgList().get(0);
				return new Tuple2(map, Long.valueOf((String) map.get(columns.get(aggindex).column)));
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return null;
		};
		return pairfunc;
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MapToPairFunction<Map, Tuple2<Map, Long>> aggregateCallCount(List<OperandSqlFunction> columns,List<Integer> agggroup) {
		MapToPairFunction<Map, Tuple2<Map, Long>> pairfunc = (Serializable & MapToPairFunction<Map, Tuple2<Map, Long>>) (map) -> {
			try {
				for(Integer index:agggroup) {
					map.put(columns.get(index).column,cast(map.get(columns.get(index).column),columns.get(index).sqltype));
				}
				return new Tuple2(map, 0l);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return null;
		};
		return pairfunc;
	}
	
	
	public void getAllColumnsEnumerableHashJoin(RelNode node, Map<String, String> alltablecolumnindexsmap) {
		if (node instanceof EnumerableTableScan ets) {
			RelOptTableImpl rtimpl = (RelOptTableImpl) ets.getTable();
			Map<String, String> indextablesmap = globaltableindextablesmap.get(rtimpl.getQualifiedName().get(1));
			alltablecolumnindexsmap.putAll(indextablesmap);
		} else if (node instanceof EnumerableHashJoin ehj) {
			getAllColumnsEnumerableHashJoin(ehj.getLeft(), alltablecolumnindexsmap);
			getAllColumnsEnumerableHashJoin(ehj.getRight(), alltablecolumnindexsmap);
		} else if (node instanceof EnumerableFilter ef) {
			getAllColumnsEnumerableHashJoin(ef.getInput(), alltablecolumnindexsmap);
		}
	}

	class OperandSqlFunction {
		private String column;
		private String sqltype;

		public OperandSqlFunction(String column, String sqltype) {
			this.column = column;
			this.sqltype = sqltype;
		}

		public String getColumn() {
			return column;
		}

		public Object getSqltype() {
			return sqltype;
		}
	}

	public List<OperandSqlFunction> convertIndexToColumns(List<RexNode> indexes) {
		List<OperandSqlFunction> columns = indexes.stream().map(rexnode -> {
			if (rexnode instanceof RexCall rc) {
				OperandSqlFunction osf = new OperandSqlFunction(
						globalindexcolumnsmap.get(((RexInputRef) rc.getOperands().get(0)).getName()),
						rc.getType().getSqlTypeName().getName());
				return osf;
			}
			OperandSqlFunction osf = new OperandSqlFunction(
					globalindexcolumnsmap.get(((RexInputRef) rexnode).getName()), "String");
			return osf;
		}).collect(Collectors.toList());
		return columns;
	}

	@SuppressWarnings("unchecked")
	public MapFunction<Object, Map> tuple2ToProjection(OperandSqlFunction[] columns) {
		return (Serializable & MapFunction<Object, Map>) (Object object) -> {
			Map result = new LinkedHashMap<>();
			if (object instanceof Tuple2 tuple) {
				for (OperandSqlFunction osf : columns) {
					result.put(osf.column, ((Map) tuple.v1).get(osf.column) == null ? ((Map) tuple.v2).get(osf.column)
							: ((Map) tuple.v1).get(osf.column));
				}
			}else if(object instanceof Map map) {
				for (OperandSqlFunction osf : columns) {
					result.put(osf.column, map.get(osf.column));
				}
			}
			return result;
		};

	}

	public MapToPairFunction<Map, Tuple> mapToTuple(OperandSqlFunction[] columns) {
		if (columns.length == 1) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype));
		} else if (columns.length == 2) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype));
		} else if (columns.length == 3) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype));
		} else if (columns.length == 4) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype));
		} else if (columns.length == 5) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype));
		} else if (columns.length == 6) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype));
		} else if (columns.length == 7) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype));
		} else if (columns.length == 8) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype));
		} else if (columns.length == 9) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype));
		} else if (columns.length == 10) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype),
					cast(map.get(columns[9].column), columns[9].sqltype));
		} else if (columns.length == 11) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype),
					cast(map.get(columns[9].column), columns[9].sqltype),
					cast(map.get(columns[10].column), columns[10].sqltype));
		} else if (columns.length == 12) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype),
					cast(map.get(columns[9].column), columns[9].sqltype),
					cast(map.get(columns[10].column), columns[10].sqltype),
					cast(map.get(columns[11].column), columns[11].sqltype));
		} else if (columns.length == 13) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype),
					cast(map.get(columns[9].column), columns[9].sqltype),
					cast(map.get(columns[10].column), columns[10].sqltype),
					cast(map.get(columns[11].column), columns[11].sqltype),
					cast(map.get(columns[12].column), columns[12].sqltype));
		} else if (columns.length == 14) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype),
					cast(map.get(columns[9].column), columns[9].sqltype),
					cast(map.get(columns[10].column), columns[10].sqltype),
					cast(map.get(columns[11].column), columns[11].sqltype),
					cast(map.get(columns[12].column), columns[12].sqltype),
					cast(map.get(columns[13].column), columns[13].sqltype));
		} else if (columns.length == 15) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype),
					cast(map.get(columns[9].column), columns[9].sqltype),
					cast(map.get(columns[10].column), columns[10].sqltype),
					cast(map.get(columns[11].column), columns[11].sqltype),
					cast(map.get(columns[12].column), columns[12].sqltype),
					cast(map.get(columns[13].column), columns[13].sqltype),
					cast(map.get(columns[14].column), columns[14].sqltype));
		} else if (columns.length == 16) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype),
					cast(map.get(columns[9].column), columns[9].sqltype),
					cast(map.get(columns[10].column), columns[10].sqltype),
					cast(map.get(columns[11].column), columns[11].sqltype),
					cast(map.get(columns[12].column), columns[12].sqltype),
					cast(map.get(columns[13].column), columns[13].sqltype),
					cast(map.get(columns[14].column), columns[14].sqltype),
					cast(map.get(columns[15].column), columns[15].sqltype));
		} else if (columns.length > 16) {
			return (Serializable & MapToPairFunction<Map, Tuple>) (Map map) -> Tuple.tuple(cast(map.get(columns[0].column), columns[0].sqltype),
					cast(map.get(columns[1].column), columns[1].sqltype),
					cast(map.get(columns[2].column), columns[2].sqltype),
					cast(map.get(columns[3].column), columns[3].sqltype),
					cast(map.get(columns[4].column), columns[4].sqltype),
					cast(map.get(columns[5].column), columns[5].sqltype),
					cast(map.get(columns[6].column), columns[6].sqltype),
					cast(map.get(columns[7].column), columns[7].sqltype),
					cast(map.get(columns[8].column), columns[8].sqltype),
					cast(map.get(columns[9].column), columns[9].sqltype),
					cast(map.get(columns[10].column), columns[10].sqltype),
					cast(map.get(columns[11].column), columns[11].sqltype),
					cast(map.get(columns[12].column), columns[12].sqltype),
					cast(map.get(columns[13].column), columns[13].sqltype),
					cast(map.get(columns[14].column), columns[14].sqltype),
					cast(map.get(columns[15].column), columns[15].sqltype));
		}
		return null;
	}

	public Object cast(Object value, String sqloperatortype) {
		if (!Objects.isNull(sqloperatortype) && sqloperatortype.toLowerCase().startsWith("integer")
				|| !Objects.isNull(sqloperatortype) && sqloperatortype.toLowerCase().startsWith("decimal")) {
			return Long.valueOf((String)(String) value);
		}
		return value;
	}

	@SuppressWarnings("rawtypes")
	class FilteredRexVisitor implements RexVisitor {
		Stack stack;
		StreamPipeline<Map> mdp;
		EnumerableTableScan ets, etsleft, etsright;
		EnumerableHashJoin ehjleft, ehjright;
		RelOptTableImpl rtimpl = null;
		Map<String, String> indexcolumn;

		public FilteredRexVisitor(Stack stack, StreamPipeline mdp, EnumerableTableScan ets) {
			this.stack = stack;
			this.mdp = mdp;
			this.ets = ets;
			if (!Objects.isNull(ets)) {
				rtimpl = (RelOptTableImpl) ets.getTable();
				indexcolumn = tablecolumnindexsmap.get(rtimpl.getQualifiedName().get(1));
			}
		}

		public FilteredRexVisitor(Stack stack, EnumerableTableScan etsleft, EnumerableTableScan etsright) {
			this.stack = stack;
			this.etsleft = etsleft;
			this.etsright = etsright;
		}

		public FilteredRexVisitor(Stack stack, EnumerableHashJoin ehjleft, EnumerableTableScan etsright) {
			this.stack = stack;
			this.ehjleft = ehjleft;
			this.etsright = etsright;
		}

		public FilteredRexVisitor(Stack stack, EnumerableHashJoin ehjleft, EnumerableHashJoin ehjright) {
			this.stack = stack;
			this.ehjleft = ehjleft;
			this.ehjright = ehjright;
		}

		public FilteredRexVisitor(Stack stack, EnumerableTableScan etsleft, EnumerableHashJoin ehjright) {
			this.stack = stack;
			this.etsleft = etsleft;
			this.ehjright = ehjright;
		}

		@Override
		public Object visitInputRef(RexInputRef inputRef) {
			return null;
		}

		@Override
		public Object visitLocalRef(RexLocalRef localRef) {
			return null;
		}

		@Override
		public Object visitLiteral(RexLiteral literal) {
			return null;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object visitCall(RexCall call) {
			try {
				SqlOperator operator = call.getOperator();
				List<RexNode> operands = call.getOperands();
				Object left = operands.get(0);
				Object right = operands.get(1);
				if (left instanceof RexCall lrc) {
					if (!lrc.op.getName().equals("CAST")) {
						visitCall(lrc);
					}
				}
				if (right instanceof RexCall rrc) {
					if (!rrc.op.getName().equals("CAST")) {
						visitCall(rrc);
					}
				}
				if (operator.getName().equals("=")) {
					if (left instanceof RexCall rinref && right instanceof RexLiteral rl) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Object value = rl.getValue2();
						PredicateSerializable<Map> pred = val -> cast(val.get(column),
								rinref.getType().getSqlTypeName().getName()).equals(value);
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexCall rinref) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Object value = rl.getValue2();
						PredicateSerializable<Map> pred = val -> cast(val.get(column),
								rinref.getType().getSqlTypeName().getName()).equals(value);
						stack.push(pred);
					} else if (left instanceof RexInputRef rinref && right instanceof RexLiteral rl) {
						String column = indexcolumn.get(rinref.getName());
						Object value = rl.getValue2();
						PredicateSerializable<Map> pred = val -> val.get(column).equals(value);
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexInputRef rinref) {
						String column = indexcolumn.get(rinref.getName());
						Object value = rl.getValue2();
						PredicateSerializable<Map> pred = val -> val.get(column).equals(value);
						stack.push(pred);
					} else if (left instanceof RexInputRef rinrefleft && right instanceof RexInputRef rinrefright) {
						String column1 = globalindexcolumnsmap.get(rinrefleft.getName());
						String column2 = globalindexcolumnsmap.get(rinrefright.getName());
						PredicateSerializable<Map> pred = (val1) -> {
							return val1.get(column1).equals(val1.get(column2));
						};
						stack.push(pred);
					}
				} else if (operator.getName().equals("<>")) {
					if (left instanceof RexCall rinref && right instanceof RexLiteral rl) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Object value = rl.getValue2();
						PredicateSerializable<Map> pred = val -> !cast(val.get(column),
								rinref.getType().getSqlTypeName().getName()).equals(value);
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexCall rinref) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Object value = rl.getValue2();
						PredicateSerializable<Map> pred = val -> !cast(val.get(column),
								rinref.getType().getSqlTypeName().getName()).equals(value);
						stack.push(pred);
					} else if (left instanceof RexInputRef rinref && right instanceof RexLiteral rl) {
						String column = indexcolumn.get(rinref.getName());
						Object value = rl.getValue2();
						PredicateSerializable<Map> pred = (Serializable & PredicateSerializable<Map>) val -> !val.get(column).equals(value);
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexInputRef rinref) {
						String column = indexcolumn.get(rinref.getName());
						Object value = rl.getValue2();
						PredicateSerializable<Map> pred = val -> !val.get(column).equals(value);
						stack.push(pred);
					} else if (left instanceof RexInputRef rinrefleft && right instanceof RexInputRef rinrefright) {
						String column1 = globalindexcolumnsmap.get(rinrefleft.getName());
						String column2 = globalindexcolumnsmap.get(rinrefright.getName());
						PredicateSerializable<Map> pred = (val1) -> {
							return !val1.get(column1).equals(val1.get(column2));
						};
						stack.push(pred);
					}
				} else if (operator.getName().equals(">")) {
					if (left instanceof RexCall rinref && right instanceof RexLiteral rl) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Long value = (Long) rl.getValue2();
						PredicateSerializable<Map> pred = val -> ((Long) cast(val.get(column),
								rinref.getType().getSqlTypeName().getName())).compareTo(value) > 0;
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexCall rinref) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Long value = (Long) rl.getValue2();
						PredicateSerializable<Map> pred = val -> value.compareTo(
								((Long) cast(val.get(column), rinref.getType().getSqlTypeName().getName()))) > 0;
						stack.push(pred);
					}
				} else if (operator.getName().equals("<")) {
					if (left instanceof RexCall rinref && right instanceof RexLiteral rl) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Long value = (Long) rl.getValue2();
						PredicateSerializable<Map> pred = val -> ((Long) cast(val.get(column),
								rinref.getType().getSqlTypeName().getName())).compareTo(value) < 0;
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexCall rinref) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Long value = (Long) rl.getValue2();
						PredicateSerializable<Map> pred = val -> value.compareTo(
								((Long) cast(val.get(column), rinref.getType().getSqlTypeName().getName()))) < 0;
						stack.push(pred);
					}
				} else if (operator.getName().equals(">=")) {
					if (left instanceof RexCall rinref && right instanceof RexLiteral rl) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Long value = (Long) rl.getValue2();
						PredicateSerializable<Map> pred = val -> ((Long) cast(val.get(column),
								rinref.getType().getSqlTypeName().getName())).compareTo(value) >= 0;
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexCall rinref) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Long value = (Long) rl.getValue2();
						PredicateSerializable<Map> pred = val -> value.compareTo(
								((Long) cast(val.get(column), rinref.getType().getSqlTypeName().getName()))) >= 0;
						stack.push(pred);
					}
				} else if (operator.getName().equals("<=")) {
					if (left instanceof RexCall rinref && right instanceof RexLiteral rl) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Long value = (Long) rl.getValue2();
						PredicateSerializable<Map> pred = val -> ((Long) cast(val.get(column),
								rinref.getType().getSqlTypeName().getName())).compareTo(value) <= 0;
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexCall rinref) {
						String column = indexcolumn.get(((RexInputRef) rinref.getOperands().get(0)).getName());
						Long value = (Long) rl.getValue2();
						PredicateSerializable<Map> pred = val -> value.compareTo(
								((Long) cast(val.get(column), rinref.getType().getSqlTypeName().getName()))) <= 0;
						stack.push(pred);
					}
				} else if (operator.getName().equalsIgnoreCase("and")) {
					PredicateSerializable<Map> pred2 = (PredicateSerializable<Map>) stack.pop();
					PredicateSerializable<Map> pred1 = (PredicateSerializable<Map>) stack.pop();
					stack.push(pred1.and(pred2));
				} else if (operator.getName().equalsIgnoreCase("or")) {
					PredicateSerializable<Map> pred2 = (PredicateSerializable<Map>) stack.pop();
					PredicateSerializable<Map> pred1 = (PredicateSerializable<Map>) stack.pop();
					stack.push(pred1.or(pred2));
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return null;
		}

		@Override
		public Object visitOver(RexOver over) {
			return null;
		}

		@Override
		public Object visitCorrelVariable(RexCorrelVariable correlVariable) {
			return null;
		}

		@Override
		public Object visitDynamicParam(RexDynamicParam dynamicParam) {
			return null;
		}

		@Override
		public Object visitRangeRef(RexRangeRef rangeRef) {
			return null;
		}

		@Override
		public Object visitFieldAccess(RexFieldAccess fieldAccess) {
			return null;
		}

		@Override
		public Object visitSubQuery(RexSubQuery subQuery) {
			return null;
		}

		@Override
		public Object visitTableInputRef(RexTableInputRef fieldRef) {
			return null;
		}

		@Override
		public Object visitPatternFieldRef(RexPatternFieldRef fieldRef) {
			return null;
		}

	}

	
	@SuppressWarnings("rawtypes")
	class EnumerableProjectFilterHashJoinRexVisitor implements RexVisitor {
		Set<String> tablescancolumns;
		public EnumerableProjectFilterHashJoinRexVisitor(Set<String> tablescancolumns){
			this.tablescancolumns = tablescancolumns;
		}
		
		@Override
		public Object visitCall(RexCall call) {
			try {
				SqlOperator op = call.getOperator();
				if (op.getName().equalsIgnoreCase("cast")) {
					return null;
				}
				List<RexNode> operands = call.getOperands();
				Object left = operands.get(0);

				if (left instanceof RexCall lrc) {
					visitCall(lrc);
				}
				Object right = operands.size() > 1 ? operands.get(1) : null;
				if (Objects.isNull(right) && right instanceof RexCall rrc) {
					visitCall(rrc);
				}

				if (left instanceof RexInputRef rinrefleft) {
					tablescancolumns.add(rinrefleft.getName());
				}
				if (!Objects.isNull(right) && right instanceof RexInputRef rinrefright) {
					tablescancolumns.add(rinrefright.getName());
				}
				if (left instanceof RexCall rc) {
					String indexcolumn = ((RexInputRef) rc.getOperands().get(0)).getName();
					tablescancolumns.add(indexcolumn);
				}
				if (!Objects.isNull(right) && right instanceof RexCall rc) {
					String indexcolumn = ((RexInputRef) rc.getOperands().get(0)).getName();
					tablescancolumns.add(indexcolumn);
				}

			} catch (Exception ex) {
			}
			return null;
		}


		@Override
		public Object visitInputRef(RexInputRef inputRef) {
			if (!Objects.isNull(inputRef) && inputRef instanceof RexInputRef rinrefright) {
				tablescancolumns.add(inputRef.getName());
			}
			return null;
		}


		@Override
		public Object visitLocalRef(RexLocalRef localRef) {
			return null;
		}


		@Override
		public Object visitLiteral(RexLiteral literal) {
			return null;
		}


		@Override
		public Object visitOver(RexOver over) {
			return null;
		}


		@Override
		public Object visitCorrelVariable(RexCorrelVariable correlVariable) {
			return null;
		}


		@Override
		public Object visitDynamicParam(RexDynamicParam dynamicParam) {
			return null;
		}


		@Override
		public Object visitRangeRef(RexRangeRef rangeRef) {
			return null;
		}


		@Override
		public Object visitFieldAccess(RexFieldAccess fieldAccess) {
			return null;
		}


		@Override
		public Object visitSubQuery(RexSubQuery subQuery) {
			return null;
		}


		@Override
		public Object visitTableInputRef(RexTableInputRef fieldRef) {
			return null;
		}


		@Override
		public Object visitPatternFieldRef(RexPatternFieldRef fieldRef) {
			return null;
		}

	}
	
	@SuppressWarnings("rawtypes")
	class HashJoinRexVisitor extends FilteredRexVisitor {
		private String jointype;

		public HashJoinRexVisitor(Stack stack, StreamPipeline mdp, EnumerableTableScan ets) {
			super(stack, mdp, ets);
		}

		public HashJoinRexVisitor(Stack stack, EnumerableTableScan etsleft, EnumerableTableScan etsright,
				String jointype) {
			super(stack, etsleft, etsright);
			this.jointype = jointype;
		}

		public HashJoinRexVisitor(Stack stack, EnumerableHashJoin ehjleft, EnumerableTableScan etsright,
				String jointype) {
			super(stack, ehjleft, etsright);
			this.jointype = jointype;
		}

		public HashJoinRexVisitor(Stack stack, EnumerableTableScan etsleft, EnumerableHashJoin ehjright,
				String jointype) {
			super(stack, etsleft, ehjright);
			this.jointype = jointype;
		}

		public HashJoinRexVisitor(Stack stack, EnumerableHashJoin ehjleft, EnumerableHashJoin ehjright,
				String jointype) {
			super(stack, ehjleft, ehjright);
			this.jointype = jointype;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object visitCall(RexCall call) {
			try {
				SqlOperator operator = call.getOperator();
				List<RexNode> operands = call.getOperands();
				Object left = operands.get(0);
				Object right = operands.get(1);
				if (left instanceof RexCall lrc) {
					visitCall(lrc);
				}
				if (right instanceof RexCall rrc) {
					visitCall(rrc);
				}
				if (operator.getName().equals("=")) {
					if (left instanceof RexInputRef rinref && right instanceof RexLiteral rl) {
						String column = globalindexcolumnsmap.get(rinref.getName());
						Object value = rl.getValue2();
						String table = globalindextablesmap.get(rinref.getName());
						BiPredicateSerializable<Map, Map> pred;
						if (jointype.equals("inner")) {
							pred = (Serializable & JoinPredicate<Map, Map>) (val1, val2) -> {
								Map val = table.equals(etsleft.getTable().getQualifiedName().get(1)) ? val1
										: val2;
								return val.get(column).equals(value);
							};
						} else if (jointype.equals("left")) {
							pred = (Serializable & LeftOuterJoinPredicate<Map, Map>) (val1, val2) -> {
								Map val = table.equals(etsleft.getTable().getQualifiedName().get(1)) ? val1
										: val2;
								return val.get(column).equals(value);
							};
						} else {
							pred = (Serializable & RightOuterJoinPredicate<Map, Map>) (val1, val2) -> {
								Map val = table.equals(etsleft.getTable().getQualifiedName().get(1)) ? val1
										: val2;
								return val.get(column).equals(value);
							};
						}
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexInputRef rinref) {
						String column = globalindexcolumnsmap.get(rinref.getName());
						Object value = rl.getValue2();
						String table = globalindextablesmap.get(rinref.getName());
						BiPredicateSerializable<Map, Map> pred;
						if (jointype.equals("inner")) {
							pred = (Serializable & JoinPredicate<Map, Map>) (val1, val2) -> {
								Map val = table.equals(etsright.getTable().getQualifiedName().get(1)) ? val2
										: val1;
								return val.get(column).equals(value);
							};
						} else if (jointype.equals("left")) {
							pred = (Serializable & LeftOuterJoinPredicate<Map, Map>) (val1, val2) -> {
								Map val = table.equals(etsright.getTable().getQualifiedName().get(1)) ? val2
										: val1;
								return val.get(column).equals(value);
							};
						} else {
							pred = (Serializable & RightOuterJoinPredicate<Map, Map>) (val1, val2) -> {
								Map val = table.equals(etsright.getTable().getQualifiedName().get(1)) ? val2
										: val1;
								return val.get(column).equals(value);
							};
						}
						stack.push(pred);
					} else if (left instanceof RexInputRef rinrefleft && right instanceof RexInputRef rinrefright) {
						String column1 = globalindexcolumnsmap.get(rinrefleft.getName());
						String column2 = globalindexcolumnsmap.get(rinrefright.getName());
						String tableleft = globalindextablesmap.get(rinrefleft.getName());
						String tableright = globalindextablesmap.get(rinrefright.getName());
						String lefttable = !Objects.isNull(etsleft) ? etsleft.getTable().getQualifiedName().get(1)
								: hashjoinglobalindextablesmap.get(ehjleft).get(rinrefleft.getName());
						String righttable = !Objects.isNull(etsright) ? etsright.getTable().getQualifiedName().get(1)
								: hashjoinglobalindextablesmap.get(ehjright).get(rinrefright.getName());
						BiPredicateSerializable<Map, Map> pred;
						if (jointype.equals("inner")) {
							pred = (Serializable & JoinPredicate<Map, Map>) (val1, val2) -> {
								Map leftval = tableleft.equals(lefttable) ? val1 : val2;
								Map rightval = tableright.equals(righttable) ? val2 : val1;
								return leftval.get(column1).equals(rightval.get(column2));
							};
						} else if (jointype.equals("left")) {
							pred = (Serializable & LeftOuterJoinPredicate<Map, Map>) (val1, val2) -> {
								Map leftval = tableleft.equals(lefttable) ? val1 : val2;
								Map rightval = tableright.equals(righttable) ? val2 : val1;
								return leftval.get(column1).equals(rightval.get(column2));
							};
						} else {
							pred = (Serializable & RightOuterJoinPredicate<Map, Map>) (val1, val2) -> {
								Map leftval = tableleft.equals(lefttable) ? val1 : val2;
								Map rightval = tableright.equals(righttable) ? val2 : val1;
								return leftval.get(column1).equals(rightval.get(column2));
							};
						}
						stack.push(pred);
					}
				} else if (operator.getName().equalsIgnoreCase("and")) {
					if (jointype.equals("inner")) {
						JoinPredicate<Map, Map> pred2 = (JoinPredicate<Map, Map>) stack.pop();
						JoinPredicate<Map, Map> pred1 = (JoinPredicate<Map, Map>) stack.pop();
						stack.push(pred1.and(pred2));
					} else if (jointype.equals("left")) {
						LeftOuterJoinPredicate<Map, Map> pred2 = (LeftOuterJoinPredicate<Map, Map>) stack
								.pop();
						LeftOuterJoinPredicate<Map, Map> pred1 = (LeftOuterJoinPredicate<Map, Map>) stack
								.pop();
						stack.push(pred1.and(pred2));
					} else {
						RightOuterJoinPredicate<Map, Map> pred2 = (RightOuterJoinPredicate<Map, Map>) stack
								.pop();
						RightOuterJoinPredicate<Map, Map> pred1 = (RightOuterJoinPredicate<Map, Map>) stack
								.pop();
						stack.push(pred1.and(pred2));
					}
				} else if (operator.getName().equalsIgnoreCase("or")) {
					if (jointype.equals("inner")) {
						JoinPredicate<Map, Map> pred2 = (JoinPredicate<Map, Map>) stack.pop();
						JoinPredicate<Map, Map> pred1 = (JoinPredicate<Map, Map>) stack.pop();
						stack.push(pred1.or(pred2));
					} else if (jointype.equals("left")) {
						LeftOuterJoinPredicate<Map, Map> pred2 = (LeftOuterJoinPredicate<Map, Map>) stack
								.pop();
						LeftOuterJoinPredicate<Map, Map> pred1 = (LeftOuterJoinPredicate<Map, Map>) stack
								.pop();
						stack.push(pred1.or(pred2));
					} else {
						RightOuterJoinPredicate<Map, Map> pred2 = (RightOuterJoinPredicate<Map, Map>) stack
								.pop();
						RightOuterJoinPredicate<Map, Map> pred1 = (RightOuterJoinPredicate<Map, Map>) stack
								.pop();
						stack.push(pred1.or(pred2));
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return null;
		}

	}
	
	@SuppressWarnings("rawtypes")
	class HashMultipleJoinRexVisitor extends FilteredRexVisitor {
		private String jointype;

		public HashMultipleJoinRexVisitor(Stack stack, StreamPipeline mdp, EnumerableTableScan ets) {
			super(stack, mdp, ets);
		}

		public HashMultipleJoinRexVisitor(Stack stack, EnumerableTableScan etsleft, EnumerableTableScan etsright,
				String jointype) {
			super(stack, etsleft, etsright);
			this.jointype = jointype;
		}

		public HashMultipleJoinRexVisitor(Stack stack, EnumerableHashJoin ehjleft, EnumerableTableScan etsright,
				String jointype) {
			super(stack, ehjleft, etsright);
			this.jointype = jointype;
		}

		public HashMultipleJoinRexVisitor(Stack stack, EnumerableTableScan etsleft, EnumerableHashJoin ehjright,
				String jointype) {
			super(stack, etsleft, ehjright);
			this.jointype = jointype;
		}

		public HashMultipleJoinRexVisitor(Stack stack, EnumerableHashJoin ehjleft, EnumerableHashJoin ehjright,
				String jointype) {
			super(stack, ehjleft, ehjright);
			this.jointype = jointype;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object visitCall(RexCall call) {
			try {
				SqlOperator operator = call.getOperator();
				List<RexNode> operands = call.getOperands();
				Object left = operands.get(0);
				Object right = operands.get(1);
				if (left instanceof RexCall lrc) {
					visitCall(lrc);
				}
				if (right instanceof RexCall rrc) {
					visitCall(rrc);
				}
				if (operator.getName().equals("=")) {
					if (left instanceof RexInputRef rinref && right instanceof RexLiteral rl) {
						String column = globalindexcolumnsmap.get(rinref.getName());
						Object value = rl.getValue2();
						String table = globalindextablesmap.get(rinref.getName());
						BiPredicateSerializable<Map, Tuple2<Map,Object>> pred;
						if (jointype.equals("inner")) {
							pred = (Serializable & JoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object val = table.equals(etsleft.getTable().getQualifiedName().get(1)) ? val1
										: val2;
								if(val instanceof Map map) {
									return map.get(column).equals(value);
								}else if(val instanceof Tuple2 tup2) {
									return ((Map)tup2.v1).get(column).equals(value);
								}
								return false;
							};
						} else if (jointype.equals("left")) {
							pred = (Serializable & LeftOuterJoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object val = table.equals(etsleft.getTable().getQualifiedName().get(1)) ? val1
										: val2;
								if(val instanceof Map map) {
									return map.get(column).equals(value);
								}else if(val instanceof Tuple2 tup2) {
									return ((Map)tup2.v1).get(column).equals(value);
								}
								return false;
							};
						} else {
							pred = (Serializable & RightOuterJoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object val = table.equals(etsleft.getTable().getQualifiedName().get(1)) ? val1
										: val2;
								if(val instanceof Map map) {
									return map.get(column).equals(value);
								}else if(val instanceof Tuple2 tup2) {
									return ((Map)tup2.v1).get(column).equals(value);
								}
								return false;
							};
						}
						stack.push(pred);
					} else if (left instanceof RexLiteral rl && right instanceof RexInputRef rinref) {
						String column = globalindexcolumnsmap.get(rinref.getName());
						Object value = rl.getValue2();
						String table = globalindextablesmap.get(rinref.getName());
						BiPredicateSerializable<Map, Tuple2<Map,Object>> pred;
						if (jointype.equals("inner")) {
							pred = (Serializable & JoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object val = table.equals(etsright.getTable().getQualifiedName().get(1)) ? val2
										: val1;
								if(val instanceof Map map) {
									return map.get(column).equals(value);
								}else if(val instanceof Tuple2 tup2) {
									return ((Map)tup2.v1).get(column).equals(value);
								}
								return false;
							};
						} else if (jointype.equals("left")) {
							pred = (Serializable & LeftOuterJoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object val = table.equals(etsright.getTable().getQualifiedName().get(1)) ? val2
										: val1;
								if(val instanceof Map map) {
									return map.get(column).equals(value);
								}else if(val instanceof Tuple2 tup2) {
									return ((Map)tup2.v1).get(column).equals(value);
								}
								return false;
							};
						} else {
							pred = (Serializable & RightOuterJoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object val = table.equals(etsright.getTable().getQualifiedName().get(1)) ? val2
										: val1;
								if(val instanceof Map map) {
									return map.get(column).equals(value);
								}else if(val instanceof Tuple2 tup2) {
									return ((Map)tup2.v1).get(column).equals(value);
								}
								return false;
							};
						}
						stack.push(pred);
					} else if (left instanceof RexInputRef rinrefleft && right instanceof RexInputRef rinrefright) {
						String column1 = globalindexcolumnsmap.get(rinrefleft.getName());
						String column2 = globalindexcolumnsmap.get(rinrefright.getName());
						String tableleft = globalindextablesmap.get(rinrefleft.getName());
						String tableright = globalindextablesmap.get(rinrefright.getName());
						String lefttable = !Objects.isNull(etsleft) ? etsleft.getTable().getQualifiedName().get(1)
								: hashjoinglobalindextablesmap.get(ehjleft).get(rinrefleft.getName());
						String righttable = !Objects.isNull(etsright) ? etsright.getTable().getQualifiedName().get(1)
								: hashjoinglobalindextablesmap.get(ehjright).get(rinrefright.getName());
						BiPredicateSerializable<Map, Tuple2<Map,Object>> pred;
						if (jointype.equals("inner")) {
							pred = (Serializable & JoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object leftval = tableleft.equals(lefttable) ? val1 : val2;
								Object rightval = tableright.equals(righttable) ? val2 : val1;
								Object value1 = null,value2 = null;
								if(leftval instanceof Map map) {
									value1 = map.get(column1);
								}else if(leftval instanceof Tuple2 tupl2){
									value1 = ((Map)tupl2.v1).get(column1);
								}
								if(rightval instanceof Map map) {
									value2 = map.get(column2);
								}else if(leftval instanceof Tuple2 tupl2){
									value2 = ((Map)tupl2.v1).get(column2);
								}
								return value1.equals(value2);
							};
						} else if (jointype.equals("left")) {
							pred = (Serializable & LeftOuterJoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object leftval = tableleft.equals(lefttable) ? val1 : val2;
								Object rightval = tableright.equals(righttable) ? val2 : val1;
								Object value1 = null,value2 = null;
								if(leftval instanceof Map map) {
									value1 = map.get(column1);
								}else if(leftval instanceof Tuple2 tupl2){
									value1 = ((Map)tupl2.v1).get(column1);
								}
								if(rightval instanceof Map map) {
									value2 = map.get(column2);
								}else if(leftval instanceof Tuple2 tupl2){
									value2 = ((Map)tupl2.v1).get(column2);
								}
								return value1.equals(value2);
							};
						} else {
							pred = (Serializable & RightOuterJoinPredicate<Map, Tuple2<Map,Object>>) (val1, val2) -> {
								Object leftval = tableleft.equals(lefttable) ? val1 : val2;
								Object rightval = tableright.equals(righttable) ? val2 : val1;
								Object value1 = null,value2 = null;
								if(leftval instanceof Map map) {
									value1 = map.get(column1);
								}else if(leftval instanceof Tuple2 tupl2){
									value1 = ((Map)tupl2.v1).get(column1);
								}
								if(rightval instanceof Map map) {
									value2 = map.get(column2);
								}else if(leftval instanceof Tuple2 tupl2){
									value2 = ((Map)tupl2.v1).get(column2);
								}
								return value1.equals(value2);
							};
						}
						stack.push(pred);
					}
				} else if (operator.getName().equalsIgnoreCase("and")) {
					if (jointype.equals("inner")) {
						JoinPredicate<Map, Tuple2<Map,Object>> pred2 = (JoinPredicate<Map, Tuple2<Map,Object>>) stack.pop();
						JoinPredicate<Map, Tuple2<Map,Object>> pred1 = (JoinPredicate<Map, Tuple2<Map,Object>>) stack.pop();
						stack.push(pred1.and(pred2));
					} else if (jointype.equals("left")) {
						LeftOuterJoinPredicate<Map, Tuple2<Map,Object>> pred2 = (LeftOuterJoinPredicate<Map, Tuple2<Map,Object>>) stack
								.pop();
						LeftOuterJoinPredicate<Map, Tuple2<Map,Object>> pred1 = (LeftOuterJoinPredicate<Map, Tuple2<Map,Object>>) stack
								.pop();
						stack.push(pred1.and(pred2));
					} else {
						RightOuterJoinPredicate<Map, Tuple2<Map,Object>> pred2 = (RightOuterJoinPredicate<Map, Tuple2<Map,Object>>) stack
								.pop();
						RightOuterJoinPredicate<Map, Tuple2<Map,Object>> pred1 = (RightOuterJoinPredicate<Map, Tuple2<Map,Object>>) stack
								.pop();
						stack.push(pred1.and(pred2));
					}
				} else if (operator.getName().equalsIgnoreCase("or")) {
					if (jointype.equals("inner")) {
						JoinPredicate<Map, Tuple2<Map,Object>> pred2 = (JoinPredicate<Map, Tuple2<Map,Object>>) stack.pop();
						JoinPredicate<Map, Tuple2<Map,Object>> pred1 = (JoinPredicate<Map, Tuple2<Map,Object>>) stack.pop();
						stack.push(pred1.or(pred2));
					} else if (jointype.equals("left")) {
						LeftOuterJoinPredicate<Map, Tuple2<Map,Object>> pred2 = (LeftOuterJoinPredicate<Map, Tuple2<Map,Object>>) stack
								.pop();
						LeftOuterJoinPredicate<Map, Tuple2<Map,Object>> pred1 = (LeftOuterJoinPredicate<Map, Tuple2<Map,Object>>) stack
								.pop();
						stack.push(pred1.or(pred2));
					} else {
						RightOuterJoinPredicate<Map, Tuple2<Map,Object>> pred2 = (RightOuterJoinPredicate<Map, Tuple2<Map,Object>>) stack
								.pop();
						RightOuterJoinPredicate<Map, Tuple2<Map,Object>> pred1 = (RightOuterJoinPredicate<Map, Tuple2<Map,Object>>) stack
								.pop();
						stack.push(pred1.or(pred2));
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return null;
		}

	}

	public SimpleTable getSimpleTable(String tablename, String[] fields, SqlTypeName[] types) {
		SimpleTable.Builder builder = SimpleTable.newBuilder(tablename);
		List<String> fieldsl = Arrays.asList(fields);
		int typecount = 0;
		for (String field : fieldsl) {
			builder = builder.addField(field, types[typecount]);
			typecount++;
		}
		return builder.withRowCount(60000L).build();
	}

	private void print(String header, RelNode relTree) {
		StringWriter sw = new StringWriter();

		sw.append(header).append(":").append("\n");

		RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

		relTree.explain(relWriter);
		
		System.out.println(sw.toString());
		
	}
}
