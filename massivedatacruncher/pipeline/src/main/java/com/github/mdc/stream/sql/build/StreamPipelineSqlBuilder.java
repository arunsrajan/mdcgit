package com.github.mdc.stream.sql.build;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVRecord;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.functions.MapFunction;
import com.github.mdc.stream.NumPartitions;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.StreamPipeline;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;  

public class StreamPipelineSqlBuilder implements Serializable{	
	private static final long serialVersionUID = -8585345445522511086L;
	private static Logger log = LoggerFactory.getLogger(StreamPipelineSqlBuilder.class); 
	String sql;
	ConcurrentMap<String, String> tablefoldermap = new ConcurrentHashMap<>();
	ConcurrentMap<String, String[]> tablecolumnsmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, SqlTypeName[]> tablecolumntypesmap = new ConcurrentHashMap<>();
	String hdfs;
	transient PipelineConfig pc;
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
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
	    Statement statement = parserManager.parse(new StringReader(sql));
		return new StreamPipelineSql(execute(statement));
	}
	
	protected Object execute(Object statement) throws JSQLParserException, PipelineException {
		if (!(statement instanceof Select)) {
			throw new IllegalArgumentException("Only SELECT statements are supported");
		}
		Select select = (Select) statement;
		if (select.getSelectBody() instanceof PlainSelect plainSelect) {
			List<SelectItem> selectItems = plainSelect.getSelectItems();
			List<Function> functions = new ArrayList<>();
			Map<String, Set<String>> tablerequiredcolumns = new ConcurrentHashMap<>();

			Table table = (Table) plainSelect.getFromItem();
			if (plainSelect.getSelectItems().get(0).toString().equals("*")
					|| plainSelect.getSelectItems().get(0).toString().equals("count(*)")) {
				tablerequiredcolumns.put(table.getName(),
						new LinkedHashSet<>(Arrays.asList(tablecolumnsmap.get(table.getName()))));
				List<Join> joins = plainSelect.getJoins();
				if (CollectionUtils.isNotEmpty(joins)) {
					joins.parallelStream().forEach(join -> {
						String tablename = ((Table) join.getRightItem()).getName();
						tablerequiredcolumns.put(tablename,
								new LinkedHashSet<>(Arrays.asList(tablecolumnsmap.get(tablename))));
					});
				}
				if (plainSelect.getSelectItems().get(0)instanceof SelectExpressionItem seitems && seitems.getExpression() instanceof Function function) {
					functions.add(function);
				}
			} else {
				for (SelectItem selectItem : selectItems) {
					if (selectItem instanceof SelectExpressionItem) {
						SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
						if (selectExpressionItem.getExpression() instanceof Column column) {
							if (nonNull(column.getTable())) {
								Set<String> requiredcolumns = tablerequiredcolumns.get(column.getTable().getName());
								if (isNull(requiredcolumns)) {
									requiredcolumns = new LinkedHashSet<>();
									tablerequiredcolumns.put(column.getTable().getName(), requiredcolumns);
								}
								requiredcolumns.add(column.getColumnName());
							}
						} else if (selectExpressionItem.getExpression() instanceof Function function) {
							functions.add(function);
						}
					}
				}
			}

			StreamPipeline<CSVRecord> pipeline = StreamPipeline
					.newCsvStreamHDFS(hdfs, tablefoldermap.get(table.getName()), this.pc,
							tablecolumnsmap.get(table.getName()))
					.map((Serializable & MapFunction<CSVRecord, CSVRecord>) record -> record);
			Map<String, List<Expression>> expressionsTable = new ConcurrentHashMap<>();
			Map<String, Set<String>> tablerequiredAllcolumns = new ConcurrentHashMap<>();
			Map<String, List<Expression>> joinTableExpressions = new ConcurrentHashMap<>();
			getRequiredColumnsForAllTables(plainSelect, tablerequiredAllcolumns, expressionsTable,
					joinTableExpressions);
			addAllRequiredColumnsFromSelectItems(tablerequiredAllcolumns, tablerequiredcolumns);
			List<Expression> expressionLeftTable = expressionsTable.get(table.getName());
			if(CollectionUtils.isEmpty(expressionLeftTable)) {
				expressionLeftTable = expressionsTable.get(table.getName()+"-"+table.getName());
			}
			if (!CollectionUtils.isEmpty(expressionLeftTable)) {
				BinaryExpression exp = (BinaryExpression) expressionLeftTable.get(0);
				for (int expcount = 1; expcount < expressionLeftTable.size(); expcount++) {
					exp = new AndExpression(exp, expressionLeftTable.get(expcount));
				}
				pipeline = buildPredicate(pipeline, exp);
			}
			if (nonNull(plainSelect.getJoins())) {
				StreamPipeline<Map<String, Object>> pipelineLeft = pipeline
						.map((Serializable & MapFunction<CSVRecord, Map<String, Object>>) (record -> {
							Map<String, Object> columnWithValues = new HashMap<>();
							Set<String> columnsLeft = tablerequiredAllcolumns.get(table.getName());
							columnsLeft.forEach(column -> {
								columnWithValues.put(column, record.get(column));
							});
							return columnWithValues;
						}));
				for(Join join:plainSelect.getJoins()) {
					String tablename = ((Table) join.getRightItem()).getName();
					StreamPipeline<CSVRecord> joinpipe1 = StreamPipeline.newCsvStreamHDFS(hdfs,
							tablefoldermap.get(tablename), this.pc, tablecolumnsmap.get(tablename));
					List<Expression> expressionRightTable = expressionsTable.get(tablename);
					if (!CollectionUtils.isEmpty(expressionRightTable)) {
						BinaryExpression exp = (BinaryExpression) expressionRightTable.get(0);
						for (int expcount = 1; expcount < expressionRightTable.size(); expcount++) {
							exp = new AndExpression(exp, expressionRightTable.get(expcount));
						}
						joinpipe1 = buildPredicate(joinpipe1, exp);
					}
					StreamPipeline<Map<String, Object>> joinpipe = joinpipe1
							.map((Serializable & MapFunction<CSVRecord, Map<String, Object>>) (record -> {
								Map<String, Object> columnWithValues = new HashMap<>();
								Set<String> columnsLeft = tablerequiredAllcolumns.get(tablename);
								columnsLeft.forEach(column -> {
									columnWithValues.put(column, record.get(column));
								});
								return columnWithValues;
							}));
					pipelineLeft = buildJoinPredicate(pipelineLeft, joinpipe, table.getName(), tablename,
							join.getOnExpressions().iterator().next(), join.isInner(), join.isLeft(), join.isRight())
							.filter(tuple2 -> {
								String table1 = table.getName();
								String table2 = tablename;
								Expression express = joinTableExpressions.get(table1 + "-" + table2).get(0);
								return evaluateExpressionJoin(express, table1, table2, tuple2.v1, tuple2.v2);
							}).map(tuple2 -> {
								tuple2.v1.putAll(tuple2.v2);
								tablerequiredcolumns.get(table.getName()).addAll(tablerequiredcolumns.get(tablename));
								tuple2.v1.keySet().retainAll(tablerequiredcolumns.get(table.getName()));
								return tuple2.v1;
							});
				}
				if (!CollectionUtils.isEmpty(functions)) {
					if (functions.get(0).getName().toLowerCase().startsWith("count")) {
						return pipelineLeft.count(new NumPartitions(1));
					}
				}
				return pipelineLeft;
			}

			if (!CollectionUtils.isEmpty(functions)) {				
				if (functions.get(0).getName().toLowerCase().startsWith("sum")) {
					Column column = getColumn(functions);
					return pipeline.mapToInt(record -> Integer.parseInt(getValueString(column, record))).sum();
				} else if (functions.get(0).getName().toLowerCase().startsWith("min")) {
					Column column = getColumn(functions);
					return pipeline.mapToInt(record -> Integer.parseInt(getValueString(column, record))).min();
				} else if (functions.get(0).getName().toLowerCase().startsWith("max")) {
					Column column = getColumn(functions);
					return pipeline.mapToInt(record -> Integer.parseInt(getValueString(column, record))).max();
				} else if (functions.get(0).getName().toLowerCase().startsWith("count")) {
					return pipeline.count(new NumPartitions(1));
				}
			}
			if (nonNull(joinTableExpressions.get(table.getName() + "-" + table.getName()))) {
				pipeline = pipeline.filter(record -> {
					String table1 = table.getName();
					String table2 = table.getName();
					Expression express = joinTableExpressions.get(table1 + "-" + table2).get(0);
					return evaluateExpression(express, record);
				});
			}
			return pipeline.map((Serializable & MapFunction<CSVRecord, Map<String, Object>>) (record -> {
				Map<String, Object> columnWithValues = new HashMap<>();
				Set<String> columns = tablerequiredcolumns.get(table.getName());
				columns.forEach(column -> {
					columnWithValues.put(column, record.get(column));
				});
				return columnWithValues;
			}));
		}
		return statement.toString();
	}
	
	public Column getColumn(List<Function> functions) {
		List<Expression> parameters = functions.get(0).getParameters().getExpressions();
		return (Column)parameters.get(0);
	}
	
	public void addAllRequiredColumnsFromSelectItems(Map<String, Set<String>> allRequiredColumns,Map<String, Set<String>> allColumnsSelectItems) {
		allColumnsSelectItems.keySet().parallelStream().forEach(key->{
			Set<String> allReqColumns = allRequiredColumns.get(key);
			Set<String> allSelectItem = allColumnsSelectItems.get(key);
			if(isNull(allReqColumns)) {
				allRequiredColumns.put(key, allSelectItem);
			} else {
				allReqColumns.addAll(allSelectItem);
			}
		});
	}
	
	public static StreamPipeline<CSVRecord> buildPredicate(StreamPipeline<CSVRecord> pipeline, Expression expression) throws PipelineException {
		return pipeline.filter(row -> evaluateExpression(expression, row));
	}
	
	
	public static StreamPipeline<Tuple2<Map<String,Object>,Map<String,Object>>> buildJoinPredicate(StreamPipeline<Map<String,Object>> pipeline1, StreamPipeline<Map<String,Object>> pipeline2,
			String table1, String table2,
			Expression expression, Boolean inner, 
			Boolean left, Boolean right ) throws PipelineException {
		if(inner) {
			return pipeline1.join(pipeline2, (row1, row2) -> evaluateExpressionJoin(expression,table1,table1, row1, row2));
		} else if(left) {
			return pipeline1.leftOuterjoin(pipeline2, (row1, row2) -> evaluateExpressionJoin(expression,table1,table1, row1, row2));
		} else {
			return pipeline1.rightOuterjoin(pipeline2, (row1, row2) -> evaluateExpressionJoin(expression,table1,table1, row1, row2));
		}
	}

	public static boolean evaluateExpression(Expression expression, CSVRecord row) {
	    if (expression instanceof BinaryExpression) {
	        BinaryExpression binaryExpression = (BinaryExpression) expression;
	        String operator = binaryExpression.getStringExpression();
	        Expression leftExpression = binaryExpression.getLeftExpression();
	        Expression rightExpression = binaryExpression.getRightExpression();	        

	        switch (operator.toUpperCase()) {
	            case "AND":
	                return evaluateExpression(leftExpression, row) && evaluateExpression(rightExpression, row);
	            case "OR":
	                return evaluateExpression(leftExpression, row) || evaluateExpression(rightExpression, row);
	            case ">":
	            	String leftValue = getValueString(leftExpression, row);
	    	        String rightValue = getValueString(rightExpression, row);
	                return Double.parseDouble(leftValue) > Double.parseDouble(rightValue);
	            case ">=":
	            	leftValue = getValueString(leftExpression, row);
	    	        rightValue = getValueString(rightExpression, row);
	                return Double.parseDouble(leftValue) >= Double.parseDouble(rightValue);
	            case "<":
	            	leftValue = getValueString(leftExpression, row);
	    	        rightValue = getValueString(rightExpression, row);
	                return Double.parseDouble(leftValue) < Double.parseDouble(rightValue);
	            case "<=":
	            	leftValue = getValueString(leftExpression, row);
	    	        rightValue = getValueString(rightExpression, row);
	                return Double.parseDouble(leftValue) <= Double.parseDouble(rightValue);
	            case "=":
	            	Object leftValueO = getValueString(leftExpression, row);
	    	        Object rightValueO = getValueString(rightExpression, row);
	                return leftValueO.equals(rightValueO);
	            case "<>":
	            	leftValue = getValueString(leftExpression, row);
	    	        rightValue = getValueString(rightExpression, row);
	                return !leftValue.equals(rightValue);
	            default:
	                throw new UnsupportedOperationException("Unsupported operator: " + operator);
	        }
	    } else {
	        String value = getValueString(expression, row);
	        return Boolean.parseBoolean(value);
	    }
	}
	
	
	public static boolean evaluateExpressionJoin(Expression expression, String table1,String table2, Map<String,Object> row1, Map<String,Object> row2) {
	    if (expression instanceof BinaryExpression) {
	        BinaryExpression binaryExpression = (BinaryExpression) expression;
	        String operator = binaryExpression.getStringExpression();
	        Expression leftExpression = binaryExpression.getLeftExpression();
	        Expression rightExpression = binaryExpression.getRightExpression();	        
	        Map<String,Object> rowleft = null;
	        if (leftExpression instanceof Column column && column.getTable().getName().equals(table1)) {
	        	rowleft = row1;
	        } else {
	        	rowleft = row2;
	        }
	        Map<String,Object> rowright = null;
	        if (rightExpression instanceof Column column && column.getTable().getName().equals(table1)) {
	        	rowright = row1;	            
	        } else {
	        	rowright = row2;
	        }
	        switch (operator.toUpperCase()) {
	            case "AND":
	                return evaluateExpressionJoin(leftExpression,table1, table2, row1, row2) && evaluateExpressionJoin(rightExpression,table1, table2, row1, row2);
	            case "OR":
	                return evaluateExpressionJoin(leftExpression,table1, table2, row1, row2) || evaluateExpressionJoin(rightExpression,table1, table2, row1, row2);
	            case ">":
	            	String leftValue = getValueString(leftExpression, rowleft);
	    	        String rightValue = getValueString(rightExpression, rowright);
	                return Double.parseDouble(leftValue) > Double.parseDouble(rightValue);
	            case ">=":
	            	leftValue = getValueString(leftExpression, rowleft);
	    	        rightValue = getValueString(rightExpression, rowright);
	                return Double.parseDouble(leftValue) >= Double.parseDouble(rightValue);
	            case "<":
	            	leftValue = getValueString(leftExpression, rowleft);
	    	        rightValue = getValueString(rightExpression, rowright);
	                return Double.parseDouble(leftValue) < Double.parseDouble(rightValue);
	            case "<=":
	            	leftValue = getValueString(leftExpression, rowleft);
	    	        rightValue = getValueString(rightExpression, rowright);
	                return Double.parseDouble(leftValue) <= Double.parseDouble(rightValue);
	            case "=":
	            	Object leftValueO = getValueString(leftExpression, rowleft);
	    	        Object rightValueO = getValueString(rightExpression, rowright);
	                return leftValueO.equals(rightValueO);
	            case "<>":
	            	leftValue = getValueString(leftExpression, rowleft);
	    	        rightValue = getValueString(rightExpression, rowright);
	                return !leftValue.equals(rightValue);
	            default:
	                throw new UnsupportedOperationException("Unsupported operator: " + operator);
	        }
	    } else {
	    	Map<String,Object> row = null;
	    	if (expression instanceof Column column && column.getTable().getName().equals(table1)) {
	    		row = row1;	            
	        } else {
	        	row = row2;
	        }
	        String value = getValueString(expression, row);
	        return Boolean.parseBoolean(value);
	    }
	}


	private static String getValueString(Expression expression, CSVRecord row) {
		if (expression instanceof LongValue) {
	        return String.valueOf(((LongValue) expression).getValue());
	    } else if (expression instanceof StringValue) {
	        return ((StringValue) expression).getValue();
	    } else if (expression instanceof DoubleValue) {
	        return Double.toString(((DoubleValue) expression).getValue());
	    } else {
	        Column column = (Column) expression;
	        String columnName = column.getColumnName();
	        return (String) row.get(columnName);
	    }
	}

	private static String getValueString(Expression expression, Map<String,Object> row) {
		if (expression instanceof LongValue) {
	        return String.valueOf(((LongValue) expression).getValue());
	    } else if (expression instanceof StringValue) {
	        return ((StringValue) expression).getValue();
	    } else if (expression instanceof DoubleValue) {
	        return Double.toString(((DoubleValue) expression).getValue());
	    } else {
	        Column column = (Column) expression;
	        String columnName = column.getColumnName();
	        return (String) row.get(columnName);
	    }
	}
	
	public void getRequiredColumnsForAllTables(PlainSelect plainSelect, Map<String, Set<String>> tablerequiredcolumns,
			Map<String, List<Expression>> expressionsTable, Map<String,List<Expression>> joinTableExpressions) {

		List<Expression> expressions = new Vector<>();
		
		if(nonNull(plainSelect.getJoins())) {
			plainSelect.getJoins().parallelStream().map(join->join.getOnExpression())
			.forEach(expression->expressions.add(expression));
		}
		if(nonNull(plainSelect.getWhere())) {
			getColumnsFromBinaryExpression(plainSelect.getWhere(), tablerequiredcolumns, expressionsTable, expressionsTable);
		}
		for (Expression onExpression : expressions) {
			getColumnsFromBinaryExpression(onExpression, tablerequiredcolumns, joinTableExpressions, joinTableExpressions);
		}
	}
	
	public void getColumnsFromBinaryExpression(Expression expression, Map<String, Set<String>> tablerequiredcolumns, Map<String, List<Expression>> expressions,
			Map<String,List<Expression>> joinTableExpressions) {
		if (expression instanceof BinaryExpression binaryExpression) {
			Expression leftExpression = binaryExpression.getLeftExpression();
			if (leftExpression instanceof BinaryExpression) {
				getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions, joinTableExpressions);
			}
			Expression rightExpression = binaryExpression.getRightExpression();
			if (rightExpression instanceof BinaryExpression) {
				getColumnsFromBinaryExpression(rightExpression, tablerequiredcolumns, expressions,joinTableExpressions);
			}
			if(leftExpression instanceof Column column1 && !(rightExpression instanceof Column)) {
				if(nonNull(column1.getTable())) {
					List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
					if (isNull(expressionsTable)) {
						expressionsTable = new Vector<>();
						expressions.put(column1.getTable().getName(), expressionsTable);
					}
					expressionsTable.add(binaryExpression);
				}
			} else if(!(leftExpression instanceof Column) && rightExpression instanceof Column column1) {
				if(nonNull(column1.getTable())) {
					List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
					if (isNull(expressionsTable)) {
						expressionsTable = new Vector<>();
						expressions.put(column1.getTable().getName(), expressionsTable);
					}
					expressionsTable.add(binaryExpression);
				}
			} else if(leftExpression instanceof Column col1 && rightExpression instanceof Column col2) {
				List<Expression> expressionsTable = joinTableExpressions.get(col1.getTable().getName()+"-"+col2.getTable().getName());
				if (isNull(expressionsTable)) {
					expressionsTable = new Vector<>();
					joinTableExpressions.put(col1.getTable().getName()+"-"+col2.getTable().getName(), expressionsTable);
				}
				expressionsTable.add(binaryExpression);
			}
			// Check if either left or right expression is a column
			if (leftExpression instanceof Column column) {
				if(nonNull(column.getTable())) {
					Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
					if (isNull(columns)) {
						columns = new LinkedHashSet<>();
						tablerequiredcolumns.put(column.getTable().getName(), columns);
					}
					columns.add(column.getColumnName());
				}
			}

			if (rightExpression instanceof Column column) {
				if(nonNull(column.getTable())) {
					Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
					if (isNull(columns)) {
						columns = new LinkedHashSet<>();
						tablerequiredcolumns.put(column.getTable().getName(), columns);
					}
					columns.add(column.getColumnName());
				}
			}
		} else if (expression instanceof Column column) {
			Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
			if (isNull(columns)) {
				columns = new LinkedHashSet<>();
				tablerequiredcolumns.put(column.getTable().getName(), columns);
			}
			columns.add(column.getColumnName());
		}
	}
	
	
}
