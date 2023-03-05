package com.github.mdc.stream.sql.build;

import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.functions.MapFunction;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.StreamPipeline;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
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
	    if(select.getSelectBody() instanceof PlainSelect plainSelect) {
		    if (plainSelect.getSelectItems().size() == 1 && plainSelect.getSelectItems().get(0).toString().equals("*")) {
		    	Table table = (Table) plainSelect.getFromItem();
		    	String[] columns = tablecolumnsmap.get(table.getName());
		    	Expression expression = plainSelect.getWhere();		    	
		    	StreamPipeline<CSVRecord> pipeline = StreamPipeline.newCsvStreamHDFS(hdfs,
						tablefoldermap.get(table.getName()), this.pc,
						columns);
		    	if(nonNull(expression) && expression instanceof BinaryExpression bex) {
		    		pipeline = buildPredicate(pipeline, bex);
		    	}
		    	return pipeline.map((Serializable & MapFunction<CSVRecord,Map<String,Object>>)(record->{
					Map<String,Object> columnWithValues= new HashMap<>();
					List<String> columnsl = Arrays.asList(columns);
					columnsl.forEach(column->{
						columnWithValues.put(column, record.get(column));
					});
					return columnWithValues;
				}));
		    } else {
		    	List<SelectItem> selectItems = plainSelect.getSelectItems();
		    	List<String> columns = new ArrayList<>();
		        for (SelectItem selectItem : selectItems) {
		            if (selectItem instanceof SelectExpressionItem) {
		                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
		                if (selectExpressionItem.getExpression() instanceof Column column) {
		                    String columnName = column.getColumnName();
		                    columns.add(columnName);
		                }
		               
		            }
		        }
		        Table table = (Table) plainSelect.getFromItem();
		        Expression expression = plainSelect.getWhere();
		        StreamPipeline<CSVRecord> pipeline = StreamPipeline.newCsvStreamHDFS(hdfs,
						tablefoldermap.get(table.getName()), this.pc,
						tablecolumnsmap.get(table.getName()));
		    	if(nonNull(expression) && expression instanceof BinaryExpression bex) {
		    		pipeline = buildPredicate(pipeline, bex);
		    	}
		    	return pipeline.map((Serializable & MapFunction<CSVRecord,Map<String,Object>>)(record->{
							Map<String,Object> columnWithValues= new HashMap<>();
							columns.forEach(column->{
								columnWithValues.put(column, record.get(column));
							});
							return columnWithValues;
						}));
		    }
	    }
	    return statement.toString();
	}
	
	public static StreamPipeline<CSVRecord> buildPredicate(StreamPipeline<CSVRecord> pipeline, Expression expression) throws PipelineException {
		return pipeline.filter(row -> evaluateExpression(expression, row));
	}

	public static boolean evaluateExpression(Expression expression, CSVRecord row) {
	    if (expression instanceof BinaryExpression) {
	        BinaryExpression binaryExpression = (BinaryExpression) expression;
	        String operator = binaryExpression.getStringExpression();
	        Expression leftExpression = binaryExpression.getLeftExpression();
	        if(leftExpression instanceof BinaryExpression bex) {
	        	return evaluateExpression(bex, row);
	        }
	        Expression rightExpression = binaryExpression.getRightExpression();
	        if(rightExpression instanceof BinaryExpression bex) {
	        	return evaluateExpression(bex, row);
	        }
	        String leftValue = getValueString(leftExpression, row);
	        String rightValue = getValueString(rightExpression, row);

	        switch (operator.toUpperCase()) {
	            case "AND":
	                return evaluateExpression(leftExpression, row) && evaluateExpression(rightExpression, row);
	            case "OR":
	                return evaluateExpression(leftExpression, row) || evaluateExpression(rightExpression, row);
	            case ">":
	                return Double.parseDouble(leftValue) > Double.parseDouble(rightValue);
	            case ">=":
	                return Double.parseDouble(leftValue) >= Double.parseDouble(rightValue);
	            case "<":
	                return Double.parseDouble(leftValue) < Double.parseDouble(rightValue);
	            case "<=":
	                return Double.parseDouble(leftValue) <= Double.parseDouble(rightValue);
	            case "=":
	                return Double.parseDouble(leftValue) == Double.parseDouble(rightValue);
	            case "<>":
	                return !leftValue.equals(rightValue);
	            default:
	                throw new UnsupportedOperationException("Unsupported operator: " + operator);
	        }
	    } else {
	        String value = getValueString(expression, row);
	        return Boolean.parseBoolean(value);
	    }
	}


	private static String getValueString(Expression expression, CSVRecord row) {
		if (expression instanceof StringValue) {
	        return ((StringValue) expression).getValue();
	    } else if (expression instanceof DoubleValue) {
	        return Double.toString(((DoubleValue) expression).getValue());
	    } else {
	        Column column = (Column) expression;
	        String columnName = column.getColumnName();
	        return (String) row.get(columnName);
	    }
	}

	
}
