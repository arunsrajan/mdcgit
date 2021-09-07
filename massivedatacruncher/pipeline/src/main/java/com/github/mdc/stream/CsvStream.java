package com.github.mdc.stream;

import java.util.ArrayList;
import java.util.Stack;
import java.util.stream.Collectors;

import com.github.mdc.stream.functions.MapFunction;
import com.github.mdc.stream.functions.PeekConsumer;
import com.github.mdc.stream.functions.PredicateSerializable;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * 
 * @author arun
 * This class is a stream with parametered types for pipeline api to process csv files.
 * @param <I1>
 * @param <I2>
 */
public final class CsvStream<I1,I2> extends MassiveDataPipeline<I1> {

	@SuppressWarnings({ "rawtypes" })
	public CsvStream(MassiveDataPipeline root,CsvOptions csvOptions) {
		this.root = root;
		this.task = csvOptions;
		root.childs.add(this);
		this.parents.add(root);
		this.protocol = root.protocol;
	}
	
	@SuppressWarnings({ "rawtypes" })
	public CsvStream(MassiveDataPipeline root,PeekConsumer peekconsumer) {
		this.root = root;
		this.task = peekconsumer;
		root.childs.add(this);
		this.parents.add(root);
		this.protocol = root.protocol;
	}
	
}
