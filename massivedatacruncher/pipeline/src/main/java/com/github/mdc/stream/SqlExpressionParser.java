package com.github.mdc.stream;

import java.util.function.Function;

import org.apache.commons.csv.CSVRecord;
import org.jooq.lambda.tuple.Tuple;

import com.github.mdc.common.functions.MapToPairFunction;
import com.github.mdc.common.functions.PredicateSerializable;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

public class SqlExpressionParser {
	private SqlExpressionParser() {}
	public static PredicateSerializable<CSVRecord> equals(Column col1, Column col2) {
		return (CSVRecord csv) -> csv.get(col1.getColumnName()).equals(csv.get(col2.getColumnName()));
	}

	private static PredicateSerializable<CSVRecord> equals(Object col1, Column col2) {
		
		return (CSVRecord csv) -> {
			Object constvalue = getValue(col1);
			return constvalue.equals(getCsvValue(constvalue,col2,csv));
		};
	}
	
	
	private static PredicateSerializable<CSVRecord> equals(Column col1,Object col2) {
		
		return (CSVRecord csv) -> {
			Object constvalue = getValue(col2);
			return getCsvValue(constvalue,col1,csv).equals(constvalue);
		};
	}
	private static Object getValue(Object col1) {
		var constvalue = new Object();
		if(col1 instanceof LongValue longvalue) {
			constvalue = longvalue.getValue();
		}
		else if(col1 instanceof StringValue stringvalue) {
			constvalue = stringvalue.getValue();
		}
		else if(col1 instanceof DoubleValue doublevalue) {
			constvalue = doublevalue.getValue();
		}
		return constvalue;
	}
	private static Object getCsvValue(Object col1,Column col2,CSVRecord csv) {
		var constvalue = new Object();
		if(col1 instanceof Long) {
			constvalue = Long.parseLong(csv.get(col2.getColumnName()));
		}
		else if(col1 instanceof String) {
			constvalue = csv.get(col2.getColumnName());
		}
		else if(col1 instanceof Double) {
			constvalue = Double.parseDouble(csv.get(col2.getColumnName()));
		}
		return constvalue;
	}
	

	
	public static PredicateSerializable<CSVRecord> equals(Object col1, Object col2) {
		
		if(col1 instanceof Column col) {
			return SqlExpressionParser.equals(col, col2);
		} else if(col2 instanceof Column col) {
			return SqlExpressionParser.equals(col1, col);
		}
		return null;
	}

	private static Function<CSVRecord, LongValue> sum(Column col1, Column col2) {
		return (CSVRecord csv) -> new LongValue(Long.parseLong(csv.get(col1.getColumnName()))
				+ Long.parseLong(csv.get(col2.getColumnName())));
	}

	private static Function<CSVRecord, LongValue> sum(LongValue col1, Column col2) {
		return (CSVRecord csv) -> new LongValue(col1.getValue() + Long.parseLong(csv.get(col2.getColumnName())));
	}

	private static Function<CSVRecord, LongValue> sum(Column col1, LongValue col2) {
		return (CSVRecord csv) -> new LongValue(col2.getValue() + Long.parseLong(csv.get(col1.getColumnName())));
	}

	private static Function<CSVRecord, LongValue> sum(LongValue col1, LongValue col2) {
		return (CSVRecord rec) -> new LongValue(col1.getValue() + col2.getValue());
	}

	private static Function<CSVRecord, LongValue> sum(Function<CSVRecord, LongValue> func1, LongValue col2) {
		return (CSVRecord rec) -> new LongValue(func1.apply(rec).getValue() + col2.getValue());
	}

	private static Function<CSVRecord, LongValue> sum(Function<CSVRecord, LongValue> func1,
			Function<CSVRecord, LongValue> func2) {
		return (CSVRecord rec) -> new LongValue(func1.apply(rec).getValue() + func2.apply(rec).getValue());
	}

	private static Function<CSVRecord, LongValue> sum(LongValue col1, Function<CSVRecord, LongValue> func2) {
		return (CSVRecord rec) -> new LongValue(col1.getValue() + func2.apply(rec).getValue());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	
	public static Function<CSVRecord, LongValue> sum(Object col1, Object col2) {
		if (col1 instanceof Column colproc1 && col2 instanceof Column colproc2) {
			return SqlExpressionParser.sum(colproc1, colproc2);
		} else if (col1 instanceof LongValue lv && col2 instanceof Column col) {
			return SqlExpressionParser.sum(lv, col);
		} else if (col1 instanceof Column col && col2 instanceof LongValue lv) {
			return SqlExpressionParser.sum(col, lv);
		} else if (col1 instanceof Function fn1 && col2 instanceof Function fn2) {
			return SqlExpressionParser.sum(fn1, fn2);
		} else if (col1 instanceof LongValue lv && col2 instanceof Function fn) {
			return SqlExpressionParser.sum(lv, fn);
		} else if (col1 instanceof Function fn && col2 instanceof LongValue lv) {
			return SqlExpressionParser.sum(fn, lv);
		} else {
			return SqlExpressionParser.sum((LongValue) col1, (LongValue) col2);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	
	public static Function<CSVRecord, LongValue> subtraction(Object col1, Object col2) {
		if (col1 instanceof Column colproc1 && col2 instanceof Column colproc2) {
			return SqlExpressionParser.subtraction(colproc1, colproc2);
		} else if (col1 instanceof LongValue lv && col2 instanceof Column col) {
			return SqlExpressionParser.subtraction(lv, col);
		} else if (col1 instanceof Column col && col2 instanceof LongValue lv) {
			return SqlExpressionParser.subtraction(col, lv);
		} else if (col1 instanceof Function fn1 && col2 instanceof Function fn2) {
			return SqlExpressionParser.subtraction(fn1, fn2);
		} else if (col1 instanceof LongValue lv && col2 instanceof Function fn) {
			return SqlExpressionParser.subtraction(lv, fn);
		} else if (col1 instanceof Function fn && col2 instanceof LongValue lv) {
			return SqlExpressionParser.subtraction(fn, lv);
		} else {
			return SqlExpressionParser.subtraction((LongValue) col1, (LongValue) col2);
		}
	}

	private static Function<CSVRecord, LongValue> subtraction(Column col1, Column col2) {
		return (CSVRecord csv) -> new LongValue(Long.parseLong(csv.get(col1.getColumnName()))
				- Long.parseLong(csv.get(col2.getColumnName())));
	}

	private static Function<CSVRecord, LongValue> subtraction(LongValue col1, Column col2) {
		return (CSVRecord csv) -> new LongValue(col1.getValue() - Long.parseLong(csv.get(col2.getColumnName())));
	}

	private static Function<CSVRecord, LongValue> subtraction(Column col1, LongValue col2) {
		return (CSVRecord csv) -> new LongValue(Long.parseLong(csv.get(col1.getColumnName())) - col2.getValue());
	}

	private static Function<CSVRecord, LongValue> subtraction(LongValue col1, LongValue col2) {
		return (CSVRecord rec) -> new LongValue(col1.getValue() - col2.getValue());
	}

	private static Function<CSVRecord, LongValue> subtraction(Function<CSVRecord, LongValue> func1, LongValue col2) {
		return (CSVRecord rec) -> new LongValue(func1.apply(rec).getValue() - col2.getValue());
	}

	private static Function<CSVRecord, LongValue> subtraction(Function<CSVRecord, LongValue> func1,
			Function<CSVRecord, LongValue> func2) {
		return (CSVRecord rec) -> new LongValue(func1.apply(rec).getValue() - func2.apply(rec).getValue());
	}

	private static Function<CSVRecord, LongValue> subtraction(LongValue col1, Function<CSVRecord, LongValue> func2) {
		return (CSVRecord rec) -> new LongValue(col1.getValue() - func2.apply(rec).getValue());
	}

	private static PredicateSerializable<CSVRecord> greaterThanEquals(Column col1, Column column2) {
		return (CSVRecord csv) -> Long.parseLong(csv.get(col1.getColumnName())) >= Long.parseLong(
				(csv.get(column2.getColumnName())));
	}

	public static PredicateSerializable<CSVRecord> greaterThanEquals(Function<CSVRecord, LongValue> func1,
			Column col2) {
		return (CSVRecord csv) -> func1.apply(csv).getValue() >= Long.parseLong((csv.get(col2.getColumnName())));
	}

	private static PredicateSerializable<CSVRecord> greaterThanEquals(Function<CSVRecord, LongValue> func1,
			Function<CSVRecord, LongValue> func2) {
		return (CSVRecord csv) -> func1.apply(csv).getValue() >= func2.apply(csv).getValue();
	}

	private static PredicateSerializable<CSVRecord> greaterThanEquals(Function<CSVRecord, LongValue> func1,
			LongValue value) {
		return (CSVRecord csv) -> func1.apply(csv).getValue() >= value.getValue();
	}

	private static PredicateSerializable<CSVRecord> greaterThanEquals(Column col1, LongValue col2) {
		return (CSVRecord csv) -> Long.parseLong(csv.get(col1.getColumnName())) >= col2.getValue();
	}

	private static PredicateSerializable<CSVRecord> notEquals(Object col1, Column col2) {
		return (CSVRecord csv) -> {
			Object constvalue = getValue(col1);
			return !constvalue.equals(getCsvValue(constvalue,col2,csv));
		};
	}

	private static PredicateSerializable<CSVRecord> notEquals(Column col1, Object col2) {
		return (CSVRecord csv) -> {
			Object constvalue = getValue(col2);
			return !getCsvValue(constvalue,col1,csv).equals(constvalue);
		};
	}

	
	public static PredicateSerializable<CSVRecord> notEquals(Object col1, Object col2) {
		if(col1 instanceof Column col) {
			return SqlExpressionParser.notEquals(col, col2);
		} else if(col2 instanceof Column col) {
			return SqlExpressionParser.notEquals(col1, col);
		}
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	
	public static PredicateSerializable<CSVRecord> greaterThanEquals(Object col1, Object col2) {
		if (col1 instanceof Column colproc1 && col2 instanceof Column colproc2) {
			return SqlExpressionParser.greaterThanEquals(colproc1, colproc2);
		} else if (col1 instanceof LongValue lv && col2 instanceof Column col) {
			return SqlExpressionParser.greaterThanEquals(lv, col);
		} else if (col1 instanceof Column col && col2 instanceof LongValue lv) {
			return SqlExpressionParser.greaterThanEquals(col, lv);
		} else if (col1 instanceof Function fn1 && col2 instanceof Function fn2) {
			return SqlExpressionParser.greaterThanEquals(fn1, fn2);
		} else if (col1 instanceof LongValue lv && col2 instanceof Function fn) {
			return SqlExpressionParser.greaterThanEquals(lv, fn);
		} else if (col1 instanceof Function fn && col2 instanceof LongValue lv) {
			return SqlExpressionParser.greaterThanEquals(fn, lv);
		} else {
			return SqlExpressionParser.greaterThanEquals((LongValue) col1, (LongValue) col2);
		}
	}

	@SuppressWarnings({})
	
	public static PredicateSerializable<CSVRecord> and(PredicateSerializable<CSVRecord> pred1,
			PredicateSerializable<CSVRecord> pred2) {
		return pred1.and(pred2);
	}

	@SuppressWarnings({})
	
	public static PredicateSerializable<CSVRecord> or(PredicateSerializable<CSVRecord> pred1,
			PredicateSerializable<CSVRecord> pred2) {
		return pred1.or(pred2);
	}

	
	public static MapToPairFunction<CSVRecord, Tuple> recordToTuple(String[] columns) {
		if (columns.length == 1) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]));
		} else if (columns.length == 2) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]));
		} else if (columns.length == 3) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]));
		} else if (columns.length == 4) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]));
		} else if (columns.length == 5) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]));
		} else if (columns.length == 6) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]));
		} else if (columns.length == 7) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]));
		} else if (columns.length == 8) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]));
		} else if (columns.length == 9) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]),
					csvrecord.get(columns[8]));
		} else if (columns.length == 10) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]),
					csvrecord.get(columns[8]), csvrecord.get(columns[9]));
		} else if (columns.length == 11) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]),
					csvrecord.get(columns[8]), csvrecord.get(columns[9]), csvrecord.get(columns[10]));
		} else if (columns.length == 12) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]),
					csvrecord.get(columns[8]), csvrecord.get(columns[9]), csvrecord.get(columns[10]),
					csvrecord.get(columns[11]));
		} else if (columns.length == 13) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]),
					csvrecord.get(columns[8]), csvrecord.get(columns[9]), csvrecord.get(columns[10]),
					csvrecord.get(columns[11]), csvrecord.get(columns[12]));
		} else if (columns.length == 14) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]),
					csvrecord.get(columns[8]), csvrecord.get(columns[9]), csvrecord.get(columns[10]),
					csvrecord.get(columns[11]), csvrecord.get(columns[12]), csvrecord.get(columns[13]));
		} else if (columns.length == 15) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]),
					csvrecord.get(columns[8]), csvrecord.get(columns[9]), csvrecord.get(columns[10]),
					csvrecord.get(columns[11]), csvrecord.get(columns[12]), csvrecord.get(columns[13]),
					csvrecord.get(columns[14]));
		} else if (columns.length == 16) {
			return (CSVRecord csvrecord) -> Tuple.tuple(csvrecord.get(columns[0]), csvrecord.get(columns[1]),
					csvrecord.get(columns[2]), csvrecord.get(columns[3]), csvrecord.get(columns[4]),
					csvrecord.get(columns[5]), csvrecord.get(columns[6]), csvrecord.get(columns[7]),
					csvrecord.get(columns[8]), csvrecord.get(columns[9]), csvrecord.get(columns[10]),
					csvrecord.get(columns[11]), csvrecord.get(columns[12]), csvrecord.get(columns[13]),
					csvrecord.get(columns[14]), csvrecord.get(columns[15]));
		}
		return null;
	}

}