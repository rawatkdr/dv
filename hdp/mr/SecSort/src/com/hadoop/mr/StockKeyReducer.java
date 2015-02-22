package com.hadoop.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockKeyReducer extends Reducer<StockKey, DoubleWritable, Text, Text>{

	@Override
	protected void reduce(StockKey key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		Double sum = 0d;
		Text k = null;
		
		Iterator<DoubleWritable> it = values.iterator();
		while(it.hasNext()) {
			k = new Text(key.toString());
			Text v = new Text(it.next().toString());
			sum += Double.parseDouble(v.toString());
		}
		context.write(k, new Text(sum.toString()));
	}
}
