package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockKeyMapper extends Mapper<LongWritable, Text, StockKey, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Split the record into 2 keys and val,
		String[] tokens = value.toString().split(",");
		String firstKey = tokens[0].trim();
		Long secondKey = Long.parseLong(tokens[1]);
		Double v = Double.parseDouble(tokens[2]);
		
		// make it writable into context: convert 2 cols into stock class and value 
		StockKey stockKey = new StockKey(firstKey, secondKey);
		DoubleWritable stockValue = new DoubleWritable(v);
		
		System.out.println("Mapper: firstKey="+firstKey+" secondKey="+secondKey+" stockValue="+stockValue);
		context.write(stockKey, stockValue);
	}

	
}
