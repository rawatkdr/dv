package com.hadoop.mr;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class StockKeyPartitioner extends Partitioner<StockKey, DoubleWritable> {

	@Override
	public int getPartition(StockKey key, DoubleWritable val, int numPartitions) {
		
		// numPartitions: system assigned number for available reduce partitions has to be within this limit. 
		if (numPartitions == 0) return 0;
		
		String k = key.getFirstKey();
		int hash = k.hashCode();
		int partition = hash % numPartitions;

		String s = k.substring(k.length()-1);
		System.out.println("StockKeyPartitioner.getPartition: "+k+" hash:"+hash+ " s:"+ s +" numPartitions:" + numPartitions+ " partition: "+partition);

		return (Integer.parseInt(s) % numPartitions);
		//return partition;
	}

}
