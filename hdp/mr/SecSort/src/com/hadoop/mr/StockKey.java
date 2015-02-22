package com.hadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



public class StockKey implements WritableComparable<StockKey> {

	private String firstKey;
	private Long secondKey;
	
	public StockKey() {}
	
	public StockKey(String firstKey, Long secondKey) {
		this.firstKey = firstKey;
		this.secondKey = secondKey;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		firstKey = WritableUtils.readString(in);
		secondKey = in.readLong();
		System.out.println("StockKey:readFields-firstKey="+firstKey+" secondKey="+secondKey);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, firstKey);
		out.writeLong(secondKey);
		System.out.println("StockKey:write-firstKey="+firstKey+" secondKey="+secondKey);
	}

	@Override
	public int compareTo(StockKey o) {
		int result = firstKey.compareTo(o.firstKey);
		if (0 == result) {
			result = secondKey.compareTo(o.secondKey);
		}
		return result;
	}

	@Override
	public String toString() {
		return firstKey+","+secondKey;
		//return "{"+firstKey+","+secondKey+"}";
	}

	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

	public long getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(long secondKey) {
		this.secondKey = secondKey;
	}
	
}
