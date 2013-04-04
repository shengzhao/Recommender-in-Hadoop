package com.ugc.mapreduce;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class UserItemNullMapper extends Mapper<LongWritable, Text, VectorWritable, VarIntWritable> {
	
	private static final Pattern DELIMITER = Pattern.compile("[\t,]");
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		VarIntWritable varw = new VarIntWritable();
		String[] tokens = UserItemNullMapper.DELIMITER.split(value.toString());
		int userId = Integer.parseInt(tokens[0]);
		int itemId = Integer.parseInt(tokens[1]);
		Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		vector.set(userId, itemId);
		VectorWritable vw = new VectorWritable(vector);
		vw.setWritesLaxPrecision(true);
		varw.set(-1);
		context.write(vw, varw);
	}
}
