package com.ugc.mapreduce;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ugc.hadoop.common.RecommenderJob;

public final class ItemTagCountMapper extends Mapper<LongWritable, Text, VectorWritable, VarIntWritable>{
	private static final Pattern DELIMITER = Pattern.compile("[\t,]");
	private static final VarIntWritable one = new VarIntWritable(1);
	public static final Logger log = LoggerFactory.getLogger(ItemTagCountMapper.class);
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = ItemTagCountMapper.DELIMITER.split(value.toString());
		int itemId = Integer.parseInt(tokens[1]);
		int tagId = Integer.parseInt(tokens[2]);
		Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		vector.set(itemId, tagId);
		VectorWritable vw = new VectorWritable(vector);
		vw.setWritesLaxPrecision(true);
//		log.info("map 3");
		context.write(vw, one);
	}
}
