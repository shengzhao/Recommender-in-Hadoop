package com.ugc.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.math.VarIntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class MergeAndMutipleMapper extends
		Mapper<VarIntWritable, VectorAndVectorWritable, VarIntWritable, PrefAndSimilarityColumnWritable> {

	public static final Logger log = LoggerFactory.getLogger(MergeAndMutipleMapper.class);
	protected void map(VarIntWritable key, PrefAndSimilarityColumnWritable value, Context context) throws IOException, InterruptedException {
			log.info("map5");
			context.write(key, value);
		}
	

}
