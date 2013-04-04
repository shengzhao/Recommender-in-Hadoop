package com.ugc.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TagUserSumReducer extends Reducer<VarIntWritable, VectorWritable, VarIntWritable, PrefAndSimilarityColumnWritable> {
	
	private static final float userSig = 0;
	public static final Logger log = LoggerFactory.getLogger(TagUserSumReducer.class);
	
	protected void reduce(VarIntWritable key, Iterable<VectorWritable> value, Context context) throws IOException, InterruptedException {
		int sum = 0;
		Iterable<VectorWritable> tmp = value;
		
		/*for(VectorWritable vector : value) {
			sum += 1;
		}*/
		//PropertyConfigurator.configure("log4j.properties");
		//log.info(String.valueOf(sum));
		//System.out.println("abcdefgh");
		for(VectorWritable vector : tmp) {
			Vector vw = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
			PrefAndSimilarityColumnWritable uw = new PrefAndSimilarityColumnWritable();
			Iterator<Vector.Element> iterator = vector.get().iterateNonZero();
			Vector.Element elem = iterator.next();
			int userId = elem.index();
			double count =  elem.get();
			//double result = count/Math.log(1+sum);
			double result = count;
			//double result = count;
			vw.set(userId, result);
			uw.set(userSig, vw);
			log.info("reduce 2");
			context.write(key, uw);
		}
	}
}
/*
 * 当所有的key value循环一次，算出一个sum值，还能继续循环一次吗？
 */