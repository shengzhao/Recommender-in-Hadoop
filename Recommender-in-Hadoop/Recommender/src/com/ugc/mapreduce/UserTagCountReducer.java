package com.ugc.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ugc.hadoop.common.RecommenderJob;

public final class UserTagCountReducer extends Reducer<VectorWritable, VarIntWritable, VarIntWritable, VectorWritable> {
	
	private static final VarIntWritable tag = new VarIntWritable();
	public static final Logger log = LoggerFactory.getLogger(UserTagCountReducer.class);
	protected void reduce(VectorWritable key, Iterable<VarIntWritable> value, Context context) throws IOException, InterruptedException {
		Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		int sum = 0;
		for(VarIntWritable varIntWritable : value) {
			sum += varIntWritable.get();
		}
		Vector userTag = key.get();
		Iterator<Vector.Element> iterator = userTag.iterateNonZero();
		Vector.Element elem = iterator.next();
		int userId = elem.index();
		log.info(String.valueOf(userId));
		int tagId = (int) elem.get();
		log.info(String.valueOf(tagId));
		vector.set(userId, sum);
		tag.set(tagId);
		log.info(String.valueOf(sum));
		VectorWritable vw = new VectorWritable(vector);
		vw.setWritesLaxPrecision(true);
		//log.info("reduce1");
	    context.write(tag, vw);
	}
}

//public final class UserTagCountReducer extends Reducer<PairWritable, VarIntWritable, VarIntWritable, PairWritable> {
//	
//	private static final VarIntWritable tag = new VarIntWritable();
//	public static final Logger log = LoggerFactory.getLogger(UserTagCountReducer.class);
//	protected void reduce(PairWritable key, Iterable<VarIntWritable> value, Context context) throws IOException, InterruptedException {
//		PairWritable pw = new PairWritable();
//		int sum = 0;
//		for(VarIntWritable varIntWritable : value) {
//			sum += varIntWritable.get();
//		}
//		
//		int userId = (int)key.getkey();
//		log.info(String.valueOf(userId));
//		int tagId = (int)key.getvalue();
//		log.info(String.valueOf(tagId));
//		pw.set(userId, sum);
//		tag.set(tagId);
//		log.info(String.valueOf(sum));
//		
//		
//		//log.info("reduce1");
//	    context.write(tag, pw);
//	}
