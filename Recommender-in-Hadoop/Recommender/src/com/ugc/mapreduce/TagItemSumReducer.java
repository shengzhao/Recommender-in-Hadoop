package com.ugc.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ugc.hadoop.common.RecommenderJob;

public final class TagItemSumReducer extends Reducer<VarIntWritable, VectorWritable, VarIntWritable, PrefAndSimilarityColumnWritable> {
	
	public static final float itemSig = 1;
	public static final Logger log = LoggerFactory.getLogger(TagItemSumReducer.class);
	
	protected void reduce(VarIntWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		
		/*for(VectorWritable vectorWritable : values) {
			sum += 1;
		}*/
		//Log.info("test");
		for(VectorWritable vectorWritable : values) {
			Vector vw = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
			PrefAndSimilarityColumnWritable uw = new PrefAndSimilarityColumnWritable();
			Iterator<Vector.Element> iterator = vectorWritable.get().iterateNonZero();
			Vector.Element elem = iterator.next();
			int itemId = elem.index();
			double count = elem.get();
			
			double result = count;
			//double result = count/Math.log(1+sum);
			vw.set(itemId, result);
			uw.set(itemSig, vw);
			log.info("reduce 4");
			context.write(key, uw);
		}
	}
}
