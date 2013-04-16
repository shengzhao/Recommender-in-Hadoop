package com.ugc.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagItemCountReducer extends
		Reducer<VarIntWritable, VectorWritable, VarIntWritable, VectorOrVectorWritable> {
	
	public static final Logger log = LoggerFactory.getLogger(TagUserCountReducer.class);
	
	protected void reduce(VarIntWritable key, Iterable<VectorWritable> value, Context context) throws IOException, InterruptedException {
		int sum = 0;
		Vector resultvector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1000);
		List<VectorWritable> store = new ArrayList<VectorWritable>();
		for(VectorWritable vector : value) {
			VectorWritable v = new VectorWritable(vector.get());
			store.add(v);
			sum++;
//			Iterator<VectorWritable> it = store.iterator();
//			while(it.hasNext()) {
//				Iterator<Vector.Element> iterator = it.next().get().iterateNonZero();
//				Vector.Element elem = iterator.next();
//				log.info("itemlist:" + String.valueOf(elem.index()) + " " + String.valueOf(elem.get()));
//				
//			}
		}
		for(VectorWritable vector : store) {			
			Iterator<Vector.Element> iterator = vector.get().iterateNonZero();
			Vector.Element elem = iterator.next();
			int itemId = elem.index();
			double count =  elem.get();
			double result = count/Math.log(1+sum);
//			double result = count;
//			log.info(String.valueOf(itemId));
//			log.info(String.valueOf(result));
//			log.info(String.valueOf(itemId)+" "+String.valueOf(result));
			//double result = count;
			resultvector.set(itemId, result);

			//log.info("reduce 4");
			
		}
		VectorOrVectorWritable uw = new VectorOrVectorWritable();
		uw.setItem(resultvector);
		context.write(key, uw);
	}
}
