package com.ugc.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
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

public class TagUserCountReducer extends
		Reducer<VarIntWritable, VectorWritable, VarIntWritable, VectorOrVectorWritable> {
	private static final float userSig = 0;
	public static final Logger log = LoggerFactory.getLogger(TagUserCountReducer.class);
	
	protected void reduce(VarIntWritable key, Iterable<VectorWritable> value, Context context) throws IOException, InterruptedException {
		int sum = 0;
		Vector resultvector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1000);
		log.info("!!!!!!!!!!!!!!!");
		Iterator<VectorWritable> it = value.iterator();
		//Iterator<VectorWritable> anoit = value.iterator();
		List<VectorWritable> store = new LinkedList<VectorWritable>();
		while(it.hasNext()) {
			VectorWritable v = new VectorWritable(it.next().get());
			Iterator<Vector.Element> iterator = v.get().iterateNonZero();
			Vector.Element elem = iterator.next();
//			log.info("add:" + String.valueOf(elem.index()) + " " + String.valueOf(elem.get()));
			sum++;
			store.add(v);
//			Iterator<VectorWritable> listit = store.iterator();
//			while(listit.hasNext()) {
//				VectorWritable v1 = listit.next();
//				Iterator<Vector.Element> iterator1 = v1.get().iterateNonZero();
//				Vector.Element elem1 = iterator1.next();
//				log.info("list:" + String.valueOf(elem1.index()) + " " + String.valueOf(elem1.get()));
//			}
		}
		
		
		for(VectorWritable vector : store) {
			
			Iterator<Vector.Element> iterator = vector.get().iterateNonZero();
			Vector.Element elem = iterator.next();
			int userId = elem.index();
			double count =  elem.get();
			double result = count/Math.log(1+sum);
			//double result = count;
			//double result = count;
//			log.info(String.valueOf(userId));
//			log.info(String.valueOf(result));
//			log.info(String.valueOf(userId)+" "+String.valueOf(result));
			resultvector.set(userId, result);

			//log.info("reduce 2");
			
		}
		VectorOrVectorWritable uw = new VectorOrVectorWritable();
		
		uw.setUser(resultvector);
		context.write(key, uw);
	}
}
