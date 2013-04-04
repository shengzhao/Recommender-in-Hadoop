package com.ugc.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeReducer extends Reducer<VarIntWritable, VectorOrVectorWritable, VarIntWritable, VectorAndVectorWritable> {
	
	
	public static final Logger log = LoggerFactory.getLogger(MergeReducer.class);
	protected void reduce(VarIntWritable key, Iterable<VectorOrVectorWritable> values, Context context) throws IOException, InterruptedException {
		Vector user = new RandomAccessSparseVector(Integer.MAX_VALUE, 1000);
		Vector item = new RandomAccessSparseVector(Integer.MAX_VALUE, 1000);
		for(VectorOrVectorWritable vv : values) {
			
			int sig = vv.getSig();
			if(sig == 0)
				user.assign(vv.getUserVector());
			else if(sig == 1)
				item.assign(vv.getItemVector());
			
		}
//		if(item.size() == 0)
//			item.set(0, 1);
		Vector usertmp = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		usertmp.set(0, 1);
		Iterator<Vector.Element> iterator2 = user.iterateNonZero();
		if(!iterator2.hasNext())
		{
			log.info("test");
			user.assign(usertmp);
		}
		Iterator<Vector.Element> iterator = user.iterateNonZero();
		while(iterator.hasNext()){
			Vector.Element elem = iterator.next();
			log.info(String.valueOf(elem.index()));
			log.info(String.valueOf(elem.get()));
			log.info("user");
		}
		Iterator<Vector.Element> iterator1 = item.iterateNonZero();
		while(iterator1.hasNext()){
			Vector.Element elem1 = iterator1.next();
			log.info(String.valueOf(elem1.index()));
			log.info(String.valueOf(elem1.get()));
			log.info("item");
		}
		VectorAndVectorWritable vav = new VectorAndVectorWritable(user, item);
//		Iterator<Vector.Element> iterator = user.iterateNonZero();
//		while(iterator.hasNext()){
//			Vector.Element elem = iterator.next();
//			log.info(String.valueOf(elem.index()));
//			log.info(String.valueOf(elem.get()));
//			log.info("user");
//		}
//		Iterator<Vector.Element> iterator1 = item.iterateNonZero();
//		while(iterator1.hasNext()){
//			Vector.Element elem1 = iterator1.next();
//			log.info(String.valueOf(elem1.index()));
//			log.info(String.valueOf(elem1.get()));
//			log.info("item");
//		}
		context.write(key, vav);
	}
}
