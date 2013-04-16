package com.ugc.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserItemValueReducer extends Reducer<VectorWritable, DoubleWritable, VarIntWritable, VectorWritable> {
	
	public static final Logger log = LoggerFactory.getLogger(UserItemValueReducer.class);
	protected void reduce(VectorWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		Vector itemValue = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		VarIntWritable varw = new VarIntWritable();
		double sum = 0;
		for(DoubleWritable doubleWritable : values) {
			sum += doubleWritable.get();
		}
		Iterator<Vector.Element> iterator = key.get().iterateNonZero();
		Vector.Element elem = iterator.next();
		int userid = elem.index();
		int itemid = (int)elem.get();
		itemValue.set(itemid, sum);
		varw.set(userid);
		VectorWritable vw = new VectorWritable(itemValue);
		vw.setWritesLaxPrecision(true);
//		log.info(String.valueOf(userid)+" "+String.valueOf(itemid)+" "+String.valueOf(sum));
		context.write(varw, vw);
	}
}
