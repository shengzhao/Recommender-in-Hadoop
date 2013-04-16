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

public class UserItemNullReducer extends Reducer<VectorWritable, VarIntWritable, VarIntWritable, VectorWritable> {
	public static final Logger log = LoggerFactory.getLogger(UserItemNullReducer.class);
	protected void reduce(VectorWritable key, Iterable<VarIntWritable> values, Context context) throws IOException, InterruptedException {
		
		VarIntWritable varw = new VarIntWritable();
		Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		Iterator<Vector.Element> iterator = key.get().iterateNonZero();
		Vector.Element elem = iterator.next();
		vector.set((int)elem.get(), -1.0);
		varw.set(elem.index());
		VectorWritable vw = new VectorWritable(vector);
		vw.setWritesLaxPrecision(true);
//		log.info(String.valueOf(varw.get())+" "+String.valueOf(elem.get()));
		context.write(varw, vw);
	}
}
