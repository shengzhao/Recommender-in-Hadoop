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

public final class ItemTagCountReducer extends Reducer<VectorWritable, VarIntWritable, VarIntWritable, VectorWritable> {
	
	private static final VarIntWritable tag = new VarIntWritable();
	public static final Logger log = LoggerFactory.getLogger(ItemTagCountReducer.class);
	
	protected void reduce(VectorWritable key, Iterable<VarIntWritable> value, Context context) throws IOException, InterruptedException {
		Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		int sum = 0;
		for(VarIntWritable varIntWritable : value) {
			sum += varIntWritable.get();
		}
		Vector itemTag = key.get();
		Iterator<Vector.Element> iterator = itemTag.iterateNonZero();
		Vector.Element elem = iterator.next();
		int itemId = elem.index();
		int tagId = (int) elem.get();
//		log.info(String.valueOf(itemId));
//		log.info(String.valueOf(tagId));
//		log.info(String.valueOf(sum));
		vector.set(itemId, sum);
		tag.set(tagId);
		VectorWritable vw = new VectorWritable(vector);
		vw.setWritesLaxPrecision(true);
		//log.info("reduce 3");
	    context.write(tag, vw);
	}
}
