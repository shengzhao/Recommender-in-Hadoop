package com.ugc.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MergeAndMutipleReducer extends Reducer<VarIntWritable, PrefAndSimilarityColumnWritable, VectorWritable, DoubleWritable> {
	public static final Logger log = LoggerFactory.getLogger(MergeAndMutipleReducer.class);
	protected void Reduce(VarIntWritable key, Iterable<PrefAndSimilarityColumnWritable> values, Context context) throws IOException, InterruptedException {
		List<Vector> listItem = new ArrayList<Vector>();
		List<Vector> listUser = new ArrayList<Vector>();
		float sig = -1;
		for (PrefAndSimilarityColumnWritable prefAndSimilarityColumn : values) {
			 Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
			vector = prefAndSimilarityColumn.getSimilarityColumn();
			sig = prefAndSimilarityColumn.getPrefValue();
			if(sig==1)
				listItem.add(vector);
			else if(sig==0)
				listUser.add(vector);
		}
		log.error("test");
		
//		while(values.hasNext()) {
//			UserSigWritable userSigWritable = new UserSigWritable(values.next());
//			vector = userSigWritable.getVector();
//			sig = userSigWritable.getUserSig();
//			if(sig.equals("I"))
//				listItem.add(vector);
//			else
//				listUser.add(vector);
//		}
		log.error(listItem.toString());
		log.error(listUser.toString());
		int userId = -1;
		int itemId = -1;
		double userCount = -1;
		double itemCount = -1;
		double weight = -1;
		
		// <userid, count> <itemid, count> => <userid, itemid>, value
		for(int i = 0; i < listUser.size(); i ++) {
			for(int j = 0; j < listItem.size(); j++) {
				Vector vector1 = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
				DoubleWritable dw = new DoubleWritable();
				VectorWritable vw = new VectorWritable();
				Iterator<Vector.Element> iterator = listUser.get(i).iterateNonZero();
				Vector.Element elem = iterator.next();
				userId = elem.index();
				userCount = elem.get();
				Iterator<Vector.Element> anoiterator = listItem.get(j).iterateNonZero();
				Vector.Element anoelem = anoiterator.next();
				itemId = anoelem.index();
				itemCount = anoelem.get();
				weight = userCount * itemCount;
				vector1.set(userId, itemId);
				vw.set(vector1);
				vw.setWritesLaxPrecision(true);
				dw.set(weight);
				log.info("reduce 5");
				context.write(vw, dw);
			}
		}
	}
}
