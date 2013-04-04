package com.ugc.mapreduce;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutipleMapper extends Mapper<VarIntWritable, VectorAndVectorWritable, VectorWritable, VarIntWritable> {
	public static final Logger log = LoggerFactory.getLogger(MutipleMapper.class);
	protected void map(VarIntWritable key, VectorAndVectorWritable value, Context context) throws IOException, InterruptedException {
		Vector user = value.getUserVector();
		Vector item = value.getItemVector();
		Iterator<Vector.Element> iterator = user.iterateNonZero();
		
		Iterator<Vector.Element> iterator1 = item.iterateNonZero();
		
		while(iterator.hasNext()){
			Vector.Element elem = iterator.next();
			while(iterator1.hasNext()){
				Vector.Element elem1 = iterator1.next();
				int userid = elem.index();
				int itemid = elem1.index();
				double count = elem.get();
				double count1 = elem1.get();
				double weight = count * count1;
				Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
				vector.set(userid, itemid);
				
				VarIntWritable dou = new VarIntWritable();
				dou.set((int)weight);
				VectorWritable vw = new VectorWritable(vector);
				log.info(String.valueOf(userid));
				log.info(String.valueOf(itemid));
				log.info(String.valueOf(weight));
				context.write(vw, dou);
			}
		}
	}
}
