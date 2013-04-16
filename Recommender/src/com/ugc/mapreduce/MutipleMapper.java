package com.ugc.mapreduce;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutipleMapper extends Mapper<VarIntWritable, VectorAndVectorWritable, VectorWritable, DoubleWritable> {
	public static final Logger log = LoggerFactory.getLogger(MutipleMapper.class);
	protected void map(VarIntWritable key, VectorAndVectorWritable value, Context context) throws IOException, InterruptedException {
		Vector user = value.getUserVector();
		Vector item = value.getItemVector();
//		Iterator<Vector.Element> it = user.iterateNonZero();
//		while(it.hasNext()) {
//			Vector.Element elem = it.next();
//			log.info("user:"+String.valueOf(elem.index())+" "+String.valueOf(elem.get()));
//		}
//		Iterator<Vector.Element> it1 = item.iterateNonZero();
//		while(it1.hasNext()) {
//			Vector.Element elem = it1.next();
//			log.info("item:"+String.valueOf(elem.index())+" "+String.valueOf(elem.get()));
//		}
		Iterator<Vector.Element> iterator = user.iterateNonZero();
		
		
		
		while(iterator.hasNext()){
			Vector.Element elem = iterator.next();
			//这里容易出现bug，会把iterator1写在外层循环了，每次都应该初始化
			Iterator<Vector.Element> iterator1 = item.iterateNonZero();
			while(iterator1.hasNext()){
				Vector.Element elem1 = iterator1.next();
				int userid = elem.index();
				int itemid = elem1.index();
				double count = elem.get();
				double count1 = elem1.get();
				double weight = count * count1;
				Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
				vector.set(userid, itemid);
				
				DoubleWritable dou = new DoubleWritable();
				dou.set(weight);
				VectorWritable vw = new VectorWritable(vector);
//				log.info(String.valueOf(userid)+" "+String.valueOf(itemid)+" "+String.valueOf(weight));
				context.write(vw, dou);
			}
		}
	}
}
