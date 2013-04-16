package com.ugc.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToRecommendationReducer extends Reducer<VarIntWritable, VectorWritable, VarIntWritable, RecommendedItemsWritable> {
	public static final Logger log = LoggerFactory.getLogger(ToRecommendationReducer.class);
	private static final int size = 10;
	protected void reduce(VarIntWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
//		log.info("test");
		Map<Integer, Double> map = new HashMap<Integer, Double> ();
		//Vector vector = new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
		for(VectorWritable vectorWritable : values) {
			Iterator<Vector.Element> recommendationVectorIterator = vectorWritable.get().iterateNonZero();
			Vector.Element element = recommendationVectorIterator.next();
			
			int itemId = element.index();
			double value = element.get();
//			log.info("vector:"+String.valueOf(itemId)+" "+String.valueOf(value));
			if(map.containsKey(itemId))
				map.remove(itemId);
			else
				map.put(itemId, value);
		}
//		for(Map.Entry<Integer, Double>entry : map.entrySet())
//		{
//			int itemId = entry.getKey();
//			double value = entry.getValue();
//			log.info("map:"+String.valueOf(itemId)+" "+String.valueOf(value));
//		}
		Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(size,
                Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()));
		if(map.size() > 0)
			for(Map.Entry<Integer, Double>entry : map.entrySet()) {
				int itemId = entry.getKey();
				double value = entry.getValue();
				if(value > 0)
				{
					if(topItems.size() < size)
						topItems.add(new GenericRecommendedItem(itemId, (float)value));
					else if(value > topItems.peek().getValue()) {
						topItems.add(new GenericRecommendedItem(itemId, (float)value));
						topItems.poll();
					}
				}
			}
		if (!topItems.isEmpty()) {
		      List<RecommendedItem> recommendations = new ArrayList<RecommendedItem>(topItems.size());
		      recommendations.addAll(topItems);
		      Collections.sort(recommendations, ByValueRecommendedItemComparator.getInstance());
//		      for(int i = 0; i< recommendations.size(); i++)
//		      {
//		    	  log.info(String.valueOf(recommendations.get(i).getItemID()));
//		    	  log.info(String.valueOf(recommendations.get(i).getValue()));
//		      }
		     //log.info(recommendations.toString());
//		      log.info("test");
		      context.write(key, new RecommendedItemsWritable(recommendations));
		}
	}
}
