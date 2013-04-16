package com.ugc.hadoop.common;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.ugc.mapreduce.MergeAndMutipleMapper;
import com.ugc.mapreduce.MergeAndMutipleReducer;
import com.ugc.mapreduce.MergeReducer;
import com.ugc.mapreduce.MutipleMapper;
import com.ugc.mapreduce.PairWritable;
import com.ugc.mapreduce.TagItemCountReducer;
import com.ugc.mapreduce.TagUserCountReducer;
import com.ugc.mapreduce.ToRecommendationReducer;
import com.ugc.mapreduce.UserItemNullMapper;
import com.ugc.mapreduce.UserItemNullReducer;
import com.ugc.mapreduce.UserItemValueReducer;
import com.ugc.mapreduce.VectorAndVectorWritable;
import com.ugc.mapreduce.VectorOrVectorWritable;

import com.ugc.mapreduce.ItemTagCountMapper;
import com.ugc.mapreduce.ItemTagCountReducer;
import com.ugc.mapreduce.TagItemSumReducer;
import com.ugc.mapreduce.TagUserSumReducer;
import com.ugc.mapreduce.UserTagCountMapper;
import com.ugc.mapreduce.UserTagCountReducer;


public final class RecommenderJob {
	public static final Logger log = LoggerFactory.getLogger(RecommenderJob.class);


	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("/home/hadoop/workspace/Recommender/src/com/ugc/hadoop/common/log4j.properties");
		
		log.error("main function");
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Path inputPath = new Path(otherArgs[0]);
		Path outputPath = new Path(otherArgs[1]);
	    Path tempDirPath = new Path(otherArgs[2]);
	    
	    Path userTagCountPath = new Path(tempDirPath, "userTagCount");
	    Path tagUserSumPath = new Path(tempDirPath, "tagUserSum");
	    Path itemTagCountPath = new Path(tempDirPath, "itemTagCount");
	    Path tagItemSumPath = new Path(tempDirPath, "tagItemSum");
	    Path mergeAndMutiplePath = new Path(tempDirPath, "mergeAndMutiple");
	    Path userItemValuePath = new Path(tempDirPath, "userItemValue");
	    Path userItemNullPath = new Path(tempDirPath, "userItemNull"); 
	    Path mutiplePath = new Path(tempDirPath, "mutiple");
		
	    
	    Job job = new Job(conf, "job1");
	    //job.setJarByClass(RecommenderJob.class);
		job.setJarByClass(UserTagCountReducer.class);
	    job.setMapperClass(UserTagCountMapper.class);
		job.setMapOutputKeyClass(VectorWritable.class);
		job.setMapOutputValueClass(VarIntWritable.class);
		job.setReducerClass(UserTagCountReducer.class);
		job.setOutputKeyClass(VarIntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, userTagCountPath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		job.setOutputFormatClass(SequenceOutputFormat.class);
		job.waitForCompletion(true);
////////		
////////	
		Job tagUserSum = new Job(conf, "job2");
		tagUserSum.setJarByClass(TagUserCountReducer.class);
		tagUserSum.setInputFormatClass(SequenceFileInputFormat.class);
		tagUserSum.setMapperClass(Mapper.class);
		tagUserSum.setMapOutputKeyClass(VarIntWritable.class);
		tagUserSum.setMapOutputValueClass(VectorWritable.class);
		tagUserSum.setReducerClass(TagUserCountReducer.class);
		tagUserSum.setOutputKeyClass(VarIntWritable.class);
		tagUserSum.setOutputValueClass(VectorOrVectorWritable.class);
		tagUserSum.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(tagUserSum, userTagCountPath);
		FileOutputFormat.setOutputPath(tagUserSum, tagUserSumPath);
		tagUserSum.waitForCompletion(true);
////////////
//////////////		
		Job itemTagCount = new Job(conf, "job3");
		itemTagCount.setJarByClass(ItemTagCountReducer.class);
		itemTagCount.setInputFormatClass(TextInputFormat.class);
		itemTagCount.setMapperClass(ItemTagCountMapper.class);
		itemTagCount.setMapOutputKeyClass(VectorWritable.class);
		itemTagCount.setMapOutputValueClass(VarIntWritable.class);
		itemTagCount.setReducerClass(ItemTagCountReducer.class);
		itemTagCount.setOutputKeyClass(VarIntWritable.class);
		itemTagCount.setOutputValueClass(VectorWritable.class);
		itemTagCount.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(itemTagCount, inputPath);
		FileOutputFormat.setOutputPath(itemTagCount, itemTagCountPath);
		itemTagCount.waitForCompletion(true);
////////////		
//////////////
		Job tagItemSum = new Job(conf, "job4");
		tagItemSum.setJarByClass(TagItemCountReducer.class);
		tagItemSum.setInputFormatClass(SequenceFileInputFormat.class);
		tagItemSum.setMapperClass(Mapper.class);
		tagItemSum.setMapOutputKeyClass(VarIntWritable.class);
		tagItemSum.setMapOutputValueClass(VectorWritable.class);
		tagItemSum.setReducerClass(TagItemCountReducer.class);
		tagItemSum.setOutputKeyClass(VarIntWritable.class);
		tagItemSum.setOutputValueClass(VectorOrVectorWritable.class);
		tagItemSum.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(tagItemSum, itemTagCountPath);
		FileOutputFormat.setOutputPath(tagItemSum, tagItemSumPath);
		tagItemSum.waitForCompletion(true);
////
//////////		
//////////		
		log.error("Merge");
		Job mergeAndMutiple = new Job(conf, "job5");
		mergeAndMutiple.setJarByClass(MergeReducer.class);
		mergeAndMutiple.setInputFormatClass(SequenceFileInputFormat.class);
		mergeAndMutiple.setMapperClass(Mapper.class);
		mergeAndMutiple.setMapOutputKeyClass(VarIntWritable.class);
		mergeAndMutiple.setMapOutputValueClass(VectorOrVectorWritable.class);
		mergeAndMutiple.setReducerClass(MergeReducer.class);
		mergeAndMutiple.setOutputKeyClass(VarIntWritable.class);
		mergeAndMutiple.setOutputValueClass(VectorAndVectorWritable.class);
		mergeAndMutiple.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(mergeAndMutiple, new Path(tagUserSumPath + "," + tagItemSumPath));
		FileOutputFormat.setOutputPath(mergeAndMutiple, mergeAndMutiplePath);
		Configuration partialMultiplyConf = mergeAndMutiple.getConfiguration();
	      FileSystem fs = FileSystem.get(tempDirPath.toUri(), partialMultiplyConf);
	      tagItemSumPath = tagItemSumPath.makeQualified(fs);
	      tagUserSumPath = tagUserSumPath.makeQualified(fs);
	      FileInputFormat.setInputPaths(mergeAndMutiple, tagUserSumPath, tagItemSumPath);
		mergeAndMutiple.waitForCompletion(true);
//////		//System.exit(mergeAndMutiple.waitForCompletion(true) ? 0 : 1);
////////		
		log.error("Mutiple");
		Job Mutiple = new Job(conf, "job6");
		Mutiple.setJarByClass(MutipleMapper.class);
		Mutiple.setInputFormatClass(SequenceFileInputFormat.class);
		Mutiple.setMapperClass(MutipleMapper.class);
		Mutiple.setMapOutputKeyClass(VectorWritable.class);
		Mutiple.setMapOutputValueClass(DoubleWritable.class);
		Mutiple.setReducerClass(Reducer.class);
		Mutiple.setOutputKeyClass(VectorWritable.class);
		Mutiple.setOutputValueClass(DoubleWritable.class);
		Mutiple.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(Mutiple, mergeAndMutiplePath);
		FileOutputFormat.setOutputPath(Mutiple, mutiplePath);
	
		Mutiple.waitForCompletion(true);
//////////
		Job userItemValue = new Job(conf, "job7");
		userItemValue.setJarByClass(UserItemValueReducer.class);
		userItemValue.setInputFormatClass(SequenceFileInputFormat.class);
		userItemValue.setMapperClass(Mapper.class);
		userItemValue.setMapOutputKeyClass(VectorWritable.class);
		userItemValue.setMapOutputValueClass(DoubleWritable.class);
		userItemValue.setReducerClass(UserItemValueReducer.class);
		userItemValue.setOutputKeyClass(VarIntWritable.class);
		userItemValue.setOutputValueClass(VectorWritable.class);
		userItemValue.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(userItemValue, mutiplePath);
		FileOutputFormat.setOutputPath(userItemValue, userItemValuePath);
		//FileOutputFormat.setOutputPath(userItemValue, outputPath);
		userItemValue.waitForCompletion(true);
//////////		
		Job userItemNull = new Job(conf, "job8");
		userItemNull.setJarByClass(UserItemNullReducer.class);
		userItemNull.setInputFormatClass(TextInputFormat.class);
		userItemNull.setMapperClass(UserItemNullMapper.class);
		userItemNull.setMapOutputKeyClass(VectorWritable.class);
		userItemNull.setMapOutputValueClass(VarIntWritable.class);
		userItemNull.setReducerClass(UserItemNullReducer.class);
		userItemNull.setOutputKeyClass(VarIntWritable.class);
		userItemNull.setOutputValueClass(VectorWritable.class);
		userItemNull.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(userItemNull, inputPath);
		FileOutputFormat.setOutputPath(userItemNull, userItemNullPath);
		//FileOutputFormat.setOutputPath(userItemNull, outputPath);
		userItemNull.waitForCompletion(true);
//////		
		log.info("toRecommendation");
		Job toRecommendation = new Job(conf, "job9");
		toRecommendation.setJarByClass(ToRecommendationReducer.class);
		toRecommendation.setInputFormatClass(SequenceFileInputFormat.class);
		toRecommendation.setMapperClass(Mapper.class);
		toRecommendation.setMapOutputKeyClass(VarIntWritable.class);
		toRecommendation.setMapOutputValueClass(VectorWritable.class);
		toRecommendation.setReducerClass(ToRecommendationReducer.class);
		toRecommendation.setOutputKeyClass(VarIntWritable.class);
		toRecommendation.setOutputValueClass(RecommendedItemsWritable.class);
		toRecommendation.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(toRecommendation, new Path(userItemValuePath + "," + userItemNullPath));
		FileOutputFormat.setOutputPath(toRecommendation, outputPath);
		Configuration toRecommendationConf = toRecommendation.getConfiguration();
	      FileSystem fs2 = FileSystem.get(tempDirPath.toUri(), toRecommendationConf);
	      userItemValuePath = userItemValuePath.makeQualified(fs2);
	      userItemNullPath = userItemNullPath.makeQualified(fs2);
	      FileInputFormat.setInputPaths(toRecommendation, userItemValuePath, userItemNullPath);
		
		toRecommendation.waitForCompletion(true);	   
//
//
//
//
////	    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
////	    	Job toRecommendation = prepareJob(
////	    			new Path(userItemValuePath + "," + userItemNullPath),
////	    			outputPath,
////	    			SequenceFileInputFormat.class,
////	    			Mapper.class,
////	    			VarIntWritable.class,
////	    			VectorWritable.class,
////	    			ToRecommendationReducer.class,
////	    			VarIntWritable.class,
////	    			RecommendedItemsWritable.class,
////	    			TextOutputFormat.class
////	    			);
////	    	Configuration toRecommendationConf = toRecommendation.getConfiguration();
////	        FileSystem fs = FileSystem.get(tempDirPath.toUri(), toRecommendationConf);
////	        userItemValuePath = userItemValuePath.makeQualified(fs);
////	        userItemNullPath = userItemNullPath.makeQualified(fs);
////	        FileInputFormat.setInputPaths(toRecommendation, userItemValuePath, userItemNullPath);
////	    	toRecommendation.waitForCompletion(true);
	}

	
}
