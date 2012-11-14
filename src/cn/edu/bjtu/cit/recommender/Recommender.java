/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.bjtu.cit.recommender;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.plan.ClusterOracle;
import org.apache.crunch.profile.Profiler;
import org.apache.crunch.types.writable.RecommendedItems;
import org.apache.crunch.types.writable.VectorAndPrefs;
import org.apache.crunch.types.writable.VectorOrPref;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import cn.edu.bjtu.cit.recommender.profile.ProfileConverter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Floats;

@SuppressWarnings("serial")
public class Recommender extends Configured implements Tool, Serializable {
	
	public static final String PROFILING = "profiling";
	public static final String PROFILING_SHORT = "p";
	public static final String CLUSTER_SIZE = "clustersize";
	public static final String CLUSTER_SIZE_SHORT = "cs";
	public static final String ESTIMATION = "estimation";
	public static final String ESTIMATION_SHORT = "est";
	public static final int ACTIVE_THRESHOLD = 20;
	public static final int TOP = 10;
	
	private final Log log = LogFactory.getLog(Recommender.class);
	private static final Comparator<RecommendedItem> BY_PREFERENCE_VALUE = new Comparator<RecommendedItem>() {
		@Override
		public int compare(RecommendedItem one, RecommendedItem two) {
			return Floats.compare(one.getValue(), two.getValue());
		}
	};
	
	private Profiler profiler;
	private Estimator est;
	private Map<String, String> options;
	private String profileFilePath;
	private int clusterSize = 1;
	private String estFile;

	public Recommender() {
		est = new Estimator();
		options = Maps.newHashMap();
	}
	
	public boolean on(){
		if(options.containsKey(PROFILING)){
			profileFilePath = options.get(PROFILING);
		}
		else if(options.containsKey(PROFILING_SHORT)){
			profileFilePath = options.get(PROFILING_SHORT);
		}
		return profileFilePath != null;
	}
	
	public boolean hasSetClusterSize(){
		if(options.containsKey(CLUSTER_SIZE)){
			clusterSize = Integer.parseInt(options.get(CLUSTER_SIZE));
		}
		else if(options.containsKey(CLUSTER_SIZE_SHORT)){
			clusterSize = Integer.parseInt(options.get(CLUSTER_SIZE_SHORT));
		}
		return clusterSize != 1;
	}
	
	public boolean hasEstimationFile(){
		if(options.containsKey(ESTIMATION)){
			estFile = options.get(ESTIMATION);
		}
		else if(options.containsKey(ESTIMATION_SHORT)){
			estFile = options.get(ESTIMATION_SHORT);
		}
		return estFile != null;
	}
	
	public void getOptions(String[] args){
		for(String arg : args){
			if(arg.contains("=")){
				String[] tokens = arg.split("=");
				options.put(tokens[0].toLowerCase(), tokens[1]);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println();
			System.err.println("Usage: " + this.getClass().getName() + " [generic options] input output [profiling] [estimation] [clustersize]");
			System.err.println();
			printUsage();
			GenericOptionsParser.printGenericCommandUsage(System.err);
			
			return 1;
		}
		getOptions(args);
		
		Pipeline pipeline = new MRPipeline(Recommender.class, getConf());
		if(hasSetClusterSize()){
			pipeline.getConfiguration().setInt(ClusterOracle.CLUSTER_SIZE, clusterSize);
		}
		if(on()){
			pipeline.getConfiguration().setBoolean(Profiler.IS_PROFILE, true);
		}
		if (hasEstimationFile()) {
			est = new Estimator(estFile, clusterSize);
		}
		
		profiler = new Profiler(pipeline);
		/*
		 * input node
		 */
		PCollection<String> lines = pipeline.readTextFile(args[0]);
		
		if(profiler.isProfiling() && lines.getSize() > 10 * 1024 * 1024){
			lines = lines.sample(0.1);
		}

		/*
		 * S0 + GBK
		 */
		PGroupedTable<Long, Long> userWithPrefs = lines.parallelDo(new MapFn<String, Pair<Long, Long>>() {

			@Override
			public Pair<Long, Long> map(String input) {
				String[] split = input.split(Estimator.DELM);
				long userID = Long.parseLong(split[0]);
				long itemID = Long.parseLong(split[1]);
				return Pair.of(userID, itemID);
			}

			@Override
			public float scaleFactor() {
				return est.getScaleFactor("S0").sizeFactor;
			}

			@Override
			public float scaleFactorByRecord() {
				return est.getScaleFactor("S0").recsFactor;
			}
		}, Writables.tableOf(Writables.longs(), Writables.longs())).groupByKey(est.getClusterSize());

		
		/*
		 * S1
		 */
		PTable<Long, Vector> userVector = userWithPrefs.parallelDo(
				new MapFn<Pair<Long, Iterable<Long>>, Pair<Long, Vector>>() {
					@Override
					public Pair<Long, Vector> map(Pair<Long, Iterable<Long>> input) {
						Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
						for (long itemPref : input.second()) {
							userVector.set((int) itemPref, 1.0f);
						}
						return Pair.of(input.first(), userVector);
					}

					@Override
					public float scaleFactor() {
						return est.getScaleFactor("S1").sizeFactor;
					}

					@Override
					public float scaleFactorByRecord() {
						return est.getScaleFactor("S1").recsFactor;
					}
				}, Writables.tableOf(Writables.longs(), Writables.vectors()));
		
		userVector = profiler.profile("S0-S1", pipeline, userVector, ProfileConverter.long_vector(),
				Writables.tableOf(Writables.longs(), Writables.vectors()));	
		
		/*
		 * S2
		 */
		PTable<Long, Vector> filteredUserVector = userVector.parallelDo(new DoFn<Pair<Long, Vector>, Pair<Long, Vector>>(){

			@Override
			public void process(Pair<Long, Vector> input, Emitter<Pair<Long, Vector>> emitter) {
				if(input.second().getNumNondefaultElements() > ACTIVE_THRESHOLD){
					emitter.emit(input);
				}
			}
			
			@Override
			public float scaleFactor() {
				return est.getScaleFactor("S2").sizeFactor;
			}

			@Override
			public float scaleFactorByRecord() {
				return est.getScaleFactor("S2").recsFactor;
			}
			
		}, Writables.tableOf(Writables.longs(), Writables.vectors()));
		
		filteredUserVector = profiler.profile("S2", pipeline, filteredUserVector, ProfileConverter.long_vector(),
				Writables.tableOf(Writables.longs(), Writables.vectors()));	
		
		/*
		 * S3 + GBK
		 */
		PGroupedTable<Integer, Integer> coOccurencePairs = filteredUserVector.parallelDo(
				new DoFn<Pair<Long, Vector>, Pair<Integer, Integer>>() {
					@Override
					public void process(Pair<Long, Vector> input, Emitter<Pair<Integer, Integer>> emitter) {
						Iterator<Vector.Element> it = input.second().iterateNonZero();
						while (it.hasNext()) {
							int index1 = it.next().index();
							Iterator<Vector.Element> it2 = input.second().iterateNonZero();
							while (it2.hasNext()) {
								int index2 = it2.next().index();
								emitter.emit(Pair.of(index1, index2));
							}
						}
					}

					@Override
					public float scaleFactor() {
						float size = est.getScaleFactor("S3").sizeFactor;
						return size;
					}

					@Override
					public float scaleFactorByRecord() {
						float recs = est.getScaleFactor("S3").recsFactor;
						return recs;
					}
				}, Writables.tableOf(Writables.ints(), Writables.ints())).groupByKey(est.getClusterSize());

		/*
		 * S4
		 */
		PTable<Integer, Vector> coOccurenceVector = coOccurencePairs.parallelDo(
				new MapFn<Pair<Integer, Iterable<Integer>>, Pair<Integer, Vector>>() {
					@Override
					public Pair<Integer, Vector> map(Pair<Integer, Iterable<Integer>> input) {
						Vector cooccurrenceRow = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
						for (int itemIndex2 : input.second()) {
							cooccurrenceRow.set(itemIndex2, cooccurrenceRow.get(itemIndex2) + 1.0);
						}
						return Pair.of(input.first(), cooccurrenceRow);
					}

					@Override
					public float scaleFactor() {
						return est.getScaleFactor("S4").sizeFactor;
					}

					@Override
					public float scaleFactorByRecord() {
						return est.getScaleFactor("S4").recsFactor;
					}
				}, Writables.tableOf(Writables.ints(), Writables.vectors()));

		coOccurenceVector = profiler.profile("S3-S4", pipeline, coOccurenceVector, ProfileConverter.int_vector(),
				Writables.tableOf(Writables.ints(), Writables.vectors()));

		/*
		 * S5 Wrapping co-occurrence columns
		 */
		PTable<Integer, VectorOrPref> wrappedCooccurrence = coOccurenceVector.parallelDo(
				new MapFn<Pair<Integer, Vector>, Pair<Integer, VectorOrPref>>() {

					@Override
					public Pair<Integer, VectorOrPref> map(Pair<Integer, Vector> input) {
						return Pair.of(input.first(), new VectorOrPref(input.second()));
					}
					
					@Override
					public float scaleFactor() {
						return est.getScaleFactor("S5").sizeFactor;
					}

					@Override
					public float scaleFactorByRecord() {
						return est.getScaleFactor("S5").recsFactor;
					}

				}, Writables.tableOf(Writables.ints(), VectorOrPref.vectorOrPrefs()));
		
		wrappedCooccurrence = profiler.profile("S5", pipeline, wrappedCooccurrence, ProfileConverter.int_vopv(),
				Writables.tableOf(Writables.ints(), VectorOrPref.vectorOrPrefs()));

		/*
		 * S6 Splitting user vectors
		 */
		PTable<Integer, VectorOrPref> userVectorSplit = filteredUserVector.parallelDo(
				new DoFn<Pair<Long, Vector>, Pair<Integer, VectorOrPref>>() {

					@Override
					public void process(Pair<Long, Vector> input, Emitter<Pair<Integer, VectorOrPref>> emitter) {
						long userID = input.first();
						Vector userVector = input.second();
						Iterator<Vector.Element> it = userVector.iterateNonZero();
						while (it.hasNext()) {
							Vector.Element e = it.next();
							int itemIndex = e.index();
							float preferenceValue = (float) e.get();
							emitter.emit(Pair.of(itemIndex, new VectorOrPref(userID, preferenceValue)));
						}
					}
					
					@Override
					public float scaleFactor() {
						return est.getScaleFactor("S6").sizeFactor;
					}

					@Override
					public float scaleFactorByRecord() {
						return est.getScaleFactor("S6").recsFactor;
					}
				}, Writables.tableOf(Writables.ints(), VectorOrPref.vectorOrPrefs()));
		
		userVectorSplit = profiler.profile("S6", pipeline, userVectorSplit, ProfileConverter.int_vopp(),
				Writables.tableOf(Writables.ints(), VectorOrPref.vectorOrPrefs()));

		/*
		 * S7 Combine VectorOrPrefs
		 */
		PTable<Integer, VectorAndPrefs> combinedVectorOrPref = wrappedCooccurrence.union(userVectorSplit).groupByKey(est.getClusterSize()).parallelDo(
				new DoFn<Pair<Integer, Iterable<VectorOrPref>>, Pair<Integer, VectorAndPrefs>>() {

					@Override
					public void process(Pair<Integer, Iterable<VectorOrPref>> input,
							Emitter<Pair<Integer, VectorAndPrefs>> emitter) {
						Vector vector = null;
						List<Long> userIDs = Lists.newArrayList();
						List<Float> values = Lists.newArrayList();
						for (VectorOrPref vop : input.second()) {
							if (vector == null) {
								vector = vop.getVector();
							}
							long userID = vop.getUserID();
							if(userID != Long.MIN_VALUE){
								userIDs.add(vop.getUserID());
							}
							float value = vop.getValue();
							if(!Float.isNaN(value)){
								values.add(vop.getValue());
							}
						}
						emitter.emit(Pair.of(input.first(), new VectorAndPrefs(vector, userIDs, values)));
					}

					@Override
					public float scaleFactor() {
						return est.getScaleFactor("S7").sizeFactor;
					}

					@Override
					public float scaleFactorByRecord() {
						return est.getScaleFactor("S7").recsFactor;
					}
				}, Writables.tableOf(Writables.ints(), VectorAndPrefs.vectorAndPrefs()));
		
		combinedVectorOrPref = profiler.profile("S5+S6-S7", pipeline, combinedVectorOrPref, ProfileConverter.int_vap(),
				Writables.tableOf(Writables.ints(), VectorAndPrefs.vectorAndPrefs()));
		/*
		 * S8 Computing partial recommendation vectors
		 */
		PTable<Long, Vector> partialMultiply = combinedVectorOrPref.parallelDo(
				new DoFn<Pair<Integer, VectorAndPrefs>, Pair<Long, Vector>>() {
					@Override
					public void process(Pair<Integer, VectorAndPrefs> input, Emitter<Pair<Long, Vector>> emitter) {
						Vector cooccurrenceColumn = input.second().getVector();
						List<Long> userIDs = input.second().getUserIDs();
						List<Float> prefValues = input.second().getValues();
						for (int i = 0; i < userIDs.size(); i++) {
							long userID = userIDs.get(i);
							if (userID != Long.MIN_VALUE) {
								float prefValue = prefValues.get(i);
								Vector partialProduct = cooccurrenceColumn.times(prefValue);
								emitter.emit(Pair.of(userID, partialProduct));
							}
						}
					}
					
					@Override
					public float scaleFactor() {
						return est.getScaleFactor("S8").sizeFactor;
					}

					@Override
					public float scaleFactorByRecord() {
						return est.getScaleFactor("S8").recsFactor;
					}
					
				}, Writables.tableOf(Writables.longs(), Writables.vectors())).groupByKey(est.getClusterSize()).combineValues(new CombineFn<Long, Vector>(){

					@Override
					public void process(Pair<Long, Iterable<Vector>> input, Emitter<Pair<Long, Vector>> emitter) {
						Vector partial = null;
						for (Vector vector : input.second()) {
							partial = partial == null ? vector : partial.plus(vector);
						}
						emitter.emit(Pair.of(input.first(), partial));
					}
					
					@Override
					public float scaleFactor() {
						return est.getScaleFactor("combine").sizeFactor;
					}

					@Override
					public float scaleFactorByRecord() {
						return est.getScaleFactor("combine").recsFactor;
					}
				});
		
		partialMultiply = profiler.profile("S8-combine", pipeline, partialMultiply, ProfileConverter.long_vector(),
				Writables.tableOf(Writables.longs(), Writables.vectors()));
		
		/*
		 * S9 Producing recommendations from vectors
		 */
		PTable<Long, RecommendedItems> recommendedItems = partialMultiply.parallelDo(
				new DoFn<Pair<Long, Vector>, Pair<Long, RecommendedItems>>() {

					@Override
					public void process(Pair<Long, Vector> input, Emitter<Pair<Long, RecommendedItems>> emitter) {
						Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(11, Collections
								.reverseOrder(BY_PREFERENCE_VALUE));
						Iterator<Vector.Element> recommendationVectorIterator = input.second().iterateNonZero();
						while (recommendationVectorIterator.hasNext()) {
							Vector.Element element = recommendationVectorIterator.next();
							int index = element.index();
							float value = (float) element.get();
							if (topItems.size() < TOP) {
								topItems.add(new GenericRecommendedItem(index, value));
							} else if (value > topItems.peek().getValue()) {
								topItems.add(new GenericRecommendedItem(index, value));
								topItems.poll();
							}
						}
						List<RecommendedItem> recommendations = new ArrayList<RecommendedItem>(topItems.size());
						recommendations.addAll(topItems);
						Collections.sort(recommendations, BY_PREFERENCE_VALUE);
						emitter.emit(Pair.of(input.first(), new RecommendedItems(recommendations)));
					}
					
					@Override
					public float scaleFactor() {
						return est.getScaleFactor("S9").sizeFactor;
					}

					@Override
					public float scaleFactorByRecord() {
						return est.getScaleFactor("S9").recsFactor;
					}

				}, Writables.tableOf(Writables.longs(), RecommendedItems.recommendedItems()));
		
		recommendedItems = profiler.profile("S9", pipeline, recommendedItems, ProfileConverter.long_ri(),
				Writables.tableOf(Writables.longs(), RecommendedItems.recommendedItems()));
		
		/*
		 * Profiling
		 */
		if(profiler.isProfiling()){
			pipeline.done();
			profiler.writeResultToFile(profileFilePath);
			profiler.cleanup(pipeline.getConfiguration());
			return 0;
		}
		/*
		 * asText
		 */
		pipeline.writeTextFile(recommendedItems, args[1]);
		PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}
	
	public void printUsage(){
		System.out.println("profiling: profiling=[profile filename] to enable profiling, otherwise disable");
		System.out.println("estimation: estimation=[the profile file generated by profiling]");
		System.out.println("clustersize: clustersize=[the number of node in your cluster]");
	}

	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		int result = ToolRunner.run(new Configuration(), new Recommender(), args);
		System.exit(result);
	}
}
