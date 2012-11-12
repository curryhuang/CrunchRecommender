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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import cn.edu.bjtu.cit.recommender.profile.ProfileConverter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

@SuppressWarnings("serial")
public class Top20CoBuyItems extends Configured implements Tool, Serializable {
	
	public static final String PROFILING = "profiling";
	public static final String PROFILING_SHORT = "p";
	public static final String CLUSTER_SIZE = "clustersize";
	public static final String CLUSTER_SIZE_SHORT = "cs";
	public static final String ESTIMATION = "estimation";
	public static final String ESTIMATION_SHORT = "est";
	public static final int TWENTY = 20;
	public static final int THREE = 3;
	
	private final Log log = LogFactory.getLog(Top20CoBuyItems.class);
	private static final Comparator<Pair<String, Integer>> BY_PAIR_COUNT = new Comparator<Pair<String, Integer>>(){
		@Override
		public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
			if(o1.second() < o2.second()){
				return 1;
			}
			else if(o1.second() > o2.second()){
				return -1;
			}
			else{
				return 0;
			}
		}
	};
	
	private Profiler profiler;
	private Estimator est;
	private Map<String, String> options;
	private String profileFilePath;
	private int clusterSize = 1;
	private String estFile;

	public Top20CoBuyItems() {
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
				options.put(tokens[0], tokens[1]);
			}
		}
	}

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
		
		Pipeline pipeline = new MRPipeline(Top20CoBuyItems.class, getConf());
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
				String[] split = input.split("\\s+");
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
		 * S2 + GBK + combine
		 */
		PTable<String, Integer> coOccurenceCount = userVector.parallelDo(
				new DoFn<Pair<Long, Vector>, Pair<String, Integer>>() {
					@Override
					public void process(Pair<Long, Vector> input, Emitter<Pair<String, Integer>> emitter) {
						Iterator<Vector.Element> it = input.second().iterateNonZero();
						while (it.hasNext()) {
							int index1 = it.next().index();
							Iterator<Vector.Element> it2 = input.second().iterateNonZero();
							while (it2.hasNext()) {
								int index2 = it2.next().index();
								if(index1 < index2){
									Pair<String, Integer> emitted = Pair.of(index1 + "#" + index2, 1);
									emitter.emit(emitted);
								}
							}
						}
					}

					@Override
					public float scaleFactor() {
						float size = est.getScaleFactor("S2").sizeFactor;
						return size;
					}

					@Override
					public float scaleFactorByRecord() {
						float recs = est.getScaleFactor("S2").recsFactor;
						return recs;
					}
				}, Writables.tableOf(Writables.strings(), Writables.ints())).groupByKey(est.getClusterSize()).combineValues(new CombineFn<String, Integer>(){

					@Override
					public void process(Pair<String, Iterable<Integer>> input, Emitter<Pair<String, Integer>> emitter) {
						int total = 0;
						for(int n : input.second()){
							total += n;
						}
						emitter.emit(Pair.of(input.first(), total));
					}
				});
		
		/*
		 * S3
		 */
		PTable<Integer, Pair<String, Integer>> labeledCount = coOccurenceCount.parallelDo(new MapFn<Pair<String, Integer>, Pair<Integer, Pair<String, Integer>>>(){

			@Override
			public Pair<Integer, Pair<String, Integer>> map(Pair<String, Integer> input) {
				int sectionID = input.hashCode() % (clusterSize * 2);
				return Pair.of(sectionID, input);
			}
		}, Writables.tableOf(Writables.ints(), Writables.tableOf(Writables.strings(), Writables.ints())));
		
		/*
		 * S4
		 */
		PTable<Integer, String> groupedResult = labeledCount.groupByKey().parallelDo(
				new DoFn<Pair<Integer, Iterable<Pair<String, Integer>>>, Pair<Integer, String>>() {

					@Override
					public void process(Pair<Integer, Iterable<Pair<String, Integer>>> input,
							Emitter<Pair<Integer, String>> emitter) {
						Queue<Pair<String, Integer>> heap = new PriorityQueue<Pair<String, Integer>>(TWENTY, new PairComparator());
						for(Pair<String, Integer> pair : input.second()){
							if(heap.isEmpty() || heap.size() < THREE){
								heap.add(pair);
							}
							else if(heap.size() > THREE && pair.second() > heap.peek().second()){
								heap.remove();
								heap.add(pair);
							}
						}
						StringBuffer sb = new StringBuffer();
						for(Pair<String, Integer> p : heap){
							sb.append(p.toString() + ",");
						}
						sb.deleteCharAt(sb.length() - 1);
						log.info(sb.toString());
						emitter.emit(Pair.of(1, sb.toString()));
					}
				},
				Writables.tableOf(Writables.ints(),
						Writables.strings()));
		
		/*
		 * GBK + S5
		 */
		PTable<String, Integer> finalResult = groupedResult.groupByKey().parallelDo(
						new DoFn<Pair<Integer, Iterable<String>>, Pair<String, Integer>>() {
							Pattern pattern = Pattern.compile("(\\d+#\\d+),(\\d+)");
							@Override
							public void process(Pair<Integer, Iterable<String>> input,
									Emitter<Pair<String, Integer>> emitter) {
								Queue<Pair<String, Integer>> heap = new PriorityQueue<Pair<String, Integer>>(TWENTY, new PairComparator());
								for(String str : input.second()){
									Matcher m = pattern.matcher(str);
									while(m.find()){
										Pair<String, Integer> pair = Pair.of(m.group(1), Integer.parseInt(m.group(2)));
										if(heap.isEmpty() || heap.size() < THREE){
											heap.add(pair);
										}
										else if(heap.size() == THREE && pair.second() > heap.peek().second()){
											heap.remove();
											heap.add(pair);
										}
									}
								}
								Stack<Pair<String, Integer>> stack = new Stack<Pair<String, Integer>>();
								while(!heap.isEmpty()){
									stack.push(heap.remove());
								}
								while(!stack.isEmpty()){
									emitter.emit(stack.pop());
								}
							}
						}, Writables.tableOf(Writables.strings(), Writables.ints()));
				

		if(profiler.isProfiling()){
			pipeline.done();
			profiler.writeResultToFile(profileFilePath);
			profiler.cleanup(pipeline.getConfiguration());
			return 0;
		}

		/*
		 * asText
		 */
		pipeline.writeTextFile(finalResult, args[1]);
		PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}
	
	private static class PairComparator implements Comparator<Pair<String, Integer>>, Serializable {
		
		private int one;
		
		public PairComparator(boolean reversed){
			one = reversed ? 1 : -1;
		}
		
		public PairComparator(){
			this(false);
		}
		
		@Override
		public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
			if (o1.second() < o2.second()) {
				return one;
			} else if (o1.second() > o2.second()) {
				return one * (-1);
			} else {
				return 0;
			}
		}
	};
	
	public void printUsage(){
		System.out.println("profiling: profiling=[profile filename] to enable profiling, otherwise disable");
		System.out.println("estimation: estimation=[the profile file generated by profiling]");
		System.out.println("clustersize: clustersize=[the number of node in your cluster]");
	}
	
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		int result = ToolRunner.run(new Configuration(), new Top20CoBuyItems(), args);
		System.exit(result);
	}
}
