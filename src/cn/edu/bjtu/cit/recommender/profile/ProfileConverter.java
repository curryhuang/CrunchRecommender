package cn.edu.bjtu.cit.recommender.profile;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.types.writable.RecommendedItems;
import org.apache.crunch.types.writable.VectorAndPrefs;
import org.apache.crunch.types.writable.VectorOrPref;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class ProfileConverter implements Serializable{
	
	public static MapFn<String, Pair<Long, Vector>> long_vector(){
		return new MapFn<String, Pair<Long, Vector>>(){

			@Override
			public Pair<Long, Vector> map(String input) {
				Pattern pattern = Pattern.compile("(\\d+):(\\d+\\.\\d+)");
				Matcher m = pattern.matcher(input);
				Vector v = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
				String[] key_value = input.split("\\s+");
				Long key = Long.parseLong(key_value[0]);
				while(m.find()){
					int index = Integer.parseInt(m.group(1));
					float value = Float.parseFloat(m.group(2));
					v.set(index, value);
				}
				return Pair.of(key, v);
			}
		};
	}
	
	public static MapFn<String, Pair<Integer, Vector>> int_vector(){
		return new MapFn<String, Pair<Integer, Vector>>(){

			@Override
			public Pair<Integer, Vector> map(String input) {
				Pattern pattern = Pattern.compile("(\\d+):(\\d+\\.\\d+)");
				Matcher m = pattern.matcher(input);
				Vector v = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
				String[] key_value = input.split("\\s+");
				Integer key = Integer.parseInt(key_value[0]);
				while(m.find()){
					int index = Integer.parseInt(m.group(1));
					float value = Float.parseFloat(m.group(2));
					v.set(index, value);
				}
				return Pair.of(key, v);
			}
		};
	}
	
	public static MapFn<String, Pair<Integer, VectorOrPref>> int_vopv(){
		return new MapFn<String, Pair<Integer, VectorOrPref>>(){

			@Override
			public Pair<Integer, VectorOrPref> map(String input) {
				Pattern pattern = Pattern.compile("(\\d+):(\\d+\\.\\d+)");
				Matcher m = pattern.matcher(input);
				Vector v = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
				String[] key_value = input.split("\\s+");
				int key = Integer.parseInt(key_value[0]);
				while(m.find()){
					int index = Integer.parseInt(m.group(1));
					float value = Float.parseFloat(m.group(2));
					v.set(index, value);
				}
				return Pair.of(key, new VectorOrPref(v));
			}
		};
	}
	
	public static MapFn<String, Pair<Integer, VectorOrPref>> int_vopp(){
		return new MapFn<String, Pair<Integer, VectorOrPref>>(){

			@Override
			public Pair<Integer, VectorOrPref> map(String input) {
				Pattern pattern = Pattern.compile("(\\d+):(\\d\\.\\d)");
				Matcher m = pattern.matcher(input);
				String[] key_value = input.split("\\s+");
				int key = Integer.parseInt(key_value[0]);
				if(m.find()){
					int index = Integer.parseInt(m.group(1));
					float value = Float.parseFloat(m.group(2));
					return Pair.of(key, new VectorOrPref(index, value));
				}
				return null;
			}
		};
	}
	
	public static MapFn<String, Pair<Integer, VectorAndPrefs>> int_vap(){
		return new MapFn<String, Pair<Integer, VectorAndPrefs>>(){

			@Override
			public Pair<Integer, VectorAndPrefs> map(String input) {
				String[] tokens = input.split("\t");
				int key = Integer.parseInt(tokens[0]);
				
				Pattern pattern = Pattern.compile("(\\d+):(\\d\\.\\d)");
				Matcher m = pattern.matcher(tokens[1]);
				Vector v = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
				while(m.find()){
					int index = Integer.parseInt(m.group(1));
					float value = Float.parseFloat(m.group(2));
					v.set(index, value);
				}
				
				pattern = Pattern.compile("\\d+");
				m = pattern.matcher(tokens[2]);
				List<Long> userIDs = Lists.newArrayList();
				while(m.find()){
					long userID = Long.parseLong(m.group(0));
					userIDs.add(userID);
				}
				
				pattern = Pattern.compile("\\d+\\.\\d+");
				m = pattern.matcher(tokens[3]);
				List<Float> values = Lists.newArrayList();
				while(m.find()){
					float value = Float.parseFloat(m.group(0));
					values.add(value);
				}
				
				return Pair.of(key, new VectorAndPrefs(v, userIDs, values));
			}
		};
	}

	public static MapFn<String, Pair<Long, RecommendedItems>> long_ri() {
		return new MapFn<String, Pair<Long, RecommendedItems>>(){
			@Override
			public Pair<Long, RecommendedItems> map(String input) {
				Long userID = Long.parseLong(input.split("\t")[0]);
				Pattern pattern = Pattern.compile("(\\d+):(\\d+\\.\\d+)");
				Matcher m = pattern.matcher(input);
				List<RecommendedItem> items = Lists.newArrayList();
				while(m.find()){
					long itemID = Long.parseLong(m.group(1));
					float value = Float.parseFloat(m.group(2));
					items.add(new GenericRecommendedItem(itemID, value));
				}
				return Pair.of(userID, new RecommendedItems(items));
			}
		};
	}
}
