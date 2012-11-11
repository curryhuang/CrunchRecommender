package org.apache.crunch.types.writable;

import java.util.List;

import org.apache.crunch.MapFn;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.math.Vector;

public class VectorAndPrefs {
	private Vector vector;
	private List<Long> userIDs;
	private List<Float> values;

	@SuppressWarnings("serial")
	private static final MapFn<VectorAndPrefsWritable, VectorAndPrefs> VAPW_TO_VAP = new MapFn<VectorAndPrefsWritable, VectorAndPrefs>() {
		@Override
		public VectorAndPrefs map(VectorAndPrefsWritable input) {
			VectorAndPrefs vap = new VectorAndPrefs(input.getVector(), input.getUserIDs(), input.getValues());
			return vap;
		}
	};

	@SuppressWarnings("serial")
	private static final MapFn<VectorAndPrefs, VectorAndPrefsWritable> VAP_TO_VAPW = new MapFn<VectorAndPrefs, VectorAndPrefsWritable>() {
		@Override
		public VectorAndPrefsWritable map(VectorAndPrefs input) {
			VectorAndPrefsWritable vapw = new VectorAndPrefsWritable(input.getVector(), input.getUserIDs(),
					input.getValues());
			return vapw;
		}
	};

	private static <S, W extends Writable> WritableType<S, W> create(Class<S> typeClass, Class<W> writableClass,
			MapFn<W, S> inputDoFn, MapFn<S, W> outputDoFn) {
		return new WritableType<S, W>(typeClass, writableClass, inputDoFn, outputDoFn);
	}

	private static final WritableType<VectorAndPrefs, VectorAndPrefsWritable> vectorAndPrefs = create(
			VectorAndPrefs.class, VectorAndPrefsWritable.class, VAPW_TO_VAP, VAP_TO_VAPW);

	public static final WritableType<VectorAndPrefs, VectorAndPrefsWritable> vectorAndPrefs() {
		return vectorAndPrefs;
	}

	public VectorAndPrefs() {
	}

	public VectorAndPrefs(Vector vector, List<Long> userIDs, List<Float> values) {
		this.vector = vector;
		this.userIDs = userIDs;
		this.values = values;
	}

	public Vector getVector() {
		return vector;
	}

	public List<Long> getUserIDs() {
		return userIDs;
	}

	public List<Float> getValues() {
		return values;
	}

	@Override
	public String toString() {
		return vector + "\t" + userIDs + '\t' + values;
	}
}
