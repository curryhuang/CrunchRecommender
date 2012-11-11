package org.apache.crunch.types.writable;

import org.apache.crunch.MapFn;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;

public class VectorOrPref {

	private Vector vector;
	private long userID;
	private float value;

	@SuppressWarnings("serial")
	private static final MapFn<VectorOrPrefWritable, VectorOrPref> VOPW_TO_VOP = new MapFn<VectorOrPrefWritable, VectorOrPref>() {
		@Override
		public VectorOrPref map(VectorOrPrefWritable input) {
			VectorOrPref vop = new VectorOrPref();
			Vector vector = input.getVector();
			if (vector != null) {
				vop.set(vector);
			} else {
				vop.set(input.getUserID(), input.getValue());
			}
			return vop;
		}
	};

	@SuppressWarnings("serial")
	private static final MapFn<VectorOrPref, VectorOrPrefWritable> VOP_TO_VOPW = new MapFn<VectorOrPref, VectorOrPrefWritable>() {
		@Override
		public VectorOrPrefWritable map(VectorOrPref input) {
			VectorOrPrefWritable vopw = new VectorOrPrefWritable();
			Vector vector = input.getVector();
			if (vector != null) {
				vopw.set(vector);
			} else {
				vopw.set(input.getUserID(), input.getValue());
			}
			return vopw;
		}
	};

	private static <S, W extends Writable> WritableType<S, W> create(Class<S> typeClass, Class<W> writableClass,
			MapFn<W, S> inputDoFn, MapFn<S, W> outputDoFn) {
		return new WritableType<S, W>(typeClass, writableClass, inputDoFn, outputDoFn);
	}

	private static final WritableType<VectorOrPref, VectorOrPrefWritable> vectorOrPrefs = create(VectorOrPref.class,
			VectorOrPrefWritable.class, VOPW_TO_VOP, VOP_TO_VOPW);

	public static final WritableType<VectorOrPref, VectorOrPrefWritable> vectorOrPrefs() {
		return vectorOrPrefs;
	}

	public VectorOrPref() {
	}

	public VectorOrPref(Vector vector) {
		this.vector = vector;
	}

	public VectorOrPref(long userID, float value) {
		this.userID = userID;
		this.value = value;
	}

	public Vector getVector() {
		return vector;
	}

	public long getUserID() {
		return userID;
	}

	public float getValue() {
		return value;
	}

	public void set(Vector vector) {
		this.vector = vector;
		this.userID = Long.MIN_VALUE;
		this.value = Float.NaN;
	}

	public void set(long userID, float value) {
		this.vector = null;
		this.userID = userID;
		this.value = value;
	}

	@Override
	public String toString() {
		return vector == null ? userID + ":" + value : vector.toString();
	}

}
