package org.apache.crunch.types.writable;

import java.util.List;

import org.apache.crunch.MapFn;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

public class RecommendedItems {
	private List<RecommendedItem> recommended;

	@SuppressWarnings("serial")
	private static final MapFn<RecommendedItemsWritable, RecommendedItems> RIW_TO_RI = new MapFn<RecommendedItemsWritable, RecommendedItems>() {
		@Override
		public RecommendedItems map(RecommendedItemsWritable input) {
			return new RecommendedItems(input.getRecommendedItems());
		}
	};

	@SuppressWarnings("serial")
	private static final MapFn<RecommendedItems, RecommendedItemsWritable> RI_TO_RIW = new MapFn<RecommendedItems, RecommendedItemsWritable>() {
		@Override
		public RecommendedItemsWritable map(RecommendedItems input) {
			return new RecommendedItemsWritable(input.getRecommendedItems());
		}
	};

	private static <S, W extends Writable> WritableType<S, W> create(Class<S> typeClass, Class<W> writableClass,
			MapFn<W, S> inputDoFn, MapFn<S, W> outputDoFn) {
		return new WritableType<S, W>(typeClass, writableClass, inputDoFn, outputDoFn);
	}

	private static final WritableType<RecommendedItems, RecommendedItemsWritable> recommendedItems = create(
			RecommendedItems.class, RecommendedItemsWritable.class, RIW_TO_RI, RI_TO_RIW);

	public static final WritableType<RecommendedItems, RecommendedItemsWritable> recommendedItems() {
		return recommendedItems;
	}

	public RecommendedItems() {
		// do nothing
	}

	public RecommendedItems(List<RecommendedItem> recommended) {
		this.recommended = recommended;
	}

	public List<RecommendedItem> getRecommendedItems() {
		return recommended;
	}

	public void set(List<RecommendedItem> recommended) {
		this.recommended = recommended;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder(200);
		result.append('[');
		boolean first = true;
		for (RecommendedItem item : recommended) {
			if (first) {
				first = false;
			} else {
				result.append(',');
			}
			result.append(String.valueOf(item.getItemID()));
			result.append(':');
			result.append(String.valueOf(item.getValue()));
		}
		result.append(']');
		return result.toString();
	}
}
