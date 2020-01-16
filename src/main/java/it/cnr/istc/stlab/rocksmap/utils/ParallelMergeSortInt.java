package it.cnr.istc.stlab.rocksmap.utils;

import java.util.concurrent.ForkJoinPool;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;

public class ParallelMergeSortInt extends java.util.concurrent.RecursiveAction {

	private static final long serialVersionUID = 1L;
	private static final int SMALL = 1 << 13;
	private final int from;
	private final int to;
	private final IntComparator comp;
	private final Swapper swapper;

	public ParallelMergeSortInt(final int from, final int to, final IntComparator comp, final Swapper swapper) {
		super();
		this.from = from;
		this.to = to;
		this.comp = comp;
		this.swapper = swapper;
	}

	@Override
	protected void compute() {
		final int length = to - from;
		// Insertion sort on smallest arrays
		if (length < SMALL) {
//			for (long i = from; i < to; i++) {
//				for (long j = i; j > from && (comp.compare(j - 1, j) > 0); j--) {
//					swapper.swap(j, j - 1);
//				}
//			}
			Arrays.mergeSort(from, to, comp, swapper);
			return;
		}
		// Recursively sort halves
		int mid = (from + to) >>> 1;
		ParallelMergeSortInt h0 = new ParallelMergeSortInt(from, mid, comp, swapper);
		ParallelMergeSortInt h1 = new ParallelMergeSortInt(mid, to, comp, swapper);
		invokeAll(h0, h1);
		// If list is already sorted, nothing left to do. This is an
		// optimization that results in faster sorts for nearly ordered lists.
		if (comp.compare(mid - 1, mid) <= 0)
			return;
		// Merge sorted halves
		invokeAll(new ParallelInPlaceMerge(from, mid, to, comp, swapper));
	}

	public static void mergeSort(final int from, final int to, final IntComparator comp,
			final Swapper swapper) {
		ParallelMergeSortInt pms = new ParallelMergeSortInt(from, to, comp, swapper);
		final ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
		pool.invoke(pms);
		pool.shutdown();
	}

	class ParallelInPlaceMerge extends java.util.concurrent.RecursiveAction {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private int from;
		private int mid;
		private int to;
		private IntComparator comp;
		private Swapper swapper;

		ParallelInPlaceMerge(int from, int mid, int to, IntComparator comp, Swapper swapper) {
			super();
			this.from = from;
			this.mid = mid;
			this.to = to;
			this.comp = comp;
			this.swapper = swapper;
		}

		@Override
		protected void compute() {
			if (from >= mid || mid >= to)
				return;
			if (to - from == 2) {
				if (comp.compare(mid, from) < 0) {
					swapper.swap(from, mid);
				}
				return;
			}
			int firstCut;
			int secondCut;
			if (mid - from > to - mid) {
				firstCut = from + (mid - from) / 2;
				secondCut = lowerBound(mid, to, firstCut, comp);
			} else {
				secondCut = mid + (to - mid) / 2;
				firstCut = upperBound(from, mid, secondCut, comp);
			}
			int first2 = firstCut;
			int middle2 = mid;
			int last2 = secondCut;
			if (middle2 != first2 && middle2 != last2) {
				int first1 = first2;
				int last1 = middle2;
				while (first1 < --last1)
					swapper.swap(first1++, last1);
				first1 = middle2;
				last1 = last2;
				while (first1 < --last1)
					swapper.swap(first1++, last1);
				first1 = first2;
				last1 = last2;
				while (first1 < --last1)
					swapper.swap(first1++, last1);
			}
			mid = firstCut + (secondCut - mid);
			ParallelInPlaceMerge pinm1 = new ParallelInPlaceMerge(from, firstCut, mid, comp, swapper);
			ParallelInPlaceMerge pinm2 = new ParallelInPlaceMerge(mid, secondCut, to, comp, swapper);
			invokeAll(pinm1, pinm2);

		}

	}

	/**
	 * Performs a binary search on an already sorted range: finds the first position
	 * where an element can be inserted without violating the ordering. Sorting is
	 * by a user-supplied comparison function.
	 *
	 * @param mid      Beginning of the range.
	 * @param to       One past the end of the range.
	 * @param firstCut Element to be searched for.
	 * @param comp     Comparison function.
	 * @return The largest index i such that, for every j in the range
	 *         {@code [first, i)}, {@code comp.apply(array[j], x)} is {@code true}.
	 */
	private static int lowerBound(int mid, final int to, final int firstCut, final IntComparator comp) {
		int len = to - mid;
		while (len > 0) {
			int half = len / 2;
			int middle = mid + half;
			if (comp.compare(middle, firstCut) < 0) {
				mid = middle + 1;
				len -= half + 1;
			} else {
				len = half;
			}
		}
		return mid;
	}

	/**
	 * Performs a binary search on an already-sorted range: finds the last position
	 * where an element can be inserted without violating the ordering. Sorting is
	 * by a user-supplied comparison function.
	 *
	 * @param from      Beginning of the range.
	 * @param mid       One past the end of the range.
	 * @param secondCut Element to be searched for.
	 * @param comp      Comparison function.
	 * @return The largest index i such that, for every j in the range
	 *         {@code [first, i)}, {@code comp.apply(x, array[j])} is {@code false}.
	 */
	private static int upperBound(int from, final int mid, final int secondCut, final IntComparator comp) {
		int len = mid - from;
		while (len > 0) {
			int half = len / 2;
			int middle = from + half;
			if (comp.compare(secondCut, middle) < 0) {
				len = half;
			} else {
				from = middle + 1;
				len -= half + 1;
			}
		}
		return from;
	}

}
