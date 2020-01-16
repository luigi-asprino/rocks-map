package it.cnr.istc.stlab.rocksmap.utils;

import java.util.concurrent.ForkJoinPool;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.BigSwapper;
import it.unimi.dsi.fastutil.longs.LongComparator;

public class ParallelMergeSort extends java.util.concurrent.RecursiveAction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2670942168968422028L;
	private static final int SMALL = 1 << 11;
	private final long from;
	private final long to;
	private final LongComparator comp;
	private final BigSwapper swapper;

	public ParallelMergeSort(final long from, final long to, final LongComparator comp, final BigSwapper swapper) {
		super();
		this.from = from;
		this.to = to;
		this.comp = comp;
		this.swapper = swapper;
	}

	@Override
	protected void compute() {
		final long length = to - from;
		// Insertion sort on smallest arrays
		if (length < SMALL) {
			BigArrays.mergeSort(from, to, comp, swapper);
			return;
		}
		// Recursively sort halves
		final long mid = (from + to) >>> 1;
		ParallelMergeSort h0 = new ParallelMergeSort(from, mid, comp, swapper);
		ParallelMergeSort h1 = new ParallelMergeSort(mid, to, comp, swapper);
		invokeAll(h0, h1);
		// If list is already sorted, nothing left to do. This is an
		// optimization that results in faster sorts for nearly ordered lists.
		if (comp.compare(mid - 1, mid) <= 0)
			return;
		// Merge sorted halves
		invokeAll(new ParallelInPlaceMerge(from, mid, to, comp, swapper));
	}

	public static void mergeSort(final long from, final long to, final LongComparator comp, final BigSwapper swapper) {
		ParallelMergeSort pms = new ParallelMergeSort(from, to, comp, swapper);
		final ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
		pool.invoke(pms);
		pool.shutdown();
	}

	class ParallelInPlaceMerge extends java.util.concurrent.RecursiveAction {

		/**
		 * 
		 */
		private static final long serialVersionUID = 3939883219628101921L;
		private final long from;
		private long mid;
		private final long to;
		private final LongComparator comp;
		private final BigSwapper swapper;

		ParallelInPlaceMerge(final long from, final long mid, final long to, final LongComparator comp,
				final BigSwapper swapper) {
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
			final long firstCut;
			final long secondCut;
			if (mid - from > to - mid) {
				firstCut = from + (mid - from) / 2;
				secondCut = lowerBound(mid, to, firstCut, comp);
			} else {
				secondCut = mid + (to - mid) / 2;
				firstCut = upperBound(from, mid, secondCut, comp);
			}
			final long first2 = firstCut;
			final long middle2 = mid;
			final long last2 = secondCut;
			if (middle2 != first2 && middle2 != last2) {
				long first1 = first2;
				long last1 = middle2;
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
	private static long lowerBound(long mid, final long to, final long firstCut, final LongComparator comp) {
		long len = to - mid;
		while (len > 0) {
			long half = len / 2;
			long middle = mid + half;
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
	private static long upperBound(long from, final long mid, final long secondCut, final LongComparator comp) {
		long len = mid - from;
		while (len > 0) {
			long half = len / 2;
			long middle = from + half;
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
