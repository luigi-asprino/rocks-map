package it.cnr.istc.stlab.rocksmap.utils;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.BigSwapper;
import it.unimi.dsi.fastutil.longs.LongComparator;

public class ParallelQuickSort extends RecursiveAction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6485633807126626246L;

	private final long from, to;
	private final LongComparator comp;
	private final BigSwapper swapper;

//	private static final int SMALL = 8192;
	private static final int PARALLEL_QUICKSORT_NO_FORK = 8192;

	public ParallelQuickSort(long from, long to, LongComparator comp, BigSwapper swapper) {
		super();
		this.from = from;
		this.to = to;
		this.comp = comp;
		this.swapper = swapper;
	}

	/**
	 * Returns the index of the median of the three indexed chars.
	 */
	private static long med3(final long a, final long b, final long c, final LongComparator comp) {
		int ab = comp.compare(a, b);
		int ac = comp.compare(a, c);
		int bc = comp.compare(b, c);
		return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
	}

	/** Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)]. */
	private static void vecSwap(final BigSwapper swapper, long from, long l, final long s) {
		for (int i = 0; i < s; i++, from++, l++)
			swapper.swap(from, l);
	}

	@Override
	protected void compute() {

		final long len = to - from;
		if (len < PARALLEL_QUICKSORT_NO_FORK) {
//			quickSort(from, to, comp, swapper);
			BigArrays.quickSort(from, to, comp, swapper);
			return;
		}
		// Choose a partition element, v
		long m = from + len / 2;
		long l = from;
		long n = to - 1;
		long s = len / 8;
		l = med3(l, l + s, l + 2 * s, comp);
		m = med3(m - s, m, m + s, comp);
		n = med3(n - 2 * s, n - s, n, comp);
		m = med3(l, m, n, comp);
		// Establish Invariant: v* (<v)* (>v)* v*
		long a = from, b = a, c = to - 1, d = c;
		while (true) {
			int comparison;
			while (b <= c && ((comparison = comp.compare(b, m)) <= 0)) {
				if (comparison == 0) {
					// Fix reference to pivot if necessary
					if (a == m)
						m = b;
					else if (b == m)
						m = a;
					swapper.swap(a++, b);
				}
				b++;
			}
			while (c >= b && ((comparison = comp.compare(c, m)) >= 0)) {
				if (comparison == 0) {
					// Fix reference to pivot if necessary
					if (c == m)
						m = d;
					else if (d == m)
						m = c;
					swapper.swap(c, d--);
				}
				c--;
			}
			if (b > c)
				break;
			// Fix reference to pivot if necessary
			if (b == m)
				m = d;
			else if (c == m)
				m = c;
			swapper.swap(b++, c--);
		}

		// Swap partition elements back to middle
		s = Math.min(a - from, b - a);
		vecSwap(swapper, from, b - s, s);
		s = Math.min(d - c, to - d - 1);
		vecSwap(swapper, b, to - s, s);

		// Recursively sort non-partition-elements
		long t;
		s = b - a;
		t = d - c;
		if (s > 1 && t > 1)
			invokeAll(new ParallelQuickSort(from, from + s, comp, swapper),
					new ParallelQuickSort(to - t, to, comp, swapper));
		else if (s > 1)
			invokeAll(new ParallelQuickSort(from, from + s, comp, swapper));
		else
			invokeAll(new ParallelQuickSort(to - t, to, comp, swapper));
	}

	public static void quickSort(final long from, final long to, final LongComparator comp,
			final BigSwapper swapper) {
		final ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
		pool.invoke(new ParallelQuickSort(from, to, comp, swapper));
		pool.shutdown();
	}

}
