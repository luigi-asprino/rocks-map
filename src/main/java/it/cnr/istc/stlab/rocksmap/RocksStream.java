package it.cnr.istc.stlab.rocksmap;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class RocksStream<K> implements Stream<K> {

	final private RocksBigList<K> list;

	RocksStream(final RocksBigList<K> list) {
		this.list = list;
	}

	@Override
	public Iterator<K> iterator() {
		return list.iterator();
	}

	@Override
	public Spliterator<K> spliterator() {
		return list.spliterator();
	}

	@Override
	public boolean isParallel() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Stream<K> sequential() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> parallel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> unordered() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> onClose(Runnable closeHandler) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public Stream<K> filter(Predicate<? super K> predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> Stream<R> map(Function<? super K, ? extends R> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IntStream mapToInt(ToIntFunction<? super K> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LongStream mapToLong(ToLongFunction<? super K> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DoubleStream mapToDouble(ToDoubleFunction<? super K> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> Stream<R> flatMap(Function<? super K, ? extends Stream<? extends R>> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IntStream flatMapToInt(Function<? super K, ? extends IntStream> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LongStream flatMapToLong(Function<? super K, ? extends LongStream> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DoubleStream flatMapToDouble(Function<? super K, ? extends DoubleStream> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> distinct() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> sorted() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> sorted(Comparator<? super K> comparator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> peek(Consumer<? super K> action) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> limit(long maxSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<K> skip(long n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void forEach(Consumer<? super K> action) {
		// TODO Auto-generated method stub

	}

	@Override
	public void forEachOrdered(Consumer<? super K> action) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <A> A[] toArray(IntFunction<A[]> generator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K reduce(K identity, BinaryOperator<K> accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<K> reduce(BinaryOperator<K> accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <U> U reduce(U identity, BiFunction<U, ? super K, U> accumulator, BinaryOperator<U> combiner) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super K> accumulator, BiConsumer<R, R> combiner) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R, A> R collect(Collector<? super K, A, R> collector) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<K> min(Comparator<? super K> comparator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<K> max(Comparator<? super K> comparator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long count() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean anyMatch(Predicate<? super K> predicate) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean allMatch(Predicate<? super K> predicate) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean noneMatch(Predicate<? super K> predicate) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Optional<K> findFirst() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<K> findAny() {
		// TODO Auto-generated method stub
		return null;
	}

}
