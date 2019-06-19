package it.cnr.istc.stlab.rocksmap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class CachedRocksMultiMap<K, V> extends RocksMultiMap<K, V> {

	public static int DEFAULT_NUMBER_OF_PUT_OPERATIONS_FOR_CHECKPOINT = 10000;
	private int numberOfPutOperationsBeforeCheckPoint = DEFAULT_NUMBER_OF_PUT_OPERATIONS_FOR_CHECKPOINT;
	private Map<K, Collection<V>> buffer;
	private int counter = DEFAULT_NUMBER_OF_PUT_OPERATIONS_FOR_CHECKPOINT;

	public CachedRocksMultiMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer) throws RocksDBException {
		super(rocksDBPath, keyTransformer, valueTransformer);
		buffer = new HashMap<K, Collection<V>>();
	}

	public int getNumberOfPutOperationsBeforeCheckPoint() {
		return numberOfPutOperationsBeforeCheckPoint;
	}

	public void setNumberOfPutOperationsBeforeCheckPoint(int numberOfPutOperationsBeforeCheckPoint) {
		counter = numberOfPutOperationsBeforeCheckPoint;
		this.numberOfPutOperationsBeforeCheckPoint = numberOfPutOperationsBeforeCheckPoint;
	}

	@Override
	public boolean containsKey(Object key) {
		@SuppressWarnings("unchecked")
		K k = (K) key;
		try {
			byte[] v = db.get(keyTransformer.transform(k));
			return v != null || buffer.containsKey(k);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean put(K key, V value) {

		Collection<V> value_collection = buffer.get(key);
		if (value_collection == null) {
			value_collection = super.get(key);
			if (value_collection == null) {
				value_collection = new HashSet<>();
			}
			buffer.put(key, value_collection);
		}
		value_collection.add(value);
		counter++;
		checkBuffer();
		return true;
	}

	private void checkBuffer() {
		if (counter == numberOfPutOperationsBeforeCheckPoint) {
			emptyBuffer();
		}
	}

	private void emptyBuffer() {
		buffer.forEach((k, v) -> {
			super.putAll(k, v);
		});
		buffer = new HashMap<K, Collection<V>>();
		counter = 0;
	}

	@Override
	public boolean putAll(K key, Iterable<? extends V> values) {
		Collection<V> value_collection = buffer.get(key);
		if (value_collection == null) {
			value_collection = super.get(key);
			if (value_collection == null) {
				value_collection = new HashSet<>();
			}
			buffer.put(key, value_collection);
		}
		for (V v : values) {
			value_collection.add(v);
		}
		counter++;
		checkBuffer();
		return true;
	}

	@Override
	public Collection<V> removeAll(Object key) {
		if (buffer.containsKey(key)) {
			super.removeAll(key);
			return buffer.remove(key);
		}
		return super.removeAll(key);
	}

	@Override
	public void clear() {
		super.clear();
		buffer = new HashMap<K, Collection<V>>();
		counter = 0;
	}

	@Override
	public Collection<V> get(K key) {
		Collection<V> result = null;
		result = buffer.get(key);
		if (result == null) {
			result = super.get(key);
		}
		return result;
	}

	@Override
	public Set<K> keySet() {
		Set<K> keys = new HashSet<>();
		keys.addAll(super.keySet());
		keys.addAll(buffer.keySet());
		return keys;
	}

	@Override
	public Collection<V> values() {
		Set<V> values = new HashSet<>();
		values.addAll(super.values());
		buffer.values().forEach(s -> {
			values.addAll(s);
		});

		return values;
	}

	@Override
	public Collection<Entry<K, V>> entries() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<K, Collection<V>> asMap() {
		throw new UnsupportedOperationException();
	}

	public void close() {
		logger.info("Closing {}... skipped", rocksDBPath);
		emptyBuffer();
		// db.getDefaultColumnFamily().close();
		// db.close();
	}

	public void toFile() throws IOException {
		emptyBuffer();

	}

	public void putMap(Map<K, Collection<V>> map) {
		map.forEach((k, v) -> {
			this.putAll(k, v);
		});
	}

	public Iterator<Entry<K, Collection<V>>> iterator() {
		emptyBuffer();
		return super.iterator();
	}

}
