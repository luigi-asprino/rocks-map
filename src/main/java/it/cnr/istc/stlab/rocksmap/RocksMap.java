package it.cnr.istc.stlab.rocksmap;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.rocksdb.RocksDBException;

import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class RocksMap<K, V> extends RocksDBWrapper<K, V> implements Map<K, V>, Closeable {

	public RocksMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer)
			throws RocksDBException {
		super(rocksDBPath, keyTransformer, valueTransformer);
	}

	public RocksMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer,
			boolean enableCompression) throws RocksDBException {
		super(rocksDBPath, keyTransformer, valueTransformer, enableCompression);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		V result = null;
		try {
			logger.debug("get {}", ((K) key).toString());
			byte[] r = db.get(keyTransformer.transform((K) key));
			if (r == null)
				return null;
			result = valueTransformer.transform(r);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public V put(K key, V value) {
		V old_value = null;
		if (containsKey(key)) {
			old_value = get(key);
		}
		try {
			logger.debug("put {}", (key).toString(), value.toString());
			db.put(keyTransformer.transform(key), valueTransformer.transform(value));
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return old_value;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		V r = super.removeKey((K) key);
		return r;
	}

	public Iterator<Entry<K, V>> iterator() {
		return super.entryIterator();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
//		m.forEach((k, v) -> {
//			this.put(k, v);
//		});
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<V> values() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEmpty() {
		// TODO
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsValue(Object value) {
		// TODO
		throw new UnsupportedOperationException();
	}

}
