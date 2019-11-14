package it.cnr.istc.stlab.rocksmap;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public class RocksMap<K, V> implements Map<K, V>, Closeable {

	private static Logger logger = LoggerFactory.getLogger(RocksMap.class);
	private RocksDB db;
	private RocksTransformer<K> keyTransformer;
	private RocksTransformer<V> valueTransformer;
	private String rocksDBPath;

	private static final char SEPARATOR_DUMP = '\t';

	public RocksMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer)
			throws RocksDBException {
		RocksDB.loadLibrary();
		this.keyTransformer = keyTransformer;
		this.valueTransformer = valueTransformer;
		this.rocksDBPath = rocksDBPath;
		File f = new File(rocksDBPath);
		Options options = new Options();
		options.setCreateIfMissing(true);
		options.setIncreaseParallelism(8);
		f.mkdirs();
		db = RocksDB.open(options, rocksDBPath);
	}

	public RocksMap(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer,
			String dumpFile) throws RocksDBException, IOException {
		RocksDB.loadLibrary();
		this.keyTransformer = keyTransformer;
		this.valueTransformer = valueTransformer;
		this.rocksDBPath = rocksDBPath;
		File f = new File(rocksDBPath);
		if (f.exists()) {
			Options options = new Options();
			options.setCreateIfMissing(true);
			new File(rocksDBPath).mkdirs();
			db = RocksDB.open(options, rocksDBPath);
			logger.info("DB exists appending dump!");
		} else {
			db = RocksDB.open(rocksDBPath);
		}
		populateRocksDBFromDump(dumpFile);
	}

	private void populateRocksDBFromDump(String dumpFile) throws IOException, RocksDBException {
		logger.info("Appending Dump to DB");
		CSVReader csvr = new CSVReader(new FileReader(new File(dumpFile)), SEPARATOR_DUMP,
				CSVWriter.NO_QUOTE_CHARACTER);
		String[] line;
		while ((line = csvr.readNext()) != null) {
			db.put(line[0].getBytes(), line[1].getBytes());
		}
		csvr.close();
	}

	public String getRocksDBPath() {
		return this.rocksDBPath;
	}

	@Override
	public int size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEmpty() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsKey(Object key) {
		@SuppressWarnings("unchecked")
		K k = (K) key;
		try {
			byte[] v = db.get(keyTransformer.transform(k));
			return v != null;
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		V result = null;
		try {
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
			db.put(keyTransformer.transform(key), valueTransformer.transform(value));
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		return old_value;
	}

	@Override
	public V remove(Object key) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		m.forEach((k, v) -> {
			this.put(k, v);
		});
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<K> keySet() {
		Set<K> keys = new HashSet<>();
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			keys.add(keyTransformer.transform(key));
			ri.next();
		}
		return keys;
	}

	@Override
	public Collection<V> values() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void toFile(String file, char separator) throws IOException {
		CSVWriter csvw = new CSVWriter(new FileWriter(new File(file)), separator, CSVWriter.NO_QUOTE_CHARACTER);
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			byte[] val = ri.value();
			csvw.writeNext(new String[] { new String(key), new String(val) });
			ri.next();
		}
		csvw.close();
	}

	public void toFile() throws IOException {
		logger.info("Dumping " + rocksDBPath + "/dump.tsv");
		CSVWriter csvw = new CSVWriter(new FileWriter(new File(rocksDBPath + "/dump.tsv")), SEPARATOR_DUMP,
				CSVWriter.NO_QUOTE_CHARACTER);
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			byte[] val = ri.value();
			csvw.writeNext(new String[] { new String(key), new String(val) });
			ri.next();
		}
		csvw.close();
	}

	public void close() {
		logger.info("Closing {}",this.rocksDBPath);
//		db.close();
	}

	public Iterator<Entry<K, V>> iterator() {
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		Iterator<Entry<K, V>> result = new Iterator<Map.Entry<K, V>>() {

			@Override
			public boolean hasNext() {
				return ri.isValid();
			}

			@Override
			public Entry<K, V> next() {
				byte[] val = ri.value();
				byte[] key = ri.key();
				Entry<K, V> entry = new Entry<K, V>() {

					@Override
					public K getKey() {
						return keyTransformer.transform(key);

					}

					@Override
					public V getValue() {
						return valueTransformer.transform(val);
					}

					@Override
					public V setValue(V value) {
						throw new UnsupportedOperationException();
					}
				};
				ri.next();
				return entry;
			}
		};
		return result;
	}

}
