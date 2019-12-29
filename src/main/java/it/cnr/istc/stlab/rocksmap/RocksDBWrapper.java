package it.cnr.istc.stlab.rocksmap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.rocksdb.CompactionPriority;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;
import it.cnr.istc.stlab.rocksmap.transformer.RocksTransformer;

public abstract class RocksDBWrapper<K, V> {

	static {
		RocksDB.loadLibrary();

	}

	protected static Logger logger = LoggerFactory.getLogger(RocksMultiMap.class);

	protected RocksDB db;
	protected RocksTransformer<K> keyTransformer;
	protected RocksTransformer<V> valueTransformer;
	protected String rocksDBPath;
	private static LRUCache c = new LRUCache(2 * SizeUnit.GB);

	protected static final char SEPARATOR_DUMP = '\t';

	public RocksDBWrapper(String rocksDBPath, RocksTransformer<K> keyTransformer, RocksTransformer<V> valueTransformer)
			throws RocksDBException {

		this.keyTransformer = keyTransformer;
		this.valueTransformer = valueTransformer;
		this.rocksDBPath = rocksDBPath;
		File f = new File(rocksDBPath);
		Options options = new Options();
		options.setCreateIfMissing(true).setWriteBufferSize(512 * SizeUnit.MB).setMaxWriteBufferNumber(4)
				.setIncreaseParallelism(16).setRowCache(c).setMaxBackgroundCompactions(4)
				.setLevelCompactionDynamicLevelBytes(true).setMaxBackgroundFlushes(2).setBytesPerSync(1048576)
				.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
		f.mkdirs();
		db = RocksDB.open(options, rocksDBPath);
	}

	public String getRocksDBPath() {
		return this.rocksDBPath;
	}

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

	public void clear() {
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		byte[] first = ri.key();
		ri.seekToLast();
		byte[] last = ri.key();
		try {
			db.deleteRange(first, last);
			db.delete(last);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
		ri.close();
	}

	public Set<K> keySet() {
		Set<K> keys = new HashSet<>();
		RocksIterator ri = db.newIterator();
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			keys.add(keyTransformer.transform(key));
			ri.next();
		}
		ri.close();
		return keys;
	}

	public void toFile() throws IOException {
		logger.info("Dumping " + rocksDBPath + "/dump.tsv");
		CSVWriter csvw = new CSVWriter(new FileWriter(new File(rocksDBPath + "/dump.tsv")), SEPARATOR_DUMP,
				CSVWriter.NO_QUOTE_CHARACTER);
		logger.debug("Creating Iterator");
		RocksIterator ri = db.newIterator();
		logger.debug("Iterator created");
		ri.seekToFirst();
		while (ri.isValid()) {
			byte[] key = ri.key();
			byte[] val = ri.value();
			csvw.writeNext(new String[] { new String(key), new String(val) });
			ri.next();
		}
		csvw.close();
		ri.close();
		logger.info("Dumped");
	}

	public void close() {
		logger.info("Closing {}", this.rocksDBPath);
		db.close();
	}

}