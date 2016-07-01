package org.apache.cassandra.tools.nodetool;

import static com.google.common.collect.Iterables.filter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Date;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.cassandra.tools.Util;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class SSTableArchive {

	private static String keyspaceName;
	private static String cfName;
	private static String destPath;
	private static int beforeDays;

	/**
	 * SSTableArchive arg[0]=keyspace arg[1]=columnfamily arg[2]=backup path
	 * arg3[] expired days
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		parseArgs(args);

		Util.initDatabaseDescriptor();

		Schema.instance.loadFromDisk(false);

		CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);
		if (metadata == null) {
			throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s", keyspaceName, cfName));
		}
		Keyspace ks = Keyspace.openWithoutSSTables(keyspaceName);
		ColumnFamilyStore cfs = ks.getColumnFamilyStore(cfName);
		Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW)
				.skipTemporary(true);
		Set<SSTableReader> sstables = new HashSet<>();
		for (Map.Entry<Descriptor, Set<Component>> sstable : lister.list().entrySet()) {
			if (sstable.getKey() != null) {
				try {
					SSTableReader reader = SSTableReader.open(sstable.getKey());
					Path path = new File(sstable.getKey().filenameFor(sstable.getValue().iterator().next())).toPath(); 																																																											// file
					if (Files.isSymbolicLink(path)) {
						continue;
					}
					sstables.add(reader);
				} catch (Throwable t) {

				}
			}
		}
		if (sstables.isEmpty()) {

			System.exit(1);
		}
		// filter sstable in directory
		Iterable<SSTableReader> oldSstable = filterOldSSTables(sstables, beforeDays);
		if (Iterables.size(oldSstable) == 0) {
			errorMsg("there is no sstable need to remove");
		}
		System.out.println("there are " + Iterables.size(oldSstable) + " sstable need to remove");
		Iterator<SSTableReader> iter = oldSstable.iterator();
		while (iter.hasNext()) {

			SSTableReader sstable = iter.next();
			System.out.println(sstable.descriptor.baseFilename() + " last update time:"
					+ new Date(sstable.getMaxTimestamp() / 1000));
			System.out.println("move file" + sstable.descriptor.baseFilename() + " to " + destPath);
			sstable.moveFiles(destPath);
			sstable.createSoftLinks(destPath);
		}
		System.exit(0);
	}

	private static void parseArgs(String[] args) {
		if (args.length != 4) {
			errorMsg("Usage:sstablearchive <keyspace> <table> <destination> <expired days>");
		}
		keyspaceName = args[0];
		cfName = args[1];
		destPath = args[2];
		File dest = new File(destPath);
		if (!dest.exists()) {
			errorMsg("Unknown directory: " + destPath);
		}
		if (!dest.isDirectory()) {
			errorMsg(destPath + "is not a directory");
		}
		beforeDays = Integer.parseInt(args[3]);
	}

	private static Iterable<SSTableReader> filterOldSSTables(Set<SSTableReader> sstables, int beforeDays) {
		if (beforeDays == 0) {
			return sstables;
		}
		// *1000 mean transfor ms to microsecond
		final long period = DateUtils.addDays(new Date(), -beforeDays).getTime() * 1000;
		System.out.println("will archive data before date:" + new Date(period));
		Iterable<SSTableReader> oldSSTable = filter(sstables, new Predicate<SSTableReader>() {
			@Override
			public boolean apply(SSTableReader sstable) {
				return sstable.getMaxTimestamp() <= period;
			}

		});
		return oldSSTable;
	}

	private static void errorMsg(String msg) {
		System.err.println(msg);
		System.exit(1);
	}
}
