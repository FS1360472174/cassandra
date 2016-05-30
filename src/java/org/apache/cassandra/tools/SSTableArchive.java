package org.apache.cassandra.tools;

import static com.google.common.collect.Iterables.filter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

import com.google.common.base.Predicate;
import com.google.common.io.Files;

public class SSTableArchive {
	private static String sourcepath;
	private static String destpath;
	/**
	 * SSTableArchive 
	 * arg[0]=source path
	 * arg[1]=dest path
	 * @param args
	 */
	public static void main(String[] args) {
		 parseArgs(args);
		 String keyspaceName=sourcepath.substring(sourcepath.indexOf("/", -2),sourcepath.lastIndexOf("/"));
	     Keyspace keyspace = Keyspace.open(keyspaceName);
	     String cfName=sourcepath.substring(sourcepath.lastIndexOf("/"),sourcepath.length()-1).split("-")[0];
	     ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
		 List<SSTableReader>sstables=new ArrayList<>(cfs.getLiveSSTables());
		 //filter sstable in directory
		 long maxAge=100;
		 long now =1;
		 Iterable<SSTableReader>sstable= filterOldSSTables(sstables, maxAge, now);
		 
		//move
		 while(sstable.iterator().hasNext()){
			 sstable.iterator().next().moveFiles(destpath);
			 sstable.iterator().next().createSoftLinks(destpath);
		 }
	}
	private static void parseArgs(String[] args){
		sourcepath=args[0];
		File source=new File(sourcepath);
		if(!source.exists()){
			errorMsg("Unknown directory: " + sourcepath);
		}
		if(!source.isDirectory()){
			errorMsg(sourcepath+"is not a directory");
		}
		
		destpath=args[1];
		File dest=new File(destpath);
		if(!dest.exists()){
			errorMsg("Unknown directory: " + destpath);
		}
		if(!dest.isDirectory()){
			errorMsg(destpath+"is not a directory");
		}
	}
	private static Iterable<SSTableReader> filterOldSSTables(List<SSTableReader> sstables, long maxAge, long now) {
		if (maxAge == 0)
			return sstables;
		final long period = now - maxAge;
		Iterable<SSTableReader> oldSSTable = filter(sstables, new Predicate<SSTableReader>() {
			@Override
			public boolean apply(SSTableReader sstable) {
				return sstable.getMaxTimestamp() < period;
			}

		});
		return oldSSTable;
	}

	private static void errorMsg(String msg)
    {
        System.err.println(msg);
        System.exit(1);
    }
}
