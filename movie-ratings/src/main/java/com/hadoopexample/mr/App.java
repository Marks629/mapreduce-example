package com.hadoopexample.mr;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main(String[] args) throws Exception {
		String uri = "file:/home/hduser/u.data";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		try {
			in = fs.open(new Path(uri));
		 	IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
