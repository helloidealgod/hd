import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.common.IOUtils;

public class HdfsUtil {

	public static String uri = "hdfs://master:9000";
	public static Logger logger = Logger.getLogger(HdfsUtil.class);
	 
	/**
	 * make a new dir in the hdfs
	 * 
	 * @param dir the dir may like '/tmp/testdir'
	 * @return boolean true-success, false-failed
	 * @exception IOException something wrong happends when operating files
	 */
	public static boolean mkdir(String dir) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return false;
		}
		dir = uri + dir;
		Configuration conf = new Configuration();
		conf.set("dfs.client.use.datanode.hostname", "true");
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		if (!fs.exists(new Path(dir))) {
			fs.mkdirs(new Path(dir));
		}
		fs.close();
		return true;
	}
 
	/**
	 * delete a dir in the hdfs.
	 * if dir not exists, it will throw FileNotFoundException
	 * 
	 * @param dir the dir may like '/tmp/testdir'
	 * @return boolean true-success, false-failed
	 * @exception IOException something wrong happends when operating files
	 * 
	 */
	public static boolean deleteDir(String dir) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return false;
		}
		dir = uri + dir;
		Configuration conf = new Configuration();
		conf.set("dfs.client.use.datanode.hostname", "true");
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		fs.delete(new Path(dir), true);
		fs.close();
		return true;
	}
 
	/**
	 * list files/directories/links names under a directory, not include embed
	 * objects
	 * 
	 * @param dir a folder path may like '/tmp/testdir'
	 * @return List<String> list of file names
	 * @throws IOException file io exception
	 */
	public static List<String> listAll(String dir) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return new ArrayList<String>();
		}
		dir = uri + dir;
		Configuration conf = new Configuration();
		conf.set("dfs.client.use.datanode.hostname", "true");
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		FileStatus[] stats = fs.listStatus(new Path(dir));
		List<String> names = new ArrayList<String>();
		for (int i = 0; i < stats.length; ++i) {
			if (stats[i].isFile()) {
				// regular file
				names.add(stats[i].getPath().toString());
			} else if (stats[i].isDirectory()) {
				// dir
				names.add(stats[i].getPath().toString());
			} else if (stats[i].isSymlink()) {
				// is s symlink in linux
				names.add(stats[i].getPath().toString());
			}
		}
 
		fs.close();
		return names;
	}
 
	/*
	 * upload the local file to the hds, 
	 * notice that the path is full like /tmp/test.txt
	 * if local file not exists, it will throw a FileNotFoundException
	 * 
	 * @param localFile local file path, may like F:/test.txt or /usr/local/test.txt
	 * 
	 * @param hdfsFile hdfs file path, may like /tmp/dir
	 * @return boolean true-success, false-failed
	 * 
	 * @throws IOException file io exception
	 */
	public static boolean uploadLocalFile2HDFS(String localFile, String hdfsFile) throws IOException {
		if (StringUtils.isBlank(localFile) || StringUtils.isBlank(hdfsFile)) {
			return false;
		}
		hdfsFile = uri + hdfsFile;
		Configuration conf = new Configuration();
		conf.set("dfs.client.use.datanode.hostname", "true");
		FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
		Path src = new Path(localFile);
		Path dst = new Path(hdfsFile);
		hdfs.copyFromLocalFile(src, dst);
		hdfs.close();
		return true;
	}
 
	/*
	 * create a new file in the hdfs.
	 * 
	 * notice that the toCreateFilePath is the full path
	 * 
	 * and write the content to the hdfs file.
	 */
	/**
	 * create a new file in the hdfs.
	 * if dir not exists, it will create one
	 * 
	 * @param newFile new file path, a full path name, may like '/tmp/test.txt'
	 * @param content file content
	 * @return boolean true-success, false-failed
	 * @throws IOException file io exception
	 */
	public static boolean createNewHDFSFile(String newFile, String content) throws IOException {
		if (StringUtils.isBlank(newFile) || null == content) {
			return false;
		}
		newFile = uri + newFile;
		Configuration conf = new Configuration();
		conf.set("dfs.client.use.datanode.hostname", "true");
		FileSystem hdfs = FileSystem.get(URI.create(newFile), conf);
		FSDataOutputStream os = hdfs.create(new Path(newFile));
		os.write(content.getBytes("UTF-8"));
		os.close();
		hdfs.close();
		return true;
	}
 
	/**
	 * delete the hdfs file
	 * 
	 * @param hdfsFile a full path name, may like '/tmp/test.txt'
	 * @return boolean true-success, false-failed
	 * @throws IOException file io exception
	 */
	public static boolean deleteHDFSFile(String hdfsFile) throws IOException {
		if (StringUtils.isBlank(hdfsFile)) {
			return false;
		}
		hdfsFile = uri + hdfsFile;
		Configuration conf = new Configuration();
		conf.set("dfs.client.use.datanode.hostname", "true");
		FileSystem hdfs = FileSystem.get(URI.create(hdfsFile), conf);
		Path path = new Path(hdfsFile);
		boolean isDeleted = hdfs.delete(path, true);
		hdfs.close();
		return isDeleted;
	}
 
	/**
	 * read the hdfs file content
	 * 
	 * @param hdfsFile a full path name, may like '/tmp/test.txt'
	 * @return byte[] file content
	 * @throws IOException file io exception
	 */
	public static byte[] readHDFSFile(String hdfsFile) throws Exception {
		if (StringUtils.isBlank(hdfsFile)) {
			return null;
		}
		hdfsFile = uri + hdfsFile;
		Configuration conf = new Configuration();
		conf.set("dfs.client.use.datanode.hostname", "true");
		FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
		// check if the file exists
		Path path = new Path(hdfsFile);
		if (fs.exists(path)) {
			FSDataInputStream is = fs.open(path);
			// get the file info to create the buffer
			FileStatus stat = fs.getFileStatus(path);
			// create the buffer
			byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
			is.readFully(0, buffer);
			is.close();
			fs.close();
			return buffer;
		} else {
			throw new Exception("the file is not found .");
		}
	}
 
	/**
	 * append something to file dst
	 * 
	 * @param hdfsFile a full path name, may like '/tmp/test.txt'
	 * @param content string
	 * @return boolean true-success, false-failed
	 * @throws Exception something wrong
	 */
	public static boolean append(String hdfsFile, String content) throws Exception {
		if (StringUtils.isBlank(hdfsFile)) {
			return false;
		}
		if(StringUtils.isEmpty(content)){
			return true;
		}
 
		hdfsFile = uri + hdfsFile;
		Configuration conf = new Configuration();
		// solve the problem when appending at single datanode hadoop env  
		conf.set("dfs.client.use.datanode.hostname", "true");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
		FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
		// check if the file exists
		Path path = new Path(hdfsFile);
		if (fs.exists(path)) {
			try {
				InputStream in = new ByteArrayInputStream(content.getBytes());
				OutputStream out = fs.append(new Path(hdfsFile));
				IOUtils.copyBytes(in, out, 4096, true);
				out.close();
				in.close();
				fs.close();
			} catch (Exception ex) {
				fs.close();
				throw ex;
			}
		} else {
			HdfsUtil.createNewHDFSFile(hdfsFile, content);
		}
		return true;
	}
	
	/**
	 * 从HDFS写文件到本地
	 * @param hdfsFile
	 * @param outputPath
	 */
	public static void writeHDFS2LocalFile(String hdfsFile, String outputPath) {
		if (StringUtils.isBlank(hdfsFile)) {
			return;
		}
		if(StringUtils.isEmpty(outputPath)) {
			return;
		}
		int index = outputPath.lastIndexOf("/");
		if(index == -1) {
			index = outputPath.lastIndexOf("\\");
		}
		String dir = outputPath.substring(0, index);
		File dirFile = new File(dir);
		if(!dirFile.exists()) {
			dirFile.mkdirs();
		}
		File newFile = new File(outputPath);
		if(newFile.exists()) {
			logger.info(outputPath + "已经存在...");
			newFile.delete();
			logger.info("生在删除文件" + outputPath);
		}
		try {
			newFile.createNewFile();
			logger.info("正在创建文件" + outputPath);
		} catch (IOException e2) {
			logger.error(outputPath + "文件创建失败...", e2);
		}
		OutputStream out = null;
		try {
			out = new FileOutputStream(newFile);
			byte[] bs = HdfsUtil.readHDFSFile(hdfsFile);
			out.write(bs);
			out.flush();
			out.close();
			logger.info(outputPath + "文件写入本地成功...");
		} catch (FileNotFoundException e1) {
			logger.error(outputPath + "文件不存在...", e1);
		} catch (Exception e) {
			logger.error(outputPath + "文件写入本地失败...", e);
		}
	}
}
