package com.gochinatv.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;


public class HdfsFileUtils {

	/**
	 * @param localFile
	 *  example d:\\apache-activemq-5.12.1-bin.zip
	 * @param hdfsFile
	 *  example hdfs://192.168.3.191/zhuhh/apache-activemq-5.12.1-bin.zip
	 */
	public static int upload(String localFile, String hdfsFile) {
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(localFile));
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
			OutputStream out = fs.create(new Path(hdfsFile), new Progressable() {
				int i=1;
				public void progress() {
					System.out.print(i);
					i++;
				}
			});
			IOUtils.copyBytes(in, out, 4096, true);
			IOUtils.closeStream(in);
			return 1;
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		} finally {

		}
	}
	
	/**
     *
     * @param content   上传的内容
     * @param hdfsFile  上传的hdfs文件路径   (hdfs://slave01/user/hdfs/ga_location/20161209.txt)
     */
    public static void upload(String content, String hdfsFile) {
        try {
            Configuration configuration = new Configuration();
            FileSystem hdfs = FileSystem.get(URI.create(hdfsFile), configuration );
            Path file = new Path(hdfsFile);
            if ( hdfs.exists( file )) { hdfs.delete(file, true ); }
            OutputStream out = hdfs.create(file, new Progressable() {
                int i=1;
                public void progress() {
                    //System.out.print(i);
                    i++;
                }
            });

            BufferedWriter br = new BufferedWriter( new OutputStreamWriter(out,"UTF-8"));
            br.write(content);
            br.close();
            hdfs.close();
        } catch (IOException e) {
            System.out.println("*****数据上传至HDFS出错*****");
            e.printStackTrace();
        }
    }
	
	
	/**
	 * Copy FileSystem files to local files.
	 * Configuration conf = new Configuration();
	 * copyFileFromFs(conf,"hdfs://192.168.3.191/zhuhh/apache-activemq-5.12.1-bin.zip",new File("d:\\apache-activemq-5.12.1-bin(1).zip"),false);
	 */
	public static boolean copyFileFromFs(Configuration conf, String src, File dst, boolean deleteSource) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(src),conf);// 初始化文件系统
		Path srcPath = new Path(src);
		/*String fileName = srcPath.getName().substring(srcPath.getName().lastIndexOf("/") + 1);
		File dstFile = new File(dst);
		System.out.println("the file is :" + dstFile.toString());
		if (!fs.exists(srcPath)) {
			System.out.println("检查是否存在");
			return false;
		}*/
		return FileUtil.copy(fs, srcPath, dst, deleteSource, conf);
	}
	
	
	/**
	 * Configuration conf = new Configuration();
	 * deleteFileFromFs(conf,"hdfs://192.168.3.191/hadoop");
	 */
	public static boolean deleteFileFromFs(Configuration conf,String src) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(src),conf);// 初始化文件系统
		Path srcPath = new Path(src);
		return fs.deleteOnExit(srcPath);
	}
	
	/**
	 * Configuration conf = new Configuration();
	 * listFileFromFs(conf,"hdfs://192.168.3.191/zhuhh");
	 */
	public static void listFileFromFs(Configuration conf,String src) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(src),conf);// 初始化文件系统
		Path path = new Path(src);
		FileStatus[] listStatus = fs.listStatus(path);
		for (FileStatus fileStatus : listStatus) {
			String filePath = fileStatus.getPath().toString();
			System.out.println(filePath);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		upload("d:\\worldcount.txt","hdfs://192.168.3.191/zhuhh/worldcount.txt");
		
		/*Configuration conf = new Configuration();
		copyFileFromFs(conf,"hdfs://192.168.3.191/zhuhh/apache-activemq-5.12.1-bin.zip",new File("d:\\bbbb.tar"),false);*/
		
		/*Configuration conf = new Configuration();
		deleteFileFromFs(conf,"hdfs://192.168.3.191/hadoop");*/
		
		/*Configuration conf = new Configuration();
		listFileFromFs(conf,"hdfs://192.168.3.191/zhuhh");*/
		
	}
}
