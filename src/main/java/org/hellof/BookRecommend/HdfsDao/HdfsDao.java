package org.hellof.BookRecommend.HdfsDao;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDao {
    private static final String HDFS = "hdfs://localhost:8020/";

    public HdfsDao(String hdfs, Configuration conf){
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    public HdfsDao(Configuration conf){
        this(HDFS, conf);
    }

    private String hdfsPath;
    private Configuration conf;

    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        HdfsDao hdfs = new HdfsDao(conf);
        hdfs.copyFile("BookData.csv", "/tmp/new");
        hdfs.ls("/tmp/new");
    }

    //创建文件
    public void mkdir(String folder) throws IOException{
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)){
            fs.mkdirs(path);
            System.out.println("Create:" + folder);
        }
        fs.close();
    }
    //删除文件
    public void rmr(String folder) throws IOException{
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete:" + folder);
        fs.close();
    }
    //查看文件详细信息
    public void ls(String folder) throws IOException{
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        
        FileStatus fileStatus = fs.getFileLinkStatus(path);//获取状态
        long blockSize = fileStatus.getBlockSize();//获取数据快大小
        long fileSize = fileStatus.getLen();//获取文件大小
        String fileOwner = fileStatus.getOwner();//获取文件拥有者
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
        long accessTime = fileStatus.getAccessTime();//获取最近访问时间
        long modifyTime = fileStatus.getModificationTime();//获取最后访问时间
        System.out.println("ls:" + folder);
        System.out.println("=====================================================");
        System.out.printf("blockSize: " + blockSize + ",fileSize: " + fileSize + ",fileOwner: " + fileOwner + ",accessTime: " +  simpleDateFormat.format(new Date(accessTime)) + ",modifyTime: " + simpleDateFormat.format(new Date(modifyTime)));
        System.out.println();
        System.out.println("=====================================================");
        fs.close();
    }
    //创建文件
    public void createFile(String file, String content) throws IOException{
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create:" + file);
        } catch(Error error){
        	
        }
    }
    //复制文件
    public void copyFile(String local, String remote) throws IOException{
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }
    //下载文件
    public void download(String remote, String local) throws IOException{
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + "to" + local);
        fs.close();
    }
    //查看文件信息
    public void cat(String remoteFile) throws IOException{
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat:" + remoteFile);
        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }
}
