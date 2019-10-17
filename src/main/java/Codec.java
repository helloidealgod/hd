import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class Codec {
    public static void main(String[] argv) {
        Configuration conf = new Configuration();
        BZip2Codec codec = new BZip2Codec();
        codec.setConf(conf);

//        String source = argv[0];
        String source = "D://report_data2.txt";
        String destination = "hdfs://node001:8020/copyFromLocal/test.bz2";
        InputStream inputStream;
        try{
            inputStream = new BufferedInputStream(new FileInputStream(source));
            FileSystem fileSystem = FileSystem.get(URI.create(destination),conf);
            OutputStream outputStream = fileSystem.create(new Path(destination));
            CompressionOutputStream compressionOutputStream = codec.createOutputStream(outputStream);
            IOUtils.copyBytes(inputStream,compressionOutputStream,4096,true);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
