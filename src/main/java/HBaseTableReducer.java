import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dell on 2016/9/29.
 */
public class HBaseTableReducer {

   static class ReadTxt extends Mapper<Object,Text,Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String ss[] = s.split(":");
            context.write(new Text(ss[0]), new Text(ss[1]));
        }
    }
    static class HBaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(key.toString()));
            for(Text t :values) {
                put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cq1"), Bytes.toBytes(t.toString()));
                ctx.write(null, put);
            }
        }

    }
}
