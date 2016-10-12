import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by dell on 2016/9/28.
 */
public class HBaseTableMapper {//count the number of the distinct values for a specific line
public static class MyMapper extends TableMapper<Text, IntWritable> {
    private IntWritable ONE = new IntWritable(1);
    private Text text = new Text();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        String cf = context.getConfiguration().get("COLUMN_FAMILY");
        if (cf == null) cf = "CF1";
        String cq = context.getConfiguration().get("COLUMN_QUALIFIER");
        if (cq == null) cq = "CQ1";
        String val = new String(value.getValue(cf.getBytes(), cq.getBytes()));
        text.set(val);
        context.write(text, ONE);
    }
}
    public static class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        private static final byte[] CF = "cf".getBytes();
        private static final byte[] COUNT = "count".getBytes();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(key.toString().getBytes());
            put.addColumn(CF, COUNT, Bytes.toBytes(i));
            context.write(null, put);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();

        String SOURCE_TABLE = args[0];
        conf.set("COLUMN_FAMILY", args[1]);
        conf.set("COLUMN_QUALIFIER", args[2]);
        String TARGET_TABLE = "CountTable";

        Job job = Job.getInstance(conf, "HBaseMapper");
        job.setJarByClass(HBaseTableMapper.class);// class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(SOURCE_TABLE, scan, MyMapper.class, Text.class, IntWritable.class, job);

        TableMapReduceUtil.initTableReducerJob(TARGET_TABLE,MyReducer.class, job);
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true)?0:1);

    }

}
