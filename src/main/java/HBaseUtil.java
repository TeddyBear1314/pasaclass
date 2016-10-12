import com.sun.org.apache.xpath.internal.SourceTree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by dell on 2016/9/28.
 */
public class HBaseUtil {
    private Configuration conf = HBaseConfiguration.create();
    private volatile Connection conn = null;
    private Admin admin;
    private Connection getConn() throws IOException {
        if (this.conn == null) {
            this.conn = ConnectionFactory.createConnection(this.conf);
        }
        return this.conn;
    }

    private Admin getAdmin() throws IOException {
        if (this.admin == null) {
            this.admin = getConn().getAdmin();
        }
        return this.admin;
    }

    public void createTable(String tableName, String... cfs) throws IOException {
        deleteTable(tableName);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf:cfs) {
            htd.addFamily(new HColumnDescriptor(cf));
        }
        getAdmin().createTable(htd);
        System.out.println("table " + tableName +" successfully created!");
    }
    public void putIntoTable(String tableName, String rowKey, String cf, String cq, String value) throws IOException {//因为put同时兼顾insert和update
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(value));
        Table table = getConn().getTable(TableName.valueOf(tableName));
        table.put(put);
        System.out.println("successfully put a row " + rowKey);
        table.close();
    }

    public void getFromTable(String tableName, String rowKey, String cf, String cq) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq));
        Table table = getConn().getTable(TableName.valueOf(tableName));
        Result res = table.get(get);
        byte[] value = res.getValue(Bytes.toBytes(cf), Bytes.toBytes(cq));
        System.out.println("successfully get a value :" + new String(value));
        table.close();
    }

    public void deleteFromTable(String tableName, String rowKey, String cf, String cq) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq));
        Table table = getConn().getTable(TableName.valueOf(tableName));
        table.delete(delete);
        System.out.println("successfully delete a row " + rowKey);
        table.close();
    }

    public void scanTable(String tableName) throws IOException {
        Table table = getConn().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        System.out.println("scan results:");
        for(Result r:rs)
        {
            byte [] bs = r.getValue("cf1".getBytes(),"cq1".getBytes());
            if(bs != null)
                System.out.println(new String(r.getRow()) + ":" + new String(bs));
        }
            table.close();
    }
    public void deleteTable(String tableName) throws IOException {
        if (getAdmin().tableExists(TableName.valueOf(tableName))) {
            getAdmin().disableTable(TableName.valueOf(tableName));
            getAdmin().deleteTable(TableName.valueOf(tableName));
        }
        System.out.println("successfully delete a table " + tableName);
    }

     public void close() throws IOException {
         admin.close();
         conn.close();
     }

    public static void main(String[] args) throws IOException {
        HBaseUtil util = new HBaseUtil();
        util.createTable("t2", "cf1","cf2");
        util.putIntoTable("t2","row1","cf1", "cq1","val1");
        util.getFromTable("t2", "row1", "cf1", "cq1");
        util.scanTable("t2");
        util.deleteFromTable("t2", "row1", "cf1", "cq1");
        util.deleteTable("t2");
        util.close();
    }
}
