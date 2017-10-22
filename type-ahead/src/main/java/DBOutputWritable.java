import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;
public class DBOutputWritable implements DBWritable {

	private String prefix;
	private String suffix;
	private int count;
	
	public DBOutputWritable(String prefix, String suffix, 
    int count) {
		this.prefix = prefix;
		this.suffix = suffix;
		this.count= count;
	}

	public void readFields(ResultSet arg0) 
    throws SQLException {
    prefix = arg0.getString(1);
    suffix = arg0.getString(2);
    count = arg0.getInt(3);
	}

	public void write(PreparedStatement arg0) 
    throws SQLException {
    arg0.setString(1, prefix);
    arg0.setString(2, suffix);
    arg0.setInt(3, count);
	}
}
