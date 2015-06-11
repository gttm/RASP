package gr.gttm.tools;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;


import backtype.storm.tuple.ITuple;

public class PhoenixMapper implements JdbcMapper {
    @Override
    public List<Column> getColumns(ITuple tuple) {
        List<Column> columns = new ArrayList<Column>();
        
        columns.add(new Column("\"ip\"", tuple.getStringByField("ip"), java.sql.Types.VARCHAR));
        columns.add(new Column("\"cf1\".\"dns\"", tuple.getStringByField("dns"), java.sql.Types.VARCHAR));
        columns.add(new Column("\"cf2\".\"latency\"", tuple.getIntegerByField("latency"), java.sql.Types.INTEGER));

        return columns;
    }
}
