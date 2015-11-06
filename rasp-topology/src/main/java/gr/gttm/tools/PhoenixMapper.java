package gr.gttm.tools;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;




import backtype.storm.tuple.ITuple;

public class PhoenixMapper implements JdbcMapper {
	private static final long serialVersionUID = 5890265642271854061L;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
    public List<Column> getColumns(ITuple tuple) {
        List<Column> columns = new ArrayList<Column>();
        
        /* Table DDL:
		 * CREATE TABLE "netdata" (
		 *     "time" BIGINT PRIMARY KEY,
		 *     "d"."ipS" VARCHAR,
		 *     "d"."ipSI" BIGINT,
		 *     "d"."ipD" VARCHAR,
		 *     "d"."ipDI" BIGINT,
		 *     "d"."proto" SMALLINT,
		 *     "d"."portS" INTEGER,
		 *     "d"."portD" INTEGER,
		 *     "d"."size" INTEGER,
		 *     "as"."asS" VARCHAR,
		 *     "as"."asD" VARCHAR,
		 *     "dns"."dnsS" VARCHAR,
		 *     "dns"."dnsD" VARCHAR
		 * );
		 * 
		 * Data Types Mapping:
		 * BIGINT 	-> Long
		 * VARCHAR 	-> String
		 * SMALLINT -> Short
		 * INTEGER 	-> Integer
		 */
        
        columns.add(new Column("\"time\"", tuple.getLongByField("dateTime"), java.sql.Types.BIGINT));
        
        columns.add(new Column("\"d\".\"ipS\"", tuple.getStringByField("sourceIP"), java.sql.Types.VARCHAR));
        columns.add(new Column("\"d\".\"ipSI\"", tuple.getLongByField("sourceIPInt"), java.sql.Types.BIGINT));
        columns.add(new Column("\"d\".\"ipD\"", tuple.getStringByField("destinationIP"), java.sql.Types.VARCHAR));
        columns.add(new Column("\"d\".\"ipDI\"", tuple.getLongByField("destinationIPInt"), java.sql.Types.BIGINT));
        columns.add(new Column("\"d\".\"proto\"", tuple.getShortByField("protocol"), java.sql.Types.SMALLINT));
        columns.add(new Column("\"d\".\"portS\"", tuple.getIntegerByField("sourcePort"), java.sql.Types.INTEGER));
        columns.add(new Column("\"d\".\"portD\"", tuple.getIntegerByField("destinationPort"), java.sql.Types.INTEGER));
        columns.add(new Column("\"d\".\"size\"", tuple.getIntegerByField("ipSize"), java.sql.Types.INTEGER));

        columns.add(new Column("\"as\".\"asS\"", tuple.getStringByField("sourceAS"), java.sql.Types.VARCHAR));
        columns.add(new Column("\"as\".\"asD\"", tuple.getStringByField("destinationAS"), java.sql.Types.VARCHAR));
        
        columns.add(new Column("\"dns\".\"dnsS\"", tuple.getStringByField("sourceDNS"), java.sql.Types.VARCHAR));
        columns.add(new Column("\"dns\".\"dnsD\"", tuple.getStringByField("destinationDNS"), java.sql.Types.VARCHAR));
        
        return columns;
    }
}
