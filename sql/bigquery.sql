-- CREATE TABLE `zerodha-332309.ticker_data.final_table` AS
-- SELECT tr.timestamp,tr.last_price,tr.volume,tr.oi,tr.open,tr.high,tr.low,tr.close,ir.name,ir.tradingsymbol
-- FROM `zerodha-332309.ticker_data.new_day`as tr 
-- LEFT JOIN `zerodha-332309.ticker_data.instrument_list` as ir
-- ON tr.instrument_token = ir.instrument_token
-- LIMIT 100

DECLARE query_create STRING;
DECLARE query_table STRING;
DECLARE query_append STRING;
DECLARE table_name STRING;
-- DECLARE query STRING;

FOR record IN
(SELECT distinct(name) as name
   FROM `zerodha-332309.ticker_data.final_table`)
DO

	SET table_name = REPLACE(record.name, "&", "and");
	SET query_table = "SELECT COUNT(table_id) AS found FROM `zerodha-332309.final_tables.__TABLES__` WHERE table_id = \""||table_name||"\"";

	IF EXISTS (EXECUTE IMMEDIATE query_table) THEN
		SET table_name = REPLACE(record.name, "&", "and");
		SET query_create = "CREATE TABLE `zerodha-332309.final_tables."||table_name ||"` AS SELECT * FROM `zerodha-332309.ticker_data.final_table` WHERE name = \"" || record.name ||"\"";
		EXECUTE IMMEDIATE query_create;

	ELSE
		SET table_name = REPLACE(record.name, "&", "and");
		SET query_append = "INSERT `zerodha-332309.final_tables."||table_name||"` (timestamp,last_price,volume,oi,open,high,low,close,name,tradingsymbol) SELECT * FROM `zerodha-332309.ticker_data.final_table` WHERE name = \""||record.name||"\"";
		EXECUTE IMMEDIATE query_append;
	END IF;

END FOR;


-- SELECT distinct(name) as name
--    FROM `zerodha-332309.ticker_data.final_table`

  -- CREATE TABLE `zerodha-332309.final_tables.test` AS
  -- SELECT * FROM `zerodha-332309.ticker_data.final_table`
  -- WHERE name = "GSPL"

INSERT `zerodha-332309.final_tables.{table_name}` (timestamp,last_price,volume,oi,open,high,low,close,name,tradingsymbol) 
SELECT
 *
FROM 
  `zerodha-332309.ticker_data.final_table` WHERE name = "AARTIIND"


"INSERT `zerodha-332309.final_tables."||table_name||"` (timestamp,last_price,volume,oi,open,high,low,close,name,tradingsymbol) 
SELECT
 *
FROM 
  `zerodha-332309.ticker_data.final_table` WHERE name = \""||record.name||"\""


-- Creating a cleaned table removing duplicate rows and same timestamp for same instrument
CREATE TABLE `zerodha-332309.ticker_data.final_cleaned_table` AS
SELECT timestamp last_price, volume, oi, open, high ,low, close,name,tradingsymbol FROM(
SELECT *, ROW_NUMBER()  OVER(PARTITION BY tradingsymbol,timestamp ORDER BY timestamp) rnum
 FROM `zerodha-332309.ticker_data.final_table`
order by  tradingsymbol, timestamp)
WHERE rnum = 1



SELECT COUNT(table_id) AS found FROM `zerodha-332309.final_tables.__TABLES__` WHERE table_id = @table_name


-- From the cleaned table to the tables for individual instruments
DECLARE query_create STRING;
DECLARE query_table STRING;
DECLARE query_append STRING;
DECLARE table_name STRING;

FOR record IN
(SELECT distinct(name) as name
   FROM `zerodha-332309.ticker_data.final_cleaned_table`)
DO
	SET table_name = REPLACE(record.name, "&", "and");
	SET query_table = "SELECT COUNT(table_id) AS found FROM `zerodha-332309.final_tables.__TABLES__` WHERE table_id = \""||table_name||"\"";

	IF EXISTS (SELECT COUNT(table_id) AS found FROM `zerodha-332309.final_tables.__TABLES__` WHERE table_id = table_name) THEN
        SET table_name = REPLACE(record.name, "&", "and");
		SET query_append = "INSERT `zerodha-332309.final_tables."||table_name||"` (timestamp,last_price,volume,oi,open,high,low,close,name,tradingsymbol) SELECT * FROM `zerodha-332309.ticker_data.final_cleaned_table` WHERE name = \""||record.name||"\"";
		EXECUTE IMMEDIATE query_append;
		
	ELSE
		SET table_name = REPLACE(record.name, "&", "and");
		SET query_create = "CREATE TABLE `zerodha-332309.final_tables."||table_name ||"` AS SELECT * FROM `zerodha-332309.ticker_data.final_cleaned_table` WHERE name = \"" || record.name ||"\"";
		EXECUTE IMMEDIATE query_create;

	END IF;

END FOR;
