CREATE TABLE option_data(
	timestamp INT,
	ltp Float,
	volume INT,
	Instrument VARCHAR(255),
	Expiry VARCHAR(255)
)

CREATE TABLE option_data(
	timestamp INT,
	ltp Float,
	volume INT
)

INTO statement

UPDATE table SET col1 = 0 WHERE col1 IS NULL;
ALTER TABLE provider ADD PRIMARY KEY(person,place,thing);
ALTER TABLE provider DROP PRIMARY KEY, ADD PRIMARY KEY(person, place, thing);

CREATE TABLE option_data(
date_time TIMESTAMP, 
instrument VARCHAR ( 50 ),
ltp REAL,
option VARCHAR ( 50 ),
strike REAL,
expiry TIMESTAMP,
delta REAL,
theta REAL,
vega REAL,
gamma REAL
);

CREATE TABLE zerodha_data(
date_time TIMESTAMP, 
instrument_token VARCHAR ( 50 ),
ltp REAL
);

CREATE TABLE fando_test(
	date_time TIMESTAMP, 
	instrument_token VARCHAR ( 50 ),
	ltp REAL,
	volume REAL,
	ohlc json,
	open_interest float
);

CREATE TABLE index_data(
	date_time TIMESTAMP, 
	instrument_token VARCHAR ( 50 ),
	ltp REAL,
	ohlc json
);
CREATE TABLE test1(
new json
);

select t1.instrument_token, t1.ltp, t1.volume, t1.ohlc, t1.open_interest, t2.name, t2.tradingsymbol,
t2.instrument_type, t2.strike,t2.expiry
INTO test_combine 
FROM test_data2 as t1 
LEFT JOIN instrument_list as 
t2 ON CAST(t1.instrument_token as INTEGER) = (t2.instrument_token);

select * from option_data where date_time::time = time '05:30:00';

select distinct(tradingsymbol) as tsym,
 avg(ltp) as avg_ltp from 
 (Select t1.tradingsymbol,t2.instrument_token,t2.ltp from 
 instruments_list_today as t1 
 right join stockdata_test as t2 
 ON CAST(t2.instrument_token as INTEGER) = (t1.instrument_token)) 
 as derived_table GROUP BY tsym;

SELECT strike, avg(delta), avg(theta), avg(gamma), avg(vega) from option_data where option ='CE' and date_time::time <= time '13:58:00' and date_time::time >= time '13:53:00' group by strike order by strike;