-- drop
soda drop PURCHASE_ORDERS;
drop table purchase_orders purge;

-- create
create table PURCHASE_ORDERS (ID VARCHAR2(255) not null primary key,
                    CREATED_ON timestamp default sys_extract_utc(SYSTIMESTAMP) not null,
                    LAST_MODIFIED timestamp default sys_extract_utc(SYSTIMESTAMP) not null,
                    VERSION varchar2(255) not null,
                    JSON_DOCUMENT BLOB,
                    check (JSON_DOCUMENT is json format oson))
                    LOB(JSON_DOCUMENT) STORE AS (CACHE)
PARTITION BY RANGE (CREATED_ON)
INTERVAL (INTERVAL '10' MINUTE)
SUBPARTITION BY HASH (id) SUBPARTITIONS 32
(
   PARTITION part_01 values LESS THAN (TO_TIMESTAMP('01-JAN-2021','DD-MON-YYYY'))
);

soda create PURCHASE_ORDERS;

-- hash partitioned Primary Key
create index PK_IDX on PURCHASE_ORDERS( id )
    global partition by hash (id )
    partitions 32;

alter table purchase_orders add constraint pk_purchase_orders primary key (ID);

-- empty collection
truncate table PURCHASE_ORDERS;

-- select latest created document
select json_query(json_document,'$') from purchase_orders where created_on = (select max(created_on) from purchase_orders);

-- Indexes
drop index idx_po_requestor;

DECLARE
    collection  SODA_COLLECTION_T;
    spec        VARCHAR2(32000);
    status      NUMBER;
BEGIN
    -- Open the collection
    collection := DBMS_SODA.open_collection('purchase_orders');

    -- Define the index specification
    spec := '{"name"   : "IDX_REQUESTOR",
              "fields" : [{"path"     : "requestor",
                           "datatype" : "string",
                           "order"    : "asc"}]}';
    -- Create the index
    status := collection.create_index(spec);

    DBMS_OUTPUT.put_Line('Status: ' || status);
END;
/

-- creates: create index idx_po_requestor on purchase_orders( json_value( json_document, '$.requestor' ERROR on ERROR NULL ON EMPTY ) );

-- Wildcard search:
select json_serialize(json_document) from purchase_orders p where json_value(json_document, '$.requestor' ) = 'Kiersti Reiman';
soda get purchase_orders -f {"requestor":"Kiersti Reiman"}

select json_serialize(json_document) from purchase_orders p where json_value(json_document, '$.requestor' ERROR on ERROR NULL ON EMPTY ) like 'Kiersti%';
soda get purchase_orders -f {"requestor":{"$like":"Kiersti%"}}

-- no index used here
select json_serialize(json_document) from purchase_orders p where regexp_like(json_value(json_document, '$.requestor' ERROR on ERROR NULL ON EMPTY), 'Kiersti Reima' );


CREATE TABLE "JSONUSER"."COUNTRY_TAXES"
   (	"COUNTRY_NAME" VARCHAR2(500 BYTE) not null primary key,
	"TAX" NUMBER(6,3) DEFAULT 0.05 not null
   )  DEFAULT COLLATION "USING_NLS_COMP" ;



-- Ingest rate analysis
select to_char(created_on,'ss') from purchase_orders;

select extract(minute from created_on) * 100 + round(extract(second from created_on)), count(*) from purchase_orders
group by extract(minute from created_on) * 100 + round(extract(second from created_on))
order by 1;

select min(created_on), max(created_on), count(*) from purchase_orders;
select count(*) from purchase_orders;

-- search index
select * from CTX_PARAMETERS;
exec CTXSYS.CTX_ADM.SET_PARAMETER('default_index_memory','4294967296');

drop index sidx_po;

CREATE SEARCH INDEX sidx_po ON purchase_orders (json_document) FOR JSON
    PARAMETERS('DATAGUIDE ON CHANGE ADD_VC MEMORY 2G SYNC(EVERY "freq=secondly;interval=10" )') PARALLEL;

desc ctxsys.ctx_ddl;

select * from CTX_INDEX_SECTIONS;

soda drop PC_ORDERS;
drop table purchase_orders purge;

-- desc ctxsys.drvdml;
alter session enable parallel dml;
truncate table pc_orders;

-- drop index "sidx_po";
drop index searchidx_po; -- force;

select count(*) from pc_orders p where json_textcontains(p.json_document,'$.requestor','fuzzy(Gya)');

exec CTXSYS.CTX_ADM.SET_PARAMETER('default_index_memory','1073741824');

CREATE SEARCH INDEX searchidx_po ON pc_orders (json_document) FOR JSON
PARAMETERS('Datastore myud filter ctxsys.null_filter Lexer mylex Wordlist MY_STEM_FUZZY_PREF DATAGUIDE OFF MEMORY 128M SYNC(EVERY "freq=secondly;interval=10" MEMORY 128M)');

CREATE SEARCH INDEX sidx_po ON purchase_orders (json_document) FOR JSON
    PARAMETERS('Datastore myud DATAGUIDE OFF MEMORY 4G OPTIMIZE (AUTO_DAILY) SYNC(EVERY "freq=secondly;interval=15" MEMORY 4G PARALLEL 8)') PARALLEL 24;

exec DBMS_SCHEDULER.SET_ATTRIBUTE(NAME => 'DR$SEARCHIDX_PO$J', ATTRIBUTE => 'job_priority', VALUE => 5 );
exec DBMS_SCHEDULER.SET_ATTRIBUTE(NAME => 'DR$SEARCHIDX_PO$J', ATTRIBUTE => 'job_class', VALUE => 'TP' );

select count(*) from purchase_orders;

select * from ctx_user_indexes;

select * from CTX_USER_INDEX_OBJECTS;

exec ctx_ddl.create_shadow_index('sidx_po', 'replace NOPOPULATE');

select idx_id from ctx_user_indexes
     where idx_name ='SIDX_PO';

exec ctx_ddl.populate_pending('RIO$'||1306);

exec ctx_ddl.drop

exec ctx_ddl.sync_index(idx_name =>'RIO$'||1306, maxtime =>1);

exec ctx_ddl.exchange_shadow_index('SIDX_PO');

select * from user_scheduler_jobs;
-- job_priority           = 3 -> 5
-- job_class              = defaul_job_class -> TP
-- job_action             = ctxsys.drvdml.auto_sync_index('"SIDX_PO"', 2147483648, NULL,  24, NULL, 0);
-- resoure_consumer_group = OTHER_GROUPS

--select * from user_scheduler_job_args;
select systimestamp, log_date, status, to_char(run_duration), to_char(cpu_used) from user_scheduler_job_run_details order by 2 desc;
desc user_scheduler_running_jobs;
select job_name, session_id, resource_consumer_group, to_char(elapsed_time) from user_scheduler_running_jobs u;


exec DBMS_SCHEDULER.drop_JOB (job_name => 'DR$SIDX_PO$J',force=>true);
exec DBMS_SCHEDULER.stop_JOB (job_name => 'DR$SEARCHIDX_PO$J',force=>true);


select count(*) from purchase_orders subpartition (SYS_SUBP21651);

select * from USER_TAB_PARTITIONS where table_name = 'PURCHASE_ORDERS';

select * from USER_TAB_SUBPARTITIONS where table_name = 'PURCHASE_ORDERS' and partition_name != 'PART_01' and partition_position= 4;


/*soda count purchase_orders;

delete from purchase_orders where rownum <= 58200;
*/


select * from all_SCHEDULER_JOB_CLASSES;

select * from v$session_longops where time_remaining > 0;

SELECT * FROM dba_rsrc_consumer_group_privs;
SELECT plan,status,comments FROM dba_rsrc_;

select * from v$sql;


select count(*) from dr$sidx_po$U;

create table PC_ORDERS (ID VARCHAR2(255) not null primary key,
                    CREATED_ON timestamp default sys_extract_utc(SYSTIMESTAMP) not null,
                    LAST_MODIFIED timestamp default sys_extract_utc(SYSTIMESTAMP) not null,
                    VERSION varchar2(255) not null,
                    JSON_DOCUMENT BLOB,
                    check (JSON_DOCUMENT is json format oson))
                    LOB(JSON_DOCUMENT) STORE AS (CACHE)
PARTITION BY RANGE (CREATED_ON)
INTERVAL (INTERVAL '10' MINUTE)
SUBPARTITION BY HASH (id) SUBPARTITIONS 32
(
   PARTITION part_01 values LESS THAN (TO_TIMESTAMP('01-JAN-2021','DD-MON-YYYY'))
);

soda create PC_ORDERS;

drop index sidx_po;

CREATE SEARCH INDEX sidx_po ON purchase_orders (json_document) FOR JSON
    PARAMETERS('DATAGUIDE OFF MEMORY 2G SYNC(MANUAL)');


create index sidx_po on purchase_orders( json_document )
     indextype is ctxsys.context
     parameters ('Datastore myud section group CTXSYS.JSON_SECTION_GROUP asynchronous_update OPTIMIZE (AUTO_DAILY) memory 4096M sync(EVERY "freq=secondly;interval=15" MEMORY 4G PARALLEL 8)')
     parallel 12;

exec DBMS_SCHEDULER.SET_ATTRIBUTE(NAME => 'DR$SIDX_PO$J', ATTRIBUTE => 'job_priority', VALUE => 5 );
exec DBMS_SCHEDULER.SET_ATTRIBUTE(NAME => 'DR$SIDX_PO$J', ATTRIBUTE => 'job_class', VALUE => 'MEDIUM' );


exec ctx_ddl.sync_index('SIDX_PO','256M',NULL,8);


desc ctxsys.drvdml;

exec ctxsys.drvdml.auto_sync_index('"SIDX_PO"', 2147483648, 'PART_01',  24, NULL, 0);

-- https://docs.oracle.com/en/database/oracle/oracle-database/19/ccref/oracle-text-indexing-elements.html#GUID-B7BEA087-C3DE-4E00-B8CC-AEF723D8355E
create or replace procedure myproc(rid in rowid, tlob in out nocopy blob) is
--  l_content varchar2(1024);
begin
      select json_object(key 'requestor' value p.json_document.requestor returning blob format oson) into tlob from purchase_orders p
                  where rowid = rid;
      --loop
      --    l_content := '{"requestor":"' || l_content || '"}';
   	  --    dbms_lob.writeappend(tlob, length(l_content), l_content);
      --end loop;
end;
/

begin
ctx_ddl.drop_preference('myud');
ctx_ddl.create_preference('myud', 'user_datastore');
ctx_ddl.set_attribute('myud', 'procedure', 'myproc');
ctx_ddl.set_attribute('myud', 'output_type', 'blob_loc');
end;
/


begin
ctx_ddl.drop_preference('mylex');
ctx_ddl.create_preference('mylex', 'BASIC_LEXER');
ctx_ddl.set_attribute ( 'mylex', 'index_themes', 'NO');
ctx_ddl.set_attribute ( 'mylex', 'index_text', 'YES');
ctx_ddl.set_attribute ( 'mylex', 'prove_themes', 'NO');
ctx_ddl.set_attribute ( 'mylex', 'index_stems', 'NONE');
ctx_ddl.set_attribute ( 'mylex', 'mixed_case', 'NO');
end;
/


begin
ctx_ddl.drop_preference('MY_STEM_FUZZY_PREF');
  ctx_ddl.create_preference('MY_STEM_FUZZY_PREF', 'BASIC_WORDLIST');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','FUZZY_MATCH','ENGLISH');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','FUZZY_SCORE','40');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','FUZZY_NUMRESULTS','100');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','SUBSTRING_INDEX','FALSE');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','PREFIX_INDEX','FALSE');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','REVERSE_INDEX','FALSE');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','WILDCARD_INDEX','TRUE');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','WILDCARD_INDEX_K','3');
  ctx_ddl.set_attribute('MY_STEM_FUZZY_PREF','STEMMER','NULL');
end;
/

select json_object(key 'requestor' value p.json_document.requestor returning blob format oson) from purchase_orders p;


