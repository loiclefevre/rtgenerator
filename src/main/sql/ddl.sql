-- drop
soda drop PURCHASE_ORDERS;
drop table purchase_orders purge;

-- create
-- Warning for on-premises: uses Partitioning Option
create table PURCHASE_ORDERS (
                    ID VARCHAR2(255) default SYS_GUID() not null primary key,
                    CREATED_ON timestamp default sys_extract_utc(SYSTIMESTAMP) not null,
                    LAST_MODIFIED timestamp default sys_extract_utc(SYSTIMESTAMP) not null,
                    VERSION varchar2(255) not null,
                    JSON_DOCUMENT BLOB,
                    check (JSON_DOCUMENT is json format oson))
                    LOB(JSON_DOCUMENT) STORE AS (CACHE)
PARTITION BY RANGE (CREATED_ON)
INTERVAL (INTERVAL '5' MINUTE)
--SUBPARTITION BY HASH (ID) SUBPARTITIONS 64 -- useful starting with 21c
(
   PARTITION part_01 values LESS THAN (TO_TIMESTAMP('01-JAN-2021','DD-MON-YYYY'))
);

soda create PURCHASE_ORDERS;

soda list;
soda count PURCHASE_ORDERS;


-- empty collection
truncate table PURCHASE_ORDERS;

-- select latest created document
select json_query(json_document,'$') from purchase_orders where created_on = (select max(created_on) from purchase_orders);

-- select some JSON document and pretty print
select json_query(p.json_document,'$' returning varchar2(4000) pretty) from purchase_orders p;

-- Single Field Index
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

    IF status = 1 THEN
    	DBMS_OUTPUT.put_Line('Status: OK');
    END IF;
END;
/


DECLARE
    collection  SODA_COLLECTION_T;
    spec        VARCHAR2(32000);
    status      NUMBER;
BEGIN
    -- Open the collection
    collection := DBMS_SODA.open_collection('purchase_orders');

    -- Define the index specification
    spec := '{"name"   : "MY_INDEX_NAME",
              "fields" : [{"path"     : "requestor",
                           "datatype" : "string",
                           "order"    : "asc"},
                           {"path"     : "id",
                            "datatype" : "number",
                            "order"    : "asc"},
                           {"path"     : "address.country",
                            "datatype" : "string",
                            "order"    : "asc"}
                            ]}';
    -- Create the index
    status := collection.create_index(spec);

    IF status = 1 THEN
    	DBMS_OUTPUT.put_Line('Status: OK');
    END IF;
END;
/


-- creates: create index idx_po_requestor on purchase_orders( json_value( json_document, '$.requestor' ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;

-- Wildcard search:
select json_serialize(json_document) from purchase_orders p where json_value(json_document, '$.requestor' ) = 'Kiersti Reiman';
soda get purchase_orders -f {"requestor":"Kiersti Reiman"}

select json_serialize(json_document) from purchase_orders p where json_value(json_document, '$.requestor' ERROR on ERROR NULL ON EMPTY ) like 'Kiersti%';
soda get purchase_orders -f {"requestor":{"$like":"Kiersti%"}}

-- no index used here, search index needed
select json_serialize(json_document) from purchase_orders p where regexp_like(json_value(json_document, '$.requestor' ERROR on ERROR NULL ON EMPTY), 'Kiersti Reima' );

-- Search Index


-- Geo-Spatial Index for GeoJSON
drop index geo_idx;

DECLARE
    collection  SODA_COLLECTION_T;
    spec        VARCHAR2(32000);
    status      NUMBER;
BEGIN
    -- Open the collection
    collection := DBMS_SODA.open_collection('purchase_orders');

    -- Define the index specification
    spec := '{"name"   : "GEO_IDX",
              "spatial" : "shippingInstructions.address.geometry"}';
    -- Create the index
    status := collection.create_index(spec);

    DBMS_OUTPUT.put_Line('Status: ' || status);
END;
/

CREATE INDEX geo_idx
  ON purchase_orders (json_value(json_document, '$.shippingInstructions.address.geometry' RETURNING SDO_GEOMETRY error on error null on empty))
  INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2
  PARAMETERS('layer_gtype=POINT cbtree_index=true')
  LOCAL PARALLEL;

select /*+ FIRST_ROWS(4) */ "JSON_DOCUMENT","ID","LAST_MODIFIED","CREATED_ON","VERSION"
from "JSONUSER"."PURCHASE_ORDERS" where (SDO_WITHIN_DISTANCE(JSON_VALUE("JSON_DOCUMENT", '$.shippingInstructions.address.geometry' returning SDO_GEOMETRY error on error null on empty),
    JSON_VALUE('{"type": "Point", "coordinates": [-122.1,-59.35]}', '$' returning SDO_GEOMETRY error on error), 'distance=0.1 unit=mile' ) = 'TRUE') order by "ID" fetch first 4 rows only;

-- QBE
soda get purchase_orders -f {"shippingInstructions.address.geometry": {"$near": {"$geometry": {"type": "Point", "coordinates": [-122.1,-59.35]},"$distance": 0.1, "$unit": "mile"    }  }}


-- Invoices reporting
drop table invoices purge;

-- TODO: mapping a SODA collection on this?
CREATE TABLE INVOICES (
    id VARCHAR2(255) not null primary key,
    CREATED_ON timestamp default sys_extract_utc(SYSTIMESTAMP) not null,
    price number not null,
    price_with_country_vat number not null,
	country_name VARCHAR2(500 BYTE) not null
)
PARTITION BY RANGE (CREATED_ON)
INTERVAL (INTERVAL '5' MINUTE)
-- SUBPARTITION BY HASH (ID) SUBPARTITIONS 32 -- useful starting with 21c
(
   PARTITION part_01 values LESS THAN (TO_TIMESTAMP('01-JAN-2021','DD-MON-YYYY'))
);

CREATE TABLE COUNTRY_TAXES (
    COUNTRY_NAME VARCHAR2(500 BYTE) not null primary key,
	TAX NUMBER(6,3) DEFAULT 0.05 not null
);

insert into country_taxes (COUNTRY_NAME, TAX) values ('Papua New Guinea', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Malta', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Guam', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Malaysia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Costa Rica', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Djibouti', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Tokelau', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Nepal', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Dominican Republic', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Timor-Leste', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Barbados', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Botswana', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Republic of Korea', 0.1);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Luxembourg', 0.17);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Italy', 0.22);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Dominica', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Slovakia (Slovak Republic)', 0.2);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Russian Federation', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Micronesia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Venezuela', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Saint Martin', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Guadeloupe', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Uzbekistan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Gibraltar', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Marshall Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Taiwan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Turkmenistan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Sierra Leone', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Germany', 0.19);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Liechtenstein', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Qatar', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Heard Island and McDonald Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Yemen', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Solomon Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cote d''Ivoire', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Ghana', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Chad', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Isle of Man', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Ethiopia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Angola', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Western Sahara', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Puerto Rico', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Vietnam', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Mexico', 0.16);
insert into country_taxes (COUNTRY_NAME, TAX) values ('San Marino', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Zimbabwe', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Equatorial Guinea', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('South Georgia and the South Sandwich Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cameroon', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Vanuatu', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cook Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Nicaragua', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Aruba', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Falkland Islands (Malvinas)', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Saint Barthelemy', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Kenya', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Ecuador', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Nauru', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Canada', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Philippines', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Sweden', 0.25);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Niger', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Gabon', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Panama', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Turkey', 0.18);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Montenegro', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Switzerland', 0.077);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bahrain', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Maldives', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Sudan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Seychelles', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Liberia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Libyan Arab Jamahiriya', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Ireland', 0.23);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Georgia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Montserrat', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Faroe Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Finland', 0.24);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Anguilla', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Somalia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Austria', 0.2);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Thailand', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Lithuania', 0.21);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bangladesh', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Palau', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Antarctica (the territory South of 60 deg S)', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Reunion', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Guyana', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bermuda', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('British Indian Ocean Territory (Chagos Archipelago)', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Oman', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Lesotho', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cape Verde', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Guatemala', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Suriname', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Iraq', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Tajikistan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Iceland', 0.24);
insert into country_taxes (COUNTRY_NAME, TAX) values ('United States Minor Outlying Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Romania', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Iran', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Egypt', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Japan', 0.1);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Malawi', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Honduras', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Samoa', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Comoros', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Jamaica', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Denmark', 0.25);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cuba', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Paraguay', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Christmas Island', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Rwanda', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cyprus', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('New Caledonia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Monaco', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Mayotte', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cayman Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Namibia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Burundi', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('New Zealand', 0.15);
insert into country_taxes (COUNTRY_NAME, TAX) values ('China', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Congo', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Andorra', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Albania', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Wallis and Futuna', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Colombia', 0.19);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Kazakhstan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Saint Vincent and the Grenadines', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Saint Pierre and Miquelon', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('India', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Saint Kitts and Nevis', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Uganda', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Poland', 0.23);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cocos (Keeling) Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Lebanon', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Sao Tome and Principe', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Mozambique', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Myanmar', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Guinea', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Saudi Arabia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Israel', 0.17);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Macedonia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Argentina', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Norway', 0.25);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Morocco', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Fiji', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Algeria', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Jersey', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Tonga', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Trinidad and Tobago', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Democratic People''s Republic of Korea', 0.1);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Singapore', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Moldova', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bhutan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Guernsey', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Spain', 0.21);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Lao People''s Democratic Republic', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Peru', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Niue', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Turks and Caicos Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Portugal', 0.23);
insert into country_taxes (COUNTRY_NAME, TAX) values ('France', 0.2);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Pakistan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Afghanistan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Syrian Arab Republic', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Ukraine', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Palestinian Territory', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('El Salvador', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Tanzania', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Kuwait', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('United States of America', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bosnia and Herzegovina', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('American Samoa', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Cambodia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bahamas', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Zambia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Belgium', 0.21);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Benin', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Guinea-Bissau', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Jordan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Svalbard & Jan Mayen Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Burkina Faso', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Armenia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('United Kingdom', 0.2);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bouvet Island (Bouvetoya)', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Senegal', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Hungary', 0.27);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Greece', 0.24);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Sri Lanka', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Norfolk Island', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('United Arab Emirates', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Togo', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Madagascar', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Virgin Islands, U.S.', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Nigeria', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Central African Republic', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Tuvalu', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Netherlands Antilles', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Haiti', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Indonesia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Virgin Islands, British', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Azerbaijan', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Martinique', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Brunei Darussalam', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Brazil', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Macao', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Croatia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Slovenia', 0.22);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Netherlands', 0.21);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Grenada', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Swaziland', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Pitcairn Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('French Southern Territories', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Tunisia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Czech Republic', 0.21);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Northern Mariana Islands', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Saint Helena', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Greenland', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Chile', 0.19);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Latvia', 0.21);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Belize', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Mali', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Gambia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Saint Lucia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Hong Kong', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Estonia', 0.2);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Kiribati', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bolivia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Belarus', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Uruguay', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('French Guiana', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('South Africa', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Kyrgyz Republic', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('French Polynesia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Bulgaria', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Holy See (Vatican City State)', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Mauritania', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Australia', 0.1);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Eritrea', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Antigua and Barbuda', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Mauritius', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Mongolia', 0.05);
insert into country_taxes (COUNTRY_NAME, TAX) values ('Serbia', 0.05);
commit;

-- Ingest rate analysis
select to_char(created_on,'ss') from purchase_orders;

select extract(minute from created_on) * 100 + round(extract(second from created_on)), count(*) from purchase_orders
group by extract(minute from created_on) * 100 + round(extract(second from created_on))
order by 1;

select /*+ parallel (8) */
trunc( (select count (*) from purchase_orders) /
(select extract( day from diff )*24*60*60  +
           extract( hour from diff )*60*60  +
           extract( minute from diff )*60  +
           round(extract( second from diff ) ) total_seconds
      from (select max (created_on) - min(created_on) diff
             from purchase_orders))
) avg from dual ;

select min(created_on), max(created_on), count(*) from purchase_orders;
select count(*) from purchase_orders;

-- JSON Search index
DECLARE
    collection  SODA_COLLECTION_T;
    status      NUMBER;
BEGIN
    -- Open the collection
    collection := DBMS_SODA.open_collection('purchase_orders');

    -- Define the index specification
    spec := '{"name"   : "SIDX_PO"}';
    -- Create the index
    status := collection.create_index(spec);

    DBMS_OUTPUT.put_Line('Status: ' || status);
END;
/

select * from CTX_PARAMETERS;

exec CTXSYS.CTX_ADM.SET_PARAMETER('default_index_memory','4294967296'); -- 4 GB
-- exec CTXSYS.CTX_ADM.SET_PARAMETER('default_index_memory','2147483648'); -- 2 GB

drop index sidx_po force;

-- https://docs.oracle.com/en/database/oracle/oracle-database/19/ccref/oracle-text-indexing-elements.html#GUID-B7BEA087-C3DE-4E00-B8CC-AEF723D8355E
create or replace procedure myproc(rid in rowid, tlob in out nocopy blob) is
begin
      select json_object(key 'requestor' value p.json_document.requestor returning blob format oson) into tlob
        from purchase_orders p
       where rowid = rid;
end;
/

begin
  begin
    ctx_ddl.drop_preference('my_lexer_pref');
  exception when others then null;
  end;
  ctx_ddl.create_preference('my_lexer_pref', 'BASIC_LEXER');
  ctx_ddl.set_attribute('my_lexer_pref', 'index_themes', 'NO');
  ctx_ddl.set_attribute('my_lexer_pref', 'index_text', 'YES');
  ctx_ddl.set_attribute('my_lexer_pref', 'prove_themes', 'NO');
  ctx_ddl.set_attribute('my_lexer_pref', 'index_stems', 'NONE');
  ctx_ddl.set_attribute('my_lexer_pref', 'mixed_case', 'NO');

  begin
    ctx_ddl.drop_preference('my_stem_fuzzy_pref');
  exception when others then null;
  end;
  ctx_ddl.create_preference('my_stem_fuzzy_pref', 'BASIC_WORDLIST');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','FUZZY_MATCH','ENGLISH');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','FUZZY_SCORE','40');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','FUZZY_NUMRESULTS','100');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','SUBSTRING_INDEX','FALSE');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','PREFIX_INDEX','FALSE');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','REVERSE_INDEX','FALSE');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','WILDCARD_INDEX','TRUE');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','WILDCARD_INDEX_K','3');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','WILDCARD_MAXTERMS','10000');
  ctx_ddl.set_attribute('my_stem_fuzzy_pref','STEMMER','NULL');

  begin
    ctx_ddl.drop_preference('my_storage_pref');
  exception when others then null;
  end;
  ctx_ddl.create_preference('my_storage_pref', 'BASIC_STORAGE');
  ctx_ddl.set_attribute('my_storage_pref', 'stage_itab', 'YES');
  ctx_ddl.set_attribute('my_storage_pref', 'stage_itab_parallel', '4');
  ctx_ddl.set_attribute('my_storage_pref', 'stage_itab_max_rows', '100000');
  ctx_ddl.set_attribute('my_storage_pref', 'stage_itab_auto_opt', 'TRUE');

  begin
    ctx_ddl.drop_section_group('my_sec_group_pref');
  exception when others then null;
  end;
    ctx_ddl.create_section_group('my_sec_group_pref', 'PATH_SECTION_GROUP');
    ctx_ddl.set_sec_grp_attr('my_sec_group_pref', 'json_enable', 'T');
    -- ctx_ddl.add_sdata_section('my_sec_group_pref', 'requestor', 'requestor', 'Varchar2');
    --ctx_ddl.set_section_attribute('my_sec_group_pref', 'requestor', 'OPTIMIZED_FOR', 'SEARCH');

  -- OPTIONAL: for the full-text search index to index only the requestor JSON field
  begin
    ctx_ddl.drop_preference('my_user_datastore_pref');
  exception when others then null;
  end;
  ctx_ddl.create_preference('my_user_datastore_pref', 'user_datastore');
  ctx_ddl.set_attribute('my_user_datastore_pref', 'procedure', 'myproc');
  ctx_ddl.set_attribute('my_user_datastore_pref', 'output_type', 'blob_loc');
end;
/



-- Optimized Search Index
-- To build on empty collection right after its creation
CREATE SEARCH INDEX sidx_po ON purchase_orders (json_document) FOR JSON
local( partition, partition, partition, partition, partition, partition, partition )
PARAMETERS('filter ctxsys.null_filter Lexer my_lexer_pref Wordlist my_stem_fuzzy_pref Storage my_storage_pref DATAGUIDE OFF SEARCH_ON TEXT MEMORY 4G')
PARALLEL 8; -- Datastore my_user_datastore_pref

CREATE SEARCH INDEX sidx_po ON purchase_orders (json_document) FOR JSON
PARAMETERS('Datastore my_user_datastore_pref DATAGUIDE ON SEARCH_ON TEXT'); -- Datastore my_user_datastore_pref


CREATE INDEX sidx_po ON purchase_orders (json_document)
--local( partition, partition, partition, partition, partition, partition, partition )
INDEXTYPE IS CTXSYS.CONTEXT_V2
PARAMETERS('filter ctxsys.null_filter Lexer my_lexer_pref Wordlist my_stem_fuzzy_pref Storage my_storage_pref section group my_sec_group_pref DATAGUIDE OFF MEMORY 4G')
PARALLEL 8;

CREATE INDEX my_jsn_idx ON PURCHASE_ORDERS (JSON_DOCUMENT) INDEXTYPE IS CTXSYS.CONTEXT_V2 PARAMETERS ('section group my_sec_group');


CREATE SEARCH INDEX sidx_po ON purchase_orders (json_document) FOR JSON
local( partition );


select * from CTX_USER_INDEX_OBJECTS;

select count(*) from purchase_orders p where json_textcontains(p.json_document,'$.requestor','fuzzy(Gya)');


-- Change job properties for "sync every N seconds index"
-- exec DBMS_SCHEDULER.SET_ATTRIBUTE(NAME => 'DR$SIDX_PO$J', ATTRIBUTE => 'job_priority', VALUE => 5 );
-- select * from all_SCHEDULER_JOB_CLASSES;
-- SELECT * FROM dba_rsrc_consumer_group_privs;
-- SELECT plan,status,comments FROM dba_rsrc_;
-- exec DBMS_SCHEDULER.SET_ATTRIBUTE(NAME => 'DR$SIDX_PO$J', ATTRIBUTE => 'job_class', VALUE => 'TP' );
-- select * from user_scheduler_jobs;
-- select systimestamp, log_date, status, to_char(run_duration), to_char(cpu_used) from user_scheduler_job_run_details order by 2 desc;
-- select job_name, session_id, resource_consumer_group, to_char(elapsed_time) from user_scheduler_running_jobs u;
-- exec DBMS_SCHEDULER.stop_JOB (job_name => 'DR$SIDX_PO$J', force=>true);
-- exec DBMS_SCHEDULER.drop_JOB (job_name => 'DR$SIDX_PO$J', force=>true);


-- Shadow index
-- exec ctx_ddl.create_shadow_index('sidx_po', 'replace NOPOPULATE');
-- select idx_id from ctx_user_indexes where idx_name ='SIDX_PO';
-- exec ctx_ddl.populate_pending('RIO$'||1306);
-- exec ctx_ddl.sync_index(idx_name =>'RIO$'||1306, maxtime =>1); -- max: 1 minute refresh duration
-- exec ctx_ddl.exchange_shadow_index('SIDX_PO');


-- Multiple single field indexes
-- Warning for on-premises: uses Advanced Compression Option
create index idx_po_requestor on purchase_orders( json_value( json_document, '$.requestor' ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;
create index idx_po_reference on purchase_orders( json_value( json_document, '$.reference' ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;
create index idx_po_user on purchase_orders( json_value( json_document, '$.user' ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;
create index idx_po_requestedAt on purchase_orders( json_value( json_document, '$.requestedAt' returning TIMESTAMP(3) ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;
create index idx_po_shippingInstructions_name on purchase_orders( json_value( json_document, '$.shippingInstructions.name' ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;
create index idx_po_shippingInstructions_address_street on purchase_orders( json_value( json_document, '$.shippingInstructions.address.street' ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;
create index idx_po_shippingInstructions_address_city on purchase_orders( json_value( json_document, '$.shippingInstructions.address.city' ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;
create index idx_po_shippingInstructions_address_state on purchase_orders( json_value( json_document, '$.shippingInstructions.address.state' ERROR on ERROR NULL ON EMPTY ) ) compress advanced low;
...


CREATE OR REPLACE VIEW INVOICES_REPORT AS
SELECT p.id as purchase_order_id,
       SUM(jt.quantity * jt.unitPrice) as totalPrice,
       SUM(jt.quantity * jt.unitPrice * (1 + ct.tax)) as totalPriceWithVAT,
       jt.country
  FROM purchase_orders p,
       JSON_TABLE( json_document, '$'
                   Columns(Nested items[*]
                         Columns( quantity, unitPrice ),
                     country path '$.shippingInstructions.address.country')
       ) jt,
       invoices i,
       country_taxes ct
 WHERE ct.country_name = jt.country
   and i.id(+) = p.id
   AND i.id is null -- anti-join
 GROUP BY p.id, jt.country;

INSERT INTO invoices
       (id, price, price_with_country_vat, country_name)
SELECT purchase_order_id,
       totalPrice,
       totalPriceWithVAT,
       country
  FROM INVOICES_REPORT;

CREATE MATERIALIZED VIEW PURCHASE_ORDERS_MV
organization heap COMPRESS FOR QUERY LOW
refresh fast ON STATEMENT
enable query rewrite
AS
SELECT p.id, jt.quantity, jt.unitPrice, jt.country
  FROM purchase_orders p,
       JSON_TABLE( json_document, '$'
         Columns(Nested items[*]
           Columns(
             quantity number path '$.quantity'
                      error on error null on empty,
             unitPrice number path '$.unitPrice'
                       error on error null on empty),
           country varchar2(500) path
                 '$.shippingInstructions.address.country'
                   error on error null on empty)
       ) jt;

CREATE OR REPLACE VIEW INVOICES_REPORT AS
SELECT p.id as purchase_order_id,
       SUM(p.quantity * p.unitPrice) as totalPrice,
       SUM(p.quantity * p.unitPrice * (1 + ct.tax))
                                    as totalPriceWithVAT,
       p.country
  FROM PURCHASE_ORDERS_MV p
       left outer join invoices i on i.id = p.id,
       country_taxes ct
 WHERE ct.country_name = p.country
   AND i.id is null -- anti-join
 GROUP BY p.id, p.country;

INSERT INTO invoices
       (id, price, price_with_country_vat, country_name)
SELECT purchase_order_id,
       totalPrice,
       totalPriceWithVAT,
       country
  FROM INVOICES_REPORT;
