DELETE FROM ENTRY_ENTITY WHERE OWNER = 'default';
DELETE FROM ENTRY_PATH WHERE OWNER = 'default';
DELETE FROM ENTRY_DATASET_RELATIONSHIPS WHERE OWNER = 'default';
DELETE FROM ENTRY_ORDER WHERE OWNER = 'default';
DELETE FROM ENTRY_FILTERS WHERE OWNER = 'default';
DELETE FROM ENTRY_HAVING WHERE OWNER = 'default';
delete from ENTRY_AGGREGATORS where OWNER='default';
DELETE FROM ENTRY_DATASET_MAPPINGS where OWNER = 'default';
DELETE FROM ENTRY_DV_ENTITY where OWNER='default';
DELETE FROM ENTRY_DV_MAPPINGS where OWNER='default';
DELETE FROM ENTRY_DV_PROPERTIES where OWNER='default';

INSERT INTO ENTRY_ENTITY(COD_ENTITY, TABLE_NAME, ENTITY_TYPE, COD_PATH, STRATEGY, OWNER) VALUES
-- Sources
('ET_PSA_CUSTOMERS', 'PSA_CUSTOMERS','SRC', 'PSA', NULL, 'default'),
('ET_PSA_SALES', 'PSA_SALES','SRC', 'PSA', NULL, 'default');


INSERT INTO ENTRY_PATH (COD_PATH, DATABASE_NAME, SCHEMA_NAME, OWNER) VALUES
('PSA', 'metamorf_data_example', 'metamorf', 'default'),
('RDV', 'metamorf_data_example', 'metamorf', 'default'),
('IM', 'metamorf_data_example', 'metamorf', 'default');

INSERT INTO ENTRY_DV_ENTITY (COD_ENTITY,ENTITY_NAME,ENTITY_TYPE,COD_PATH,NAME_STATUS_TRACKING_SATELLITE,NAME_RECORD_TRACKING_SATELLITE,NAME_EFFECTIVITY_SATELLITE,OWNER) VALUES
	 ('ET_HUB_CUSTOMER','HUB_CUSTOMER','HUB','RDV','STS_CUSTOMER','RTS_CUSTOMER','','default'),
	 ('ET_HUB_SHOP','HUB_SHOP','HUB','RDV','','','','default'),
	 ('ET_HUB_PRODUCT','HUB_PRODUCT','HUB','RDV','STS_PRODUCT','','','default'),
	 ('ET_HUB_INVOICE','HUB_INVOICE','HUB','RDV','','RTS_INVOICE','','default'),
	 ('ET_LINK_PURCHASE','LINK_PURCHASE_DETAIL','LINK','RDV','','','','default');

INSERT INTO ENTRY_DV_MAPPINGS (COD_ENTITY_SOURCE,COLUMN_NAME_SOURCE,COD_ENTITY_TARGET,COLUMN_NAME_TARGET,COLUMN_TYPE_TARGET,ORDINAL_POSITION,COLUMN_LENGTH,COLUMN_PRECISION,NUM_BRANCH,NUM_CONNECTION,KEY_TYPE,SATELLITE_NAME,ORIGIN_IS_INCREMENTAL,ORIGIN_IS_TOTAL,ORIGIN_IS_CDC,OWNER) VALUES
	 ('ET_PSA_CUSTOMERS','CUSTOMER_ID','ET_HUB_CUSTOMER','CUSTOMER_ID','INTEGER',1,0,0,1,0,'BK','',1,0,0,'default'),
	 ('ET_PSA_CUSTOMERS','''customers.csv''','ET_HUB_CUSTOMER','RECORD_SOURCE','TEXT',2,0,0,1,0,'RS','',1,0,0,'default'),
	 ('ET_PSA_CUSTOMERS','''SPAIN''','ET_HUB_CUSTOMER','TENANT_COUNTRY','TEXT',3,0,0,1,0,'TN','',1,0,0,'default'),
	 ('ET_PSA_CUSTOMERS','REGISTRATION_DATE','ET_HUB_CUSTOMER','APPLIED_DATE','TEXT',4,0,0,1,0,'AD','',1,0,0,'default'),
	 ('ET_PSA_CUSTOMERS','CUSTOMER_PHONE','ET_HUB_CUSTOMER','CUSTOMER_PHONE','TEXT',5,0,0,1,0,'DCK','SAT_CUSTOMER_CONTACT',1,0,0,'default'),
	 ('ET_PSA_CUSTOMERS','REGISTRATION_DATE','ET_HUB_CUSTOMER','REGISTRATION_DATE','TEXT',6,0,0,1,0,'AT','SAT_CUSTOMER_CONTACT',1,0,0,'default'),
	 ('ET_PSA_CUSTOMERS','CUSTOMER_EMAIL','ET_HUB_CUSTOMER','CUSTOMER_EMAIL','TEXT',7,0,0,1,0,'AT','SAT_CUSTOMER_INFO',1,0,0,'default'),
	 ('ET_PSA_SALES','SHOP_ID','ET_HUB_SHOP','SHOP_ID','INTEGER',1,0,0,1,0,'BK','',1,0,0,'default'),
	 ('ET_PSA_SALES','''sales.csv''','ET_HUB_SHOP','RECORD_SOURCE','TEXT',2,0,0,1,0,'RS','',1,0,0,'default'),
	 ('ET_PSA_SALES','''SPAIN''','ET_HUB_SHOP','TENANT_COUNTRY','TEXT',3,0,0,1,0,'TN','',1,0,0,'default'),
	 ('ET_PSA_SALES','CUSTOMER_ID','ET_HUB_CUSTOMER','CUSTOMER_ID','INTEGER',1,0,0,2,0,'BK','',1,0,0,'default'),
	 ('ET_PSA_SALES','''sales.csv''','ET_HUB_CUSTOMER','RECORD_SOURCE','TEXT',2,0,0,2,0,'RS','',1,0,0,'default'),
	 ('ET_PSA_SALES','''SPAIN''','ET_HUB_CUSTOMER','TENANT_COUNTRY','TEXT',3,0,0,2,0,'TN','',1,0,0,'default'),
	 ('ET_PSA_SALES','INVOICE_DATE','ET_HUB_CUSTOMER','APPLIED_DATE','TEXT',4,0,0,2,0,'AD','',1,0,0,'default'),
	 ('ET_PSA_SALES','PRODUCT_NAME','ET_HUB_PRODUCT','PRODUCT_ID','TEXT',1,0,0,1,0,'BK','',1,0,0,'default'),
	 ('ET_PSA_SALES','''sales.csv''','ET_HUB_PRODUCT','RECORD_SOURCE','TEXT',2,0,0,1,0,'RS','',1,0,0,'default'),
	 ('ET_PSA_SALES','''SPAIN''','ET_HUB_PRODUCT','TENANT_COUNTRY','TEXT',3,0,0,1,0,'TN','',1,0,0,'default'),
	 ('ET_PSA_SALES','INVOICE_DATE','ET_HUB_PRODUCT','APPLIED_DATE','TEXT',4,0,0,1,0,'AD','',1,0,0,'default'),
	 ('ET_PSA_SALES','PRODUCT_COLOR','ET_HUB_PRODUCT','PRODUCT_COLOR','TEXT',5,0,0,1,0,'AT','SAT_PRODUCT_INFO',1,0,0,'default'),
	 ('ET_PSA_SALES','PRODUCT_SIZE','ET_HUB_PRODUCT','PRODUCT_SIZE','TEXT',6,0,0,1,0,'AT','SAT_PRODUCT_INFO',1,0,0,'default'),
	 ('ET_PSA_SALES','UNIT_PRICE','ET_HUB_PRODUCT','UNIT_PRICE','TEXT',7,0,0,1,0,'AT','SAT_PRODUCT_PRICE_HISTORY',1,0,0,'default'),
	 ('ET_PSA_SALES','INVOICE_NO','ET_HUB_INVOICE','INVOICE_ID','TEXT',1,0,0,1,0,'BK','',1,0,0,'default'),
	 ('ET_PSA_SALES','''sales.csv''','ET_HUB_INVOICE','RECORD_SOURCE','TEXT',2,0,0,1,0,'RS','',1,0,0,'default'),
	 ('ET_PSA_SALES','''SPAIN''','ET_HUB_INVOICE','TENANT_COUNTRY','TEXT',3,0,0,1,0,'TN','',1,0,0,'default'),
	 ('ET_PSA_SALES','INVOICE_DATE','ET_HUB_INVOICE','APPLIED_DATE','TEXT',4,0,0,1,0,'AD','',1,0,0,'default'),
	 ('ET_PSA_SALES','INVOICE_NO','ET_LINK_PURCHASE','INVOICE_ID','TEXT',1,0,0,1,1,'BK','',1,0,0,'default'),
	 ('ET_PSA_SALES','CUSTOMER_ID','ET_LINK_PURCHASE','CUSTOMER_ID','INTEGER',2,0,0,1,2,'BK','',1,0,0,'default'),
	 ('ET_PSA_SALES','SHOP_ID','ET_LINK_PURCHASE','SHOP_ID','INTEGER',3,0,0,1,3,'BK','',1,0,0,'default'),
	 ('ET_PSA_SALES','PRODUCT_NAME','ET_LINK_PURCHASE','PRODUCT_NAME','TEXT',4,0,0,1,4,'BK','',1,0,0,'default'),
	 ('ET_PSA_SALES','''sales.csv''','ET_LINK_PURCHASE','RECORD_SOURCE','TEXT',5,0,0,1,0,'RS','',1,0,0,'default'),
	 ('ET_PSA_SALES','''SPAIN''','ET_LINK_PURCHASE','TENANT_COUNTRY','TEXT',6,0,0,1,0,'TN','',1,0,0,'default'),
	 ('ET_PSA_SALES','INVOICE_DATE','ET_LINK_PURCHASE','APPLIED_DATE','TEXT',7,0,0,1,0,'AD','',1,0,0,'default'),
	 ('ET_PSA_SALES','QUANTITY','ET_LINK_PURCHASE','QUANTITY','INTEGER',8,0,0,1,0,'AT','SAT_PURCHASE_INFO',1,0,0,'default'),
	 ('ET_PSA_SALES','UNIT_PRICE','ET_LINK_PURCHASE','UNIT_PRICE','INTEGER',9,0,0,1,0,'AT','SAT_PURCHASE_INFO',1,0,0,'default');

INSERT INTO ENTRY_DV_PROPERTIES (COD_ENTITY,NUM_CONNECTION,HASH_NAME,OWNER) VALUES
	 ('ET_HUB_CUSTOMER',0,'HASH_CUSTOMER','default'),
	 ('ET_HUB_SHOP',0,'HASH_SHOP','default'),
	 ('ET_HUB_PRODUCT',0,'HASH_PRODUCT','default'),
	 ('ET_HUB_SHOP',0,'HASH_SHOP','default'),
	 ('ET_HUB_INVOICE',0,'HASH_INVOICE','default'),
	 ('ET_LINK_PURCHASE',1,'HASH_INVOICE','default'),
	 ('ET_LINK_PURCHASE',2,'HASH_CUSTOMER','default'),
	 ('ET_LINK_PURCHASE',3,'HASH_SHOP','default'),
	 ('ET_LINK_PURCHASE',4,'HASH_PRODUCT','default');

