%load_ext sql
import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")
DWH_ENDPOINT="dwhcluster.cgxkepuyefux.us-west-2.redshift.amazonaws.com" 
DWH_ROLE_ARN="arn:aws:iam::837210865452:role/dwhRole"
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string
import boto3

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket("udacity-labs")

for obj in sampleDbBucket.objects.filter(Prefix="tickets"):
    print(obj)

%%sql 
DROP TABLE IF EXISTS "sporting_event_ticket";
CREATE TABLE "sporting_event_ticket" (
    "id" double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    "sporting_event_id" double precision NOT NULL,
    "sport_location_id" double precision NOT NULL,
    "seat_level" numeric(1,0) NOT NULL,
    "seat_section" character varying(15) NOT NULL,
    "seat_row" character varying(10) NOT NULL,
    "seat" character varying(10) NOT NULL,
    "ticketholder_id" double precision,
    "ticket_price" numeric(8,2) NOT NULL
);

%%time
qry = """COPY sporting_event_ticket
         FROM 's3://udacity-labs/tickets/split/part'
         CREDENTIALS 'aws_iam_role={}'
         GZIP
         DELIMITER ';'
         COMPUPDATE OFF REGION 'us-west-2';
""".format(DWH_ROLE_ARN)

%sql $qry


%%sql
DROP TABLE IF EXISTS "sporting_event_ticket_full";
CREATE TABLE "sporting_event_ticket_full" (
    "id" double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    "sporting_event_id" double precision NOT NULL,
    "sport_location_id" double precision NOT NULL,
    "seat_level" numeric(1,0) NOT NULL,
    "seat_section" character varying(15) NOT NULL,
    "seat_row" character varying(10) NOT NULL,
    "seat" character varying(10) NOT NULL,
    "ticketholder_id" double precision,
    "ticket_price" numeric(8,2) NOT NULL
);


%%time

qry = """COPY sporting_event_ticket_full
         FROM 's3://udacity-labs/tickets/full/full.csv.gz'
         CREDENTIALS 'aws_iam_role={}'
         GZIP
         DELIMITER ';'
         COMPUPDATE OFF REGION 'us-west-2';
""".format(DWH_ROLE_ARN)

%sql $qry

