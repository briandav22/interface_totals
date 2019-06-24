import psycopg2
import csv
import os
import datetime
import json

fileDir = os.path.dirname(os.path.abspath(__file__))

## check if file exists, if not create it. 
if os.path.exists(fileDir +'/aggregate_utilization.csv'):
    pass
else:
    with open('aggregate_utilization.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        #set up values for column names
        writer.writerow(['Interface Description'] + ['Inbound Bytes'] + ['Outbound Bytes'] + ['Timestamp'] + ['Number of Interfaces Polled'])

#open up config file
with open(fileDir +'/config.json', 'r') as config_file:
    config = json.load(config_file)
    PG_CONFIG = config['PG_CONFIG']
    INTERFACE_LIST = config['INTERFACE_LIST']



## create postgres authenticaion from the config file.  
PG_HOST=PG_CONFIG['PG_HOST']
PG_DATABASE=PG_CONFIG['PG_DATABASE']
PG_USER=PG_CONFIG['PG_USER']
PG_PASSWORD=PG_CONFIG['PG_PASSWORD']

#query database to get back interface statistics. 

def get_interfaces(snmp_value):
    try:
        connection = psycopg2.connect(user = PG_USER,
                                    password = PG_PASSWORD,
                                    host = PG_HOST,
                                    port = "5432",
                                    database = PG_DATABASE)

        cursor = connection.cursor()
        
        cursor.execute("""SELECT * FROM (
    SELECT
    inet_b2a(ai.device_id) AS exporter_ip,
    CASE
        WHEN NOT(ii.custom_name = '' OR ISNULL(ii.custom_name)) THEN CONCAT(ii.custom_name,' - ', ii.snmp_interface)
        WHEN NOT(ii.snmp_alias = '' OR ISNULL(ii.snmp_alias)) THEN CONCAT(ii.snmp_alias,' (',ii.snmp_description,')', ' - ', ii.snmp_interface)
        WHEN NOT(ii.snmp_name = '' OR ISNULL(ii.snmp_name)) THEN CONCAT(ii.snmp_description,' (',ii.snmp_name,')', ' - ', ii.snmp_interface)
        WHEN NOT(ii.snmp_description = '' OR ISNULL(ii.snmp_description)) THEN CONCAT(ii.snmp_description, ' - ', ii.snmp_interface)
        ELSE CONCAT('Instance ',' ',ii.snmp_interface)
    END AS interface,
    SUM(in_bytes) AS sum_in,
    SUM(out_bytes) AS sum_out
    
    FROM plixer.distributed_activeif ai
    JOIN plixer.distributed_ifinfo ii ON ai.device_id = ii.device_id AND ai.snmp_interface = ii.snmp_interface
    GROUP BY ai.device_id,ai.snmp_interface,ii.device_id, ii.snmp_interface,
    ii.snmp_description,
    ii.snmp_name,
    ii.snmp_alias,
    ii.custom_name
    ) AS t
    WHERE t.interface LIKE '%{}%';""".format(snmp_value))
        
        data_back = cursor.fetchall()
        connection.commit()
        cursor.close()
        connection.close()
        return data_back
        

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)



def total_bytes(snmp_value):
    time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    interfaces = {
        "snmp_like" : "{}".format(snmp_value),
        "bytes_in" : 0,
        "bytes_out" :0,
        "time_stamp" : time_stamp,
        "number_of_ints":0,
    }
    data_back = get_interfaces(snmp_value)
    for data in data_back:
        interfaces["bytes_in"] += data[2]
        interfaces["bytes_out"] += data[3]
        interfaces["number_of_ints"] += 1

    return interfaces

#open up CSV and write in totals for last 5 minutes. 
with open('aggregate_utilization.csv', mode='a', newline='') as interface_totals:
    interface_totals = csv.writer(interface_totals, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    #loop through each interface in the config and gather data, writing it out to CSV
    for interface in INTERFACE_LIST:
        interface_data = total_bytes(interface)    
        interface_totals.writerow([interface_data[key] for key in interface_data])

    