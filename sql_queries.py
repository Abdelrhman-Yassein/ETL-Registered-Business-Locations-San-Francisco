
# Drop Table if exixtx
drop_locationtime_table = "DROP TABLE IF EXISTS locationtime;"
drop_businesstime_table = "DROP TABLE IF EXISTS businesstime;"
drop_registeredbusiness_table = "DROP TABLE IF EXISTS registeredbusiness;"
drop_location_table = "DROP TABLE IF EXISTS location;"

# Create table
#sql create locationtime table
create_locationtime_table = """
                            CREATE TABLE locationtime
                                (
                                    location_start_date timestamp without time zone NOT NULL,
                                    hour                INT4,
                                    day                 INT4,
                                    week                INT4,
                                    month               INT4,
                                    year                INT4,
                                    weekday             text,
                                    PRIMARY KEY (location_start_date)
                                );  
                            """

#sql create businesstime table
create_businesstime_table = """
                             CREATE TABLE businesstime
                                (
                                    business_start_date timestamp without time zone NOT NULL,
                                    hour                INT4,
                                    day                 INT4,
                                    week                INT4,
                                    month               INT4,
                                    year                INT4,
                                    weekday             text,
                                    PRIMARY KEY (business_start_date)
                                );  
                            """
#sql create registeredbusiness table
create_registeredbusiness_table = """
                            CREATE TABLE registeredbusiness
                                (
                                    uniqueid                text NOT NULL,
                                    business_account_number INT4 NOT NULL,
                                    ownership_name          text NOT NULL,
                                    parking_tax             BOOL NOT NULL,
                                    transient_occupancy_tax BOOL NOT NULL,
                                    business_location       text NOT NULL,
                                    location_name           text NOT NULL,
                                    business_start_date     timestamp without time zone NOT NULL,
                                    location_start_date     timestamp without time zone NOT NULL,
                                    PRIMARY KEY (uniqueid)
                                );  
                               """
#sql create location table
create_location_table = """
                        CREATE TABLE location
                                (
                                    location_name  text NOT NULL,
                                    street_address text,
                                    city           text,
                                    state          text,
                                    source_zipcode text,
                                    PRIMARY KEY (location_name)
                                );   
                        """

# INSERT RECORDS
#sql insert locationtime table
insert_locationtime_table =( """
INSERT INTO locationtime(location_start_date,hour,day,week,month,year,weekday)
        VALUES(%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;
 """)

#sql insert businesstime table
insert_businesstime_table =( """
INSERT INTO businesstime(business_start_date,hour,day,week,month,year,weekday)
        VALUES(%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;  
""")
#sql insert registeredbusiness table
insert_registeredbusiness_table = ("""
INSERT INTO registeredbusiness(uniqueid,business_account_number,ownership_name,parking_tax,transient_occupancy_tax,business_location,location_name,business_start_date, location_start_date)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;  
""")
#sql insert location table
insert_location_table =( """
INSERT INTO location(location_name,street_address,city,state,source_zipcode)
        VALUES(%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;    
""")


# QUERY LISTS

create_table_queries = [create_locationtime_table, create_businesstime_table,
                        create_registeredbusiness_table, create_location_table]


drop_table_queries = [drop_locationtime_table, drop_businesstime_table,
                      drop_registeredbusiness_table, drop_location_table]
