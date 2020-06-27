# nifi-load-clickhouse

Flow for insertion data into ClickHouse. 
# Requirements
* JDK 1.8 
* Apache Maven 3.1.1 or newer 

# Getting Started
 To build :
 * Execute `mvn clean install` for each bundle
 * install each *.nar file on own nifi instance (paste into `*/lib` folder)
 
 # Template
 There is a basic idea how to load data into clickhouse.
 * Distribute data by key[keys] in nifi and load result regardless on every shard 
 
