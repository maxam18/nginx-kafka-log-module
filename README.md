# Log nginx requests into Apache Kafka 

**[Nginx](https://nignx.org/)** module to log requests into an Apache Kafka

Uses [librdkafka](https://github.com/edenhill/librdkafka)

Inspired by [nginx-kafka-log-module](https://github.com/kaltura/nginx-kafka-log-module)

# Build

## Static build

    ./configure --add-module=/path/to/nginx-kafka-log-module

## Dynamic library
  
	./configure --add-dynamic-module=/path/to/nginx-kafka-log-module

# Configuration

**Nginx** configuration directives below. Module does not register any *nginx variables*

## klm_brokers
**Syntax**: `klm_brokers  brokers_list`
**Default**: `NULL`
**Context**: main   

    Sets Apache Kafka bootstrap servers. 

## klm_prop
**Syntax**: `klm_prop  property  value`
**Default**: `NULL`
**Context**: `main`

    Various librdkafka kafka configuration properties. Accepts multiple entries.

## klm_format
**Syntax**: `klm_format  name  format_string [message_id]`
**Default**: `NULL`
**Context**: `main`

    Log format to construct the message body. Where the 
    `name` is the format name to use in the `klm_log` instructions
    `format_string` accepts nginx variables.     
    `message_id` is an optional parameter. Kafka message key will be set to this string. Accepts nginx variables.

## klm_log
* **syntax**: `klm_log  topic[:partition]  format_name  [fallback_file]`
* **default**: `NULL`
* **context**: `location`

Enables logging from the location. Accepts multiple entries. Where 
    `topic` is the topic name,
    `partition` is the partition number or `RD_KAFKA_PARTITION_UA` if omitted, 
    `format_name` the name of the formatted string to set the message body and message key.
    `fallback_file` is an optional parameter. The file where to save messages dropped by librdkafka.

# Try
## Configuration example

    http {
    	klm_brokers     kafka1,kafka2,kafka3;
        klm_format      main        '$msec $request_uri $host';
        klm_format      mainNkey    '$msec $request_uri $host'  '$cookie_auth';
        klm_prop        client.id   nginx;
        klm_porp        log_level   3;
    	
    	server {
            listen 8080;
    
    		location /topic21 {
                empty_gif;
                klm_log     topic.twenty.one    main;
                klm_log     auth.topic          mainNkey;
    		}
            location /important {
                emtpy_gif;
                klm_log     topic.important     main    logs/important.log;
            }
    	}

Requests to the http://testhost:8080/topic21 send two messages to the `kafka1,kafka2,kafka3` Apache Kafka cluster. One message to the topic `topic.twenty.one` into a random partition with the text like `123456789.321 /topic21 testhost`. Second message to the topic `auth.topic` into a pertition selected on the hash from `auth` cookie with the same and the message id `auth` cookie content
Requests to the http://testhost:8080/important send message to the topic `topic.important` with the same message format as above. In the case of kafka failure the message is written to the file `logs/importnant.log`

## Author
Max Amzarakov `maxam18 at Google`
