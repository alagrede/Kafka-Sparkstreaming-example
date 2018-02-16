# Run SparkStreaming kafka consumer

```
spark-submit --master local --class com.tony.kafka.KafkaSparkStreamingConsumer  target/sparktreaming-kafka-consumer-1.0-SNAPSHOT.jar localhost:2181 MYTOPIC
```

Kerberos parameters example

```
--driver-java-options "-Djava.security.auth.login.config=cluster2/jaas.conf -Djava.security.krb5.conf=cluster2/krb5.conf" --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=cluster2/jaas.conf -Djava.security.krb5.conf=cluster2/krb5.conf"
```
# Run spark locally on Windows

Download __winutils.exe__ from the repository to some local folder, e.g. __C:\hadoop\bin__.
https://github.com/steveloughran/winutils

Set HADOOP_HOME to C:\hadoop.
Open command prompt with admin rights.
Run C:\hadoop\bin\winutils.exe chmod 777 /tmp
Run C:\hadoop\bin\winutils.exe chmod 777 /Temp
Download and install Spark on Windows (add the bin folder to the PATH)
