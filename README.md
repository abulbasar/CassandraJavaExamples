# cassandra-java-driver-examples
Java code examples for Cassandra driver 

Aspect to look after in the Cassandra client application 

- Cluster
- Session
- Compression
- Authentication and Encryption
- Connection Pooling
- SimpleStatement
- PreparedStatement
- AsynchrnousExecution
- Policies 
  – LoadBalancing
  - Retry
  - SpeculativeExecution
- Query with tracing


Data Type Matching Between CQL Type and Java Type
```
CQL              Java
===              ====
boolean          java.lang.Boolean
int              java.lang.Integer
bigint           java.lang.Long
float            java.lang.Float
double           java.lang.Double
inet             java.net.InetAddress
text             java.lang.String
ascii            java.lang.String
timestamp        java.util.Date
uuid             java.util.UUID
timeuuid         java.util.UUID
varint           java.math.BigInteger
decimal          java.math.BigDecimal
blob             java.nio.ByteBuffer
list<E>          java.util.List<E>      where E is also a type from this list
set<E>           java.util.Set<E>       where E is also a type from this list
map<K,V>         java.util.Map<K,V>     where K and V is also a types from this list
(user type)      com.datastax.driver.core.UDTValue
(tuple type)     com.datastax.driver.core.TupleValue
```

