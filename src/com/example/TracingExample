package com.example;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.text.SimpleDateFormat;


public class TracingExample {


    static void printQueryTrace(QueryTrace queryTrave){
        System.out.printf("\n\nQuery trace: %s\n\n", queryTrave.getTraceId());
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        System.out.printf("%15s | %10s | %12s  | %40s | %s \n",
                "Timestamp",
                "Source",
                "Time (μs)",
                "Thead",
                "Description"
        );
        System.out.println("--------------------------------------------------------------------------------------------------------------");
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        for(QueryTrace.Event event: queryTrave.getEvents()){
            System.out.printf("%15s | %10s | %12s  | %40s | %s \n",
                    sdf.format(event.getTimestamp()),
                    event.getSource(),
                    event.getSourceElapsedMicros(),
                    event.getThreadName(),
                    event.getDescription()

            );
        }
        System.out.println("--------------------------------------------------------------------------------------------------------------");

    }

    public static void main(String[] args){
        Cluster cluster;
        Session session;
        cluster = Cluster
                .builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE) //Other option: DowngradingConsistencyRetryPolicy
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .build();
        session = cluster.connect("demo");


        PreparedStatement statement = session.prepare("INSERT INTO user (id, name) VALUES (?, ?)");
        Statement boundStatement = statement
                .bind(1, "user 1")
                .enableTracing();

        long startTime = System.currentTimeMillis();
        ResultSet resultSet = session.execute(boundStatement);
        long duration = System.currentTimeMillis() - startTime;
        System.out.format("Time taken: %d", duration);

        ExecutionInfo executionInfo = resultSet.getExecutionInfo();
        printQueryTrace(executionInfo.getQueryTrace());
        cluster.close();

    }
}
