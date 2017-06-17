package com.example;

import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class AsynExecutionExample {
	
	public static void main(String[] args) {

		Session session = Connection.connect();		
		PreparedStatement preparedStatement = session.prepare("select id, name, age from user");
		
		BoundStatement boundStatement = preparedStatement.bind();
		
		ResultSetFuture future = session.executeAsync(boundStatement);
		
		Futures.addCallback(future, new FutureCallback<ResultSet>() {
			
			@Override public void onSuccess(ResultSet result) {
				for(Row row: result) {
					System.out.printf("id: %s, name: %s, age: %d\n", row.get(0, UUID.class), row.getString(1), row.getInt(2));
				}
	        }
	 
	        @Override public void onFailure(Throwable t) {
	            System.err.println("Error while reading Cassandra version: " + t.getMessage());
	        }
			
		});
		
		
		
		Connection.close();
		
	}

}
