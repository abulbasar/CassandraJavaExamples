package com.example;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

/*
Equivalent CQL statement: 
BEGIN BATCH
INSERT INTO user (id, name) VALUES(now(), 'user-1');
INSERT INTO user (id, name) VALUES(now(), 'user-2');
INSERT INTO user (id, name) VALUES(now(), 'user-3');
APPLY BATCH;
*/

public class BatchStatementExample {
	public static void main(String[] args) {

		Session session = Connection.connect();		
		BatchStatement batchStatement = new BatchStatement();
		
		PreparedStatement preparedStatement = session.prepare("insert into user (id, name) values (?, ?)");
		int i = 0;
		while(i < 10) {
			batchStatement.add(preparedStatement.bind(UUIDs.timeBased(), "user-" + i));
			++i;
		}

		try {
			ResultSet rs = session.execute(batchStatement);
			System.out.println(rs);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		Connection.close();

	}
}
