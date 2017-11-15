package com.example;

import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.utils.UUIDs;

public class SimpleStatementExample {

	public static void main(String[] args) {
		Session session = Connection.connect();
				
		SimpleStatement statement1 = new SimpleStatement("insert into user (id, name, age) values (?, ?, ?)",
				UUIDs.timeBased(), "user01", 30);
		
		statement1.setConsistencyLevel(ConsistencyLevel.ONE);

		try {
			
			ResultSet rs = session.execute(statement1);
			System.out.println(rs);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		SimpleStatement statement2 = new SimpleStatement("select id, name, age from user");

		ResultSet rs2 = session.execute(statement2);

		System.out.println(rs2);

		for (Row row : rs2) {
			System.out.printf("id: %s, name: %s, age: %d\n", row.get(0, UUID.class), 
					  row.getString(1), row.getInt(2));
		}

		Connection.close();
	}

}
