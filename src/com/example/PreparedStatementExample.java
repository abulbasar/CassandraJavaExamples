package com.example;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

public class PreparedStatementExample {

	public static void main(String[] args) {

		Session session = Connection.connect();		
		PreparedStatement preparedStatement = session.prepare("insert into user (id, name, age) values (?, ?, ?)");

		try {
			BoundStatement boundStatement = preparedStatement.bind(UUIDs.timeBased(), "Hector", 34);
			ResultSet rs = session.execute(boundStatement);
			System.out.println(rs);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		Connection.close();

	}

}
