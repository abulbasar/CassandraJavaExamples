package com.example;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.*;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;


public class DataLoader {

		
	public static List<String[]> readData(String path) throws Exception {

		CsvParserSettings settings = new CsvParserSettings();
		settings.getFormat().setLineSeparator("\n");
		CsvParser parser = new CsvParser(settings);
		InputStream inputStream = new FileInputStream(path);
		
		InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
		List<String[]> allRows = parser.parseAll(inputStreamReader);

		return allRows;
		
	}
	
	/*

	create table stocks(date date, open double,high double,low double,close double,volume double,
	 adjclose double,symbol text, primary key((symbol), date)) with clustering order by (date DESC);

	 */
	
	public static void main(String[] args) throws Exception {

	    String inputFile = args[0];

		Cluster cluster = Cluster
				.builder()
				.addContactPoint("localhost")
				.build();
		Session session = cluster.connect();
		String sql = "insert into demo.stocks (date, open, high, low, close, volume, adjclose, symbol) values (?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement loadStatement = session.prepare(sql);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		List<String[]> rows = readData(inputFile);
		List<Boolean> results = new ArrayList<>();

		long startTime = System.currentTimeMillis();
		for(String[] tokens: rows) {
			if(!tokens[0].startsWith("date")) {
				Date date = simpleDateFormat.parse(tokens[0]);
				LocalDate localDate = LocalDate.fromMillisSinceEpoch(date.getTime());
				Double open = Double.valueOf(tokens[1]);
				Double high= Double.valueOf(tokens[2]); 
				Double low= Double.valueOf(tokens[3]);
				Double close= Double.valueOf(tokens[4]); 
				Double volume= Double.valueOf(tokens[5]);
				Double adjclose = Double.valueOf(tokens[6]); 
				String symbol = tokens[7];
				
				ResultSet resultSet = session.execute(loadStatement.bind(localDate, open, high, low, close, volume, adjclose, symbol));
                results.add(resultSet.wasApplied());
			}
		}

		double avgTime = (System.currentTimeMillis() - startTime) * 1.0 / rows.size();

		System.out.println(String.format("Job is complete. Avg write time (ms): %.2f", avgTime));

		String select = "select * from demo.stocks";
		ResultSet resultSet = session.execute(select);
		for(Row row : resultSet){
		    System.out.println(row);
        }

		session.close();
		cluster.close();
	}

}
