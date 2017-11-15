package com.example;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;


public class DataLoader {
	
	private static Session session = null;
	private static PreparedStatement loadStatement = null;
		
	public static List<String[]> readData(String path) throws Exception {

		CsvParserSettings settings = new CsvParserSettings();
		settings.getFormat().setLineSeparator("\n");
		CsvParser parser = new CsvParser(settings);
		InputStream inputStream = new FileInputStream(path);
		
		InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
		List<String[]> allRows = parser.parseAll(inputStreamReader);

		return allRows;
		
	}
	
	/*create table stocks(date date, open double,high double,low double,close double,volume double,
	 * adjclose double,symbol text, primary key((symbol), date)) with clustering order by (date DESC);*/
	
	public static void main(String[] args) throws Exception {
		Cluster cluster = Cluster
				.builder()
				.addContactPoint("localhost")
				.build();
		session = cluster.connect();
		String sql = "insert into demo.stocks (date, open, high, low, close, volume, adjclose, symbol) values (?, ?, ?, ?, ?, ?, ?, ?)";
		loadStatement = session.prepare(sql);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		List<String[]> rows = readData("/home/training/Downloads/datasets/stocks.csv");
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
				
				session.execute(loadStatement.bind(localDate, open, high, low, close, volume, adjclose, symbol));
			}
		}
		session.close();
		cluster.close();
	}

}
