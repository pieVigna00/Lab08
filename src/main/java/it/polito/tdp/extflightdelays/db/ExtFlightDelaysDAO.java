package it.polito.tdp.extflightdelays.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.extflightdelays.model.Airline;
import it.polito.tdp.extflightdelays.model.Airport;
import it.polito.tdp.extflightdelays.model.Flight;
import it.polito.tdp.extflightdelays.model.Rotta;

public class ExtFlightDelaysDAO {

	public List<Airline> loadAllAirlines() {
		String sql = "SELECT * from airlines";
		List<Airline> result = new ArrayList<Airline>();

		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				result.add(new Airline(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRLINE")));
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}

	public void loadAllAirports(Map<Integer, Airport> idMapAereoporto) {
		String sql = "SELECT * FROM airports";

		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Airport airport = new Airport(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRPORT"),
						rs.getString("CITY"), rs.getString("STATE"), rs.getString("COUNTRY"), rs.getDouble("LATITUDE"),
						rs.getDouble("LONGITUDE"), rs.getDouble("TIMEZONE_OFFSET"));
				idMapAereoporto.put(rs.getInt("ID"), airport);
			}

			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}

	public List<Flight> loadAllFlights() {
		String sql = "SELECT * FROM flights";
		List<Flight> result = new LinkedList<Flight>();

		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Flight flight = new Flight(rs.getInt("ID"), rs.getInt("AIRLINE_ID"), rs.getInt("FLIGHT_NUMBER"),
						rs.getString("TAIL_NUMBER"), rs.getInt("ORIGIN_AIRPORT_ID"),
						rs.getInt("DESTINATION_AIRPORT_ID"),
						rs.getTimestamp("SCHEDULED_DEPARTURE_DATE").toLocalDateTime(), rs.getDouble("DEPARTURE_DELAY"),
						rs.getDouble("ELAPSED_TIME"), rs.getInt("DISTANCE"),
						rs.getTimestamp("ARRIVAL_DATE").toLocalDateTime(), rs.getDouble("ARRIVAL_DELAY"));
				result.add(flight);
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	public List<Airport> getVertici(int distanza,Map<Integer,Airport> idMapAereoporto ){
		String sql="SELECT a, b, AVG(DISTANCE) AS distanza "
				+ "FROM(SELECT ORIGIN_AIRPORT_ID AS a, DESTINATION_AIRPORT_ID AS b, "
				+ "DISTANCE, ID "
				+ "FROM flights "
				+ "UNION "
				+ "SELECT DESTINATION_AIRPORT_ID AS a, ORIGIN_AIRPORT_ID AS b, "
				+ "DISTANCE, ID "
				+ "FROM flights) AS tab "
				+ "GROUP BY a,b "
				+ "HAVING distanza>?";
		List<Airport> vertici= new LinkedList<>();
		try {
			Connection conn= ConnectDB.getConnection();
			PreparedStatement st= conn.prepareStatement(sql);
			st.setInt(1, distanza);
			ResultSet rs= st.executeQuery();
			while(rs.next()) {
				Airport airport1=idMapAereoporto.get(rs.getInt("a"));
				Airport airport2=idMapAereoporto.get(rs.getInt("b"));
				vertici.add(airport1);
				vertici.add(airport2);
			}
			conn.close();
			return vertici;
		}catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	public List<Rotta> getRotte(int distance, Map<Integer, Airport> idMap){
		String sql="SELECT a, b, AVG(DISTANCE) AS distanza "
				+ "FROM(SELECT ORIGIN_AIRPORT_ID AS a, DESTINATION_AIRPORT_ID AS b, "
				+ "DISTANCE, ID "
				+ "FROM flights "
				+ "UNION "
				+ "SELECT DESTINATION_AIRPORT_ID AS a, ORIGIN_AIRPORT_ID AS b, "
				+ "DISTANCE, ID "
				+ "FROM flights) AS tab "
				+ "GROUP BY a,b "
				+ "HAVING distanza>?";
		List<Rotta> rotte= new ArrayList<Rotta>();
		try {
			Connection conn= ConnectDB.getConnection();
			PreparedStatement st= conn.prepareStatement(sql);
			st.setInt(1, distance);
			ResultSet rs= st.executeQuery();
			while(rs.next()){
				Rotta rotta= new Rotta(idMap.get(rs.getInt("a")), idMap.get(rs.getInt("b")), rs.getInt("distanza"));
				rotte.add(rotta);
			}
			conn.close();
			return rotte;
		}catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
}
