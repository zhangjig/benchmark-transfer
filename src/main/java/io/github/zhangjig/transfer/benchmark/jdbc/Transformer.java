package io.github.zhangjig.transfer.benchmark.jdbc;

import io.github.zhangjig.transfer.benchmark.util.Sequences;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Transformer {

	private static Transformer instance = new Transformer();
	
	public static Transformer getInstance() {
		return instance;
	}
	
	public static final String[] TABLE_NAMES = { "A", "B", "D", "E", "C" };
	
	public static final String SELECT_SQL = "SELECT * FROM %S WHERE BASE_ACCT_NO = ?";
	
	private void select(Connection conn, String id, String name) throws SQLException {
		String sql = String.format(SELECT_SQL, name);
		try(PreparedStatement ps = conn.prepareStatement(sql)){
			ps.setString(1,  id);
			ResultSet rs = ps.executeQuery();
			int cc = rs.getMetaData().getColumnCount();
			while(rs.next()) {
				for (int i = 1; i < cc + 1; i++) {
					rs.getObject(i);
				}
			}
			rs.close();
		}
	}
	
	private void select(Connection conn, String id, int count) throws SQLException {
//		int num = (int) (Math.random() * 5);
		for (int i = 0; i < count; i++) {
			String table = TABLE_NAMES[i % 5];
			select(conn, id, table);
		}
	}
	
	private String getAcct(Connection conn, String no) {
		try(PreparedStatement ps = conn.prepareStatement("SELECT INTERNAL_KEY FROM MB_ACCT WHERE BASE_ACCT_NO = ?")) {
			ps.setString(1, no);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				return rs.getString(1);
			}
			throw new RuntimeException("acct no " + no + " not exist!");
		} catch (SQLException e) {
			throw new RuntimeException("query acct info faild", e);
		}
	}
	
	public BigDecimal checkAmount(Connection conn, String id, BigDecimal amount) {
		try(PreparedStatement ps = conn.prepareStatement("SELECT TOTAL_AMOUNT FROM MB_ACCT_BALANCE WHERE INTERNAL_KEY = ?")) {
			ps.setString(1, id);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				BigDecimal totalAmount = rs.getBigDecimal(1);
//				if(totalAmount.compareTo(amount) < 0) {
//					throw new RuntimeException("total amount not eng");
//				}
				return totalAmount;
			}
			throw new RuntimeException("acct no " + id + " not exist!");
		} catch (SQLException e) {
			throw new RuntimeException("query acct info faild", e);
		}
	}
	
	private void updateAmount(Connection conn, String id, BigDecimal amount) {
		try (PreparedStatement ps = conn.prepareStatement("UPDATE MB_ACCT_BALANCE SET TOTAL_AMOUNT = ? WHERE INTERNAL_KEY = ?")) {
			ps.setBigDecimal(1, amount);
			ps.setString(2, id);
			if(ps.executeUpdate() == 0) {
				throw new RuntimeException("acct no " + id + " not exist!");
			}
		} catch (SQLException e) {
			throw new RuntimeException("update amount faild", e);
		}
	}
	
	private void updateLimit(Connection conn, String id, BigDecimal amount) {
		try (PreparedStatement ps = conn.prepareStatement("UPDATE MB_ACCT_LIMIT SET AMOUNT = ? WHERE INTERNAL_KEY = ?")) {
			ps.setBigDecimal(1, amount);
			ps.setString(2, id);
			if(ps.executeUpdate() == 0) {
				throw new RuntimeException("acct no " + id + " not exist!");
			}
		} catch (SQLException e) {
			throw new RuntimeException("update amount faild", e);
		}
	}
	
	private void updateSeq(Connection conn, String id) {
		try (PreparedStatement ps = conn.prepareStatement("UPDATE MB_SEQ SET SEQ = SEQ + 1 WHERE ID = ?")) {
			ps.setString(1, id);
			if(ps.executeUpdate() == 0) {
				throw new RuntimeException("acct no " + id + " not exist!");
			}
		} catch (SQLException e) {
			throw new RuntimeException("update amount faild", e);
		}
	}
	
	private void insertHist(Connection conn, String table, String no, String id, BigDecimal amount, long key) {
		final SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + table
				+ "(INTERNAL_KEY, TRAN_DATE, USER_ID, BASE_ACCT_NO, CCY, ID) VALUES(?, ?,?,?,?,?)")) {
			ps.setString(1, id);
			ps.setString(2, format.format(new Date()));
			ps.setString(3, no);
			ps.setString(4, no);
			ps.setString(5, "AA");
			ps.setLong(6, key);
			if(ps.executeUpdate() == 0) {
				throw new RuntimeException("acct no " + id + " not exist!");
			}
		} catch (SQLException e) {
			throw new RuntimeException("update amount faild", e);
		}
	}
	
	private void insertAcct(Connection conn, String no, String id, BigDecimal amount, long key) {
		String seq = Long.toString(key);
		try (PreparedStatement ps = conn.prepareStatement("INSERT INTO MB_ACCT(INTERNAL_KEY, CCY, ACCT_SEQ_NO, BASE_ACCT_NO, CLIENT_NO, USER_ID) VALUES(?, ?, ?, ?, ?, ?)")) {
			ps.setString(1, seq);
			ps.setString(2, "AA");
			ps.setString(3, "ABC");
			ps.setString(4, seq);
			ps.setString(5, no);
			ps.setString(6, id);
			if(ps.executeUpdate() == 0) {
				throw new RuntimeException("acct no " + id + " not exist!");
			}
		} catch (SQLException e) {
			throw new RuntimeException("update amount faild", e);
		}
	}
	
	public void trans(Connection conn, String no, BigDecimal amt) throws SQLException {
		long current = System.nanoTime();
		long shardKey = Long.parseLong(no) % 16;
		long key = Sequences.next();
		key = (key << 4) + shardKey;
		String id = getAcct(conn, no);
		select(conn, no, 5);
		BigDecimal prev = checkAmount(conn, id, amt);
		select(conn, no, 8);
		insertAcct(conn, id, no, amt, key);
		select(conn, no, 4);
		updateAmount(conn, id, prev.subtract(amt));
		select(conn, no, 5);
		insertHist(conn, "MB_TRAN_1", id, no, amt, key);
		select(conn, no, 6);
		updateLimit(conn, id, amt);
		select(conn, no, 5);
		insertHist(conn, "MB_TRAN_2", id, no, amt, key);
		select(conn, no, 6);
		updateSeq(conn, id);
		insertHist(conn, "MB_TRAN_HIST", id, no, amt, key);
	}
	
	public void trans(Connection conn, String no, BigDecimal amt, long key) throws SQLException {
		String id = getAcct(conn, no);
		select(conn, no, 5);
		checkAmount(conn, id, amt);
		select(conn, no, 8);
		insertAcct(conn, id, no, amt, key);
		select(conn, no, 4);
		updateAmount(conn, id, amt);
		select(conn, no, 5);
		insertHist(conn, "MB_TRAN_1", id, no, amt, key);
		select(conn, no, 6);
		updateLimit(conn, id, amt);
		select(conn, no, 5);
		insertHist(conn, "MB_TRAN_2", id, no, amt, key);
		select(conn, no, 6);
		updateSeq(conn, id);
		insertHist(conn, "MB_TRAN_HIST", id, no, amt, key);
	}
	
	public static void main(String[] args) {
		System.out.println(System.currentTimeMillis());
	}
}
