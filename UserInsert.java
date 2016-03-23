package UserMsgRls;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.jdbc.Statement;

public class UserInsert {
	static String SpecCode = "\"\"++--++\"\"";

	public static void checkFK(Connection conn, String fkstr, int fkid, int type)
			throws SQLException {
		ResultSet rs = null;

		String sql1s = "select * from country where idcountry='" + fkstr + "'";
		String sql1i = "insert into country(idcountry) values(?)";

		String sql2s = "select * from gender where idgender='" + fkstr + "'";
		String sql2i = "insert into gender(idgender) values(?)";

		String sql3s = "select * from mstatus where idmstatus='" + fkstr + "'";
		String sql3i = "insert into mstatus(idmstatus) values(?)";

		String sql4s = "select * from spend_grp where idspend_grp='" + fkid
				+ "'";
		String sql4i = "insert into spend_grp(idspend_grp) values(?)";

		String sql5s = "select * from education_level where ideducation_level='"
				+ fkid + "'";
		String sql5i = "insert into education_level(ideducation_level) values(?)";

		String sql6s = "select * from job_nature where idjob_nature='" + fkid
				+ "'";
		String sql6i = "insert into job_nature(idjob_nature) values(?)";

		String sql7s = "select * from income_level where idincome_level='"
				+ fkid + "'";
		String sql7i = "insert into income_level(idincome_level) values(?)";

		String sql8s = "select * from status where idstatus='" + fkstr + "'";
		String sql8i = "insert into status(idstatus) values(?)";

		Statement stmt = (Statement) conn.createStatement();
		try {
			switch (type) {
			case 0:
				rs = stmt.executeQuery(sql1s);
				if (!rs.next()) {
					PreparedStatement pstmt = conn.prepareStatement(sql1i);
					pstmt.setString(1, fkstr);
					pstmt.executeUpdate();
				}
				break;
			case 1:
				rs = stmt.executeQuery(sql2s);
				if (!rs.next()) {
					PreparedStatement pstmt = conn.prepareStatement(sql2i);
					pstmt.setString(1, fkstr);
					pstmt.executeUpdate();
				}
				break;
			case 2:
				rs = stmt.executeQuery(sql3s);
				if (!rs.next()) {
					PreparedStatement pstmt = conn.prepareStatement(sql3i);
					pstmt.setString(1, fkstr);
					pstmt.executeUpdate();
				}
				break;
			case 3:
				rs = stmt.executeQuery(sql4s);
				if (!rs.next()) {
					PreparedStatement pstmt = conn.prepareStatement(sql4i);
					pstmt.setInt(1, fkid);
					pstmt.executeUpdate();
				}
				break;
			case 4:
				rs = stmt.executeQuery(sql5s);
				if (!rs.next()) {
					PreparedStatement pstmt = conn.prepareStatement(sql5i);
					pstmt.setInt(1, fkid);
					pstmt.executeUpdate();
				}
				break;
			case 5:
				rs = stmt.executeQuery(sql6s);
				if (!rs.next()) {
					PreparedStatement pstmt = conn.prepareStatement(sql6i);
					pstmt.setInt(1, fkid);
					pstmt.executeUpdate();
				}
				break;
			case 6:
				rs = stmt.executeQuery(sql7s);
				if (!rs.next()) {
					PreparedStatement pstmt = conn.prepareStatement(sql7i);
					pstmt.setInt(1, fkid);
					pstmt.executeUpdate();
				}
				break;
			case 7:
				rs = stmt.executeQuery(sql8s);
				if (!rs.next()) {
					PreparedStatement pstmt = conn.prepareStatement(sql8i);
					pstmt.setString(1, fkstr);
					pstmt.executeUpdate();
				}
				break;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static int[] EachStr(String str1, String str2) {
		int[] index = new int[3];
		int count = 0;
		for (int z = 0; z < str1.length() - str2.length(); z++) {
			if (str1.substring(z, z + str2.length()).equals(str2))
				index[count++] = z;
		}
		return index;
	}

	public static String[] Columns(String record) {
		String str1, str2;
		String[] columns = new String[13];
		int[] indexs = EachStr(record, SpecCode);
		str1 = record.substring(0, indexs[1] + SpecCode.length());
		str2 = record.substring(indexs[1] + SpecCode.length());

		if (str1.substring(0, indexs[0] - 1).equals((str1.split(",")[0]))
				&& (str2.split(",")).length == 12) {
			columns[0] = str1.substring(0, indexs[0] - 1);
			columns[1] = str1.substring(indexs[0] + SpecCode.length(),
					indexs[1]);
			columns[2] = (str2.split(","))[1];
			columns[3] = (str2.split(","))[2];
			columns[4] = (str2.split(","))[3];
			columns[5] = (str2.split(","))[4];
			columns[6] = (str2.split(","))[5];
			columns[7] = (str2.split(","))[6];
			columns[8] = (str2.split(","))[7];
			columns[9] = (str2.split(","))[8];
			columns[10] = (str2.split(","))[9];
			columns[11] = (str2.split(","))[10];
			columns[12] = (str2.split(","))[11];
		} else
			columns = null;
		return columns;
	}

	public static void InsertSQL(Connection conn, String[] columns)
			throws SQLException {
		try {
			PreparedStatement pstmt = conn
					.prepareStatement("insert into iuser_2010_2014 (id_key,nickname,country,gender,mstatus,dob,spend_grp,online_grp,education_level,job_nature,income_level,date_join,status) values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
			pstmt.setInt(1, Integer.parseInt(columns[0]));
			pstmt.setString(2, columns[1]);
			pstmt.setString(3, columns[2]);
			pstmt.setString(4, columns[3]);
			pstmt.setString(5, columns[4]);
			pstmt.setString(6, columns[5]);
			pstmt.setInt(7, Integer.parseInt(columns[6]));
			pstmt.setString(8, columns[7]);
			pstmt.setInt(9, Integer.parseInt(columns[8]));
			pstmt.setInt(10, Integer.parseInt(columns[9]));
			pstmt.setInt(11, Integer.parseInt(columns[10]));
			pstmt.setString(12, columns[11]);
			pstmt.setString(13, columns[12]);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {

		/*
		 * 1.Create an object of Class to implement; 2.Information
		 * aboutConnection; 3.Invoke Driver API to connect mysql;
		 */
		Driver driver = new com.mysql.jdbc.Driver();
		String url = "jdbc:mysql://localhost:3306/gaodeng?characterEncoding=utf8";
		Properties info = new Properties();
		info.put("user", "root");
		info.put("password", "123");
		Connection conn = null;
		String filePath = "/Users/xdeveloper/Desktop/gaodeng/replies/xaaaaa";
		BufferedReader bufferedReader = null;
		try {
			conn = driver.connect(url, info);
			if (conn != null) {
				bufferedReader = new BufferedReader(new InputStreamReader(
						new FileInputStream(filePath), "UTF-8"),
						5 * 1024 * 1024);
				String line = null;
				int i = 0;
				while ((line = bufferedReader.readLine()) != null) {
					if (EachStr(line, SpecCode)[2] == 0
							&& Columns(line) != null) {
						String[] columns = Columns(line);
						checkFK(conn, columns[2], 0, 0);
						checkFK(conn, columns[3], 0, 1);
						checkFK(conn, columns[4], 0, 2);
						checkFK(conn, null, Integer.parseInt(columns[6]), 3);
						checkFK(conn, null, Integer.parseInt(columns[8]), 4);
						checkFK(conn, null, Integer.parseInt(columns[9]), 5);
						checkFK(conn, null, Integer.parseInt(columns[10]), 6);
						checkFK(conn, columns[12], 0, 7);
						InsertSQL(conn, columns);
						// System.out.println("Right " + i);
					} else
						System.out.println("Wrong " + i);
					i++;
				}
				System.out.println("Successful");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
				bufferedReader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
