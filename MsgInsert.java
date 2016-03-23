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

public class MsgInsert {

	static String SpecCode = "\"\"++--++\"\"";

	public static int[] countStr(String str1, String str2) {
		int count = 0;
		int location = 0;
		for (int z = 0; z <= str1.length() - str2.length(); z++) {
			if ((str1.substring(z, z + str2.length())).equals(str2)) {
				count++;
				location = z + str2.length();
			}
		}
		int[] a = { count, location };
		return a;
	}

	public static int[] EachStr(String str1, String str2) {
		int[] index = new int[7];
		int count = 0;
		for (int z = 0; z <= str1.length() - str2.length(); z++) {
			if (str1.substring(z, z + str2.length()).equals(str2))
				index[count++] = z;
		}
		return index;
	}

	public static void checkFK(Connection conn, String fkstr, int fkid, int type)
			throws SQLException {
		ResultSet rs = null;

		String sql1s = "select * from forum_type where idforum_type='" + fkstr
				+ "'";
		String sql1i = "insert into forum_type(idforum_type) values(?)";

		String sql2s = "select * from iuser_2010_2014 where id_key='" + fkid
				+ "'";
		String sql2i = "insert into iuser_2010_2014(id_key) values(?)";

		String sql3s = "select * from status where idstatus='" + fkstr + "'";
		String sql3i = "insert into status(idstatus) values(?)";

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
					pstmt.setInt(1, fkid);
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
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static String[] Columns(String record) {
		String str1, str2;
		String[] columns = new String[13];
		int[] indexs = EachStr(record, SpecCode);
		str1 = record.substring(0, indexs[5] + SpecCode.length());
		str2 = record.substring(indexs[5] + SpecCode.length());

		if (str1.substring(0, indexs[0] - 1).equals((str1.split(",")[0]))
				&& indexs[6] == 0 && (str2.split(",")).length == 10) {
			columns[0] = str1.substring(0, indexs[0] - 1);
			columns[1] = str1.substring(indexs[0] + SpecCode.length(),
					indexs[1]);
			columns[2] = str1.substring(indexs[2] + SpecCode.length(),
					indexs[3]);
			columns[3] = str1.substring(indexs[4] + SpecCode.length(),
					indexs[5]);
			columns[4] = (str2.split(","))[1];
			columns[5] = (str2.split(","))[2];
			columns[6] = (str2.split(","))[3];
			columns[7] = (str2.split(","))[4];
			columns[8] = (str2.split(","))[5];
			columns[9] = (str2.split(","))[6];
			columns[10] = (str2.split(","))[7];
			columns[11] = (str2.split(","))[8];
			columns[12] = (str2.split(","))[9];
		} else
			columns = null;
		return columns;
	}

	public static boolean Strtoint(String str) throws NumberFormatException {
		try {
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public static void InsertSQL(Connection conn, String[] columns)
			throws SQLException {
		try {
			PreparedStatement pstmt = conn
					.prepareStatement("insert into messages_2010_2014 (messageid,messageauthor,messagetitle,messagebody,messagedate,lastpost,replies,forum_type,authorid,page_view,status,marks_good,marks_bad) values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
			if (!Strtoint(columns[0]))
				columns[0] = columns[0].substring(1);
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
			pstmt.setString(11, columns[10]);
			pstmt.setInt(12, Integer.parseInt(columns[11]));
			pstmt.setInt(13, Integer.parseInt(columns[12]));
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		Driver driver = new com.mysql.jdbc.Driver();
		String url = "jdbc:mysql://localhost:3306/gaodeng?characterEncoding=utf8";
		Properties info = new Properties();
		info.put("user", "root");
		info.put("password", "123");
		Connection conn = null;
		String record = "";
		int counta = 0;
		int countb = 0;
		String line = null;
		int i = 0;
		String[] files = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
				"k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
				"w", "x", "y", "z" };
		String[] filename = new String[312];
		for (int f1 = 0; f1 < 12; f1++) {
			for (int f2 = 0; f2 < 26; f2++) {
				filename[i++] = "x" + files[f1] + files[f2];
			}
		}
		for (int filecount = 0; filecount < 312; filecount++) {
			String filePath = "/Users/xdeveloper/Desktop/gaodeng/msg/"
					+ filename[filecount];
			BufferedReader bufferedReader = null;
			try {
				conn = driver.connect(url, info);
				if (conn != null) {
					bufferedReader = new BufferedReader(new InputStreamReader(
							new FileInputStream(filePath), "UTF-8"));
					while ((line = bufferedReader.readLine()) != null && i < 20) {
						//System.out.println(line);
						if (line.length() == 0) {
							record = record + "\n";
							continue;
						} else {
							int[] array1 = countStr(line, SpecCode);
							counta = counta + array1[0];
							if (counta == 6) {
								int[] array2 = countStr(
										line.substring(array1[1]), ",");
								countb = countb + array2[0];
								if (countb == 9) {
									record = record + line;
									String[] columns = Columns(record);
									if (columns == null)
										System.out.println("W" + line);
									else {
										// System.out.println(i + " " + record);
										checkFK(conn, columns[7], 0, 0);
										checkFK(conn, null,
												Integer.parseInt(columns[8]), 1);
										checkFK(conn, columns[10], 0, 2);
										InsertSQL(conn, columns);
										counta = 0;
										countb = 0;
										record = "";
									}
								} else {
									if (countb < 9) {
										record = record + line;
										continue;
									} else {
										record = record + line;
										System.out.println("bad1 " + record);
										counta = 0;
										countb = 0;
										record = "";
									}
								}
							} else {
								if (counta < 6) {
									record = record + line;
									continue;
								} else {
									record = record + line;
									System.out.println("bad2 " + record);
									counta = 0;
									countb = 0;
									record = "";
									break;
								}
							}
						}
						i++;
					}

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
			System.out.println("success " + filename[filecount]);
		}
	}
}
