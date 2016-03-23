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

public class RepliesInsert {

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

	public static String[] Columns(String record) {
		String str1, str2;
		String[] columns = new String[8];
		int[] indexs = EachStr(record, SpecCode);
		str1 = record.substring(0, indexs[5] + SpecCode.length());
		str2 = record.substring(indexs[5] + SpecCode.length());

		if (indexs[6] == 0 && (str2.split(",")).length == 6) {
			columns[0] = str1.substring(indexs[0] + SpecCode.length(),
					indexs[1]);
			columns[1] = str1.substring(indexs[2] + SpecCode.length(),
					indexs[3]);
			columns[2] = str1.substring(indexs[4] + SpecCode.length(),
					indexs[5]);
			columns[3] = (str2.split(","))[1];
			columns[4] = (str2.split(","))[2];
			columns[5] = (str2.split(","))[3];
			columns[6] = (str2.split(","))[4];
			columns[7] = (str2.split(","))[5];
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
					.prepareStatement("insert into replies_2014(messageauthor,messagetitle,messagebody,messagedate,replyid,replyparent,status,authorid) values (?,?,?,?,?,?,?,?)");
			pstmt.setString(1, columns[0]);
			pstmt.setString(2, columns[1]);
			pstmt.setString(3, columns[2]);
			pstmt.setString(4, columns[3]);
			if (!Strtoint(columns[4]))
				columns[4] = columns[4].substring(1);
			pstmt.setInt(5, Integer.parseInt(columns[4]));
			pstmt.setInt(6, Integer.parseInt(columns[5]));
			pstmt.setString(7, columns[6]);
			pstmt.setInt(8, Integer.parseInt(columns[7]));
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		Driver driver = new com.mysql.jdbc.Driver();
		String url = "jdbc:mysql://127.0.0.1:3306/gaodeng?characterEncoding=utf8&autoReconnect=true&useSSL=false";
		Properties info = new Properties();
		info.put("user", "root");
		info.put("password", "123");
		Connection conn = null;
		String record = "";
		int counta = 0;
		int countb = 0;
		int j = 0;
		String line = null;
		BufferedReader bufferedReader = null;
		String[] files = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
				"k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
				"w", "x", "y", "z" };
		String[] filename = new String[2704];
		for (int f1 = 0; f1 < 4; f1++) {
			for (int f2 = 0; f2 < 26; f2++) {
				for (int f3 = 0; f3 < 26; f3++) {
					filename[j++] = "x" + files[f1] + files[f2] + files[f3];
				}
			}
		}
		for (int filecount = 2000; filecount < 2599; filecount++) {
			String filePath = "/Users/xdeveloper/Desktop/gaodeng/"
					+ filename[filecount];

			try {
				conn = driver.connect(url, info);
				if (conn != null) {
					bufferedReader = new BufferedReader(new InputStreamReader(
							new FileInputStream(filePath), "UTF-8"),
							5 * 1024 * 1024);
					int i = 0;
					while ((line = bufferedReader.readLine()) != null) {
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
								if (countb == 5) {
									record = record + line;
									String[] columns = Columns(record);
									if (columns == null)
										System.out.println("W" + line);
									else {
										InsertSQL(conn, columns);
										counta = 0;
										countb = 0;
										record = "";
									}
								} else {
									if (countb < 5) {
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
