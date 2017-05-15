package sparkSeg;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * Created by cluster on 2016/12/10.
 * 获取微博评论的所有数据
 * 放入一个文件中
 */
public class TextSum {

    /**
     * 写入字符串文件
     *
     */

    public static void writeLog(String str)
    {
        try
        {
            String path="src/com/magicstudio/spark/text/Sum.txt";
            File file=new File(path);
            if(!file.exists())
                file.createNewFile();
            FileOutputStream out=new FileOutputStream(file,true); //如果追加方式用true
            StringBuffer sb=new StringBuffer();
            sb.append(str+"\n");
            out.write(sb.toString().getBytes("utf-8"));//注意需要转换对应的字符集
            out.close();
        }
        catch(IOException ex)
        {
            System.out.println(ex.getStackTrace());
        }
    }


    public static void main(String[] args) {

        Connection con;
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.31.7:3306/qczjtest?useUnicode=true&characterEncoding=UTF-8";
        String user = "root";
        String password = "dp12345678";
        //遍历查询结果集
        try {
            Class.forName(driver);
            con = (Connection) DriverManager.getConnection(url,user,password);
            PreparedStatement psql = null;

            //String sql = "SELECT * FROM weibo_content2";
            String sql = "SELECT * FROM autohome_koubei";
            //String insert_sql = "UPDATE weibo_content2 SET emotion = ? WHERE id = ?";
            psql = (PreparedStatement) con.prepareStatement(sql);
            ResultSet rs = psql.executeQuery();
            while(rs.next()){
                String text = rs.getString("content");
                writeLog(text);
                System.out.println(text);
            }
            rs.close();
            con.close();
        } catch(ClassNotFoundException e) {
            //数据库驱动类异常处理
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        } catch(SQLException e) {
            //数据库连接失败异常处理
            e.printStackTrace();
        }catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }finally{
            System.out.println("数据库数据成功获取！！");
        }

    }
}
