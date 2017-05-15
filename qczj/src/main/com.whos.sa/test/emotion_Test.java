package test;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import analysis.Analysis;
import util.log.LogUtil;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * Created by cluster on 2016/12/10.
 * 情感分析
 */
public class emotion_Test {
    public static void main(String[] args) {
        LogUtil.getInstance().logInfo("Test SA");
        Analysis analysis = new Analysis();
        //mysql连接
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

            String sql = "SELECT * FROM weibo_content2";
            String insert_sql = "UPDATE weibo_content2 SET emotion = ? WHERE id = ?";
            psql = (PreparedStatement) con.prepareStatement(sql);
            ResultSet rs = psql.executeQuery();
            while(rs.next()){
                String emotion;
                int id = rs.getInt("id");
                System.out.println(id);
                String text = rs.getString("text");
                System.out.println(text);
                //分析
                int em = analysis.parse(text).getCode();
                if (em == 1){
                    emotion = "正面";
                }else if (em == 0){
                    emotion = "中性";
                }else {
                    emotion = "负面";
                }
                System.out.println(emotion);
                psql = (PreparedStatement) con.prepareStatement(insert_sql);
                psql.setString(1,emotion);
                psql.setInt(2,id);
                psql.executeUpdate();
                //psql.executeUpdate();
                System.out.println("---------------------------------------");
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
