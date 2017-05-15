import java.util.regex.{Pattern, Matcher}

/**
  * Created by cluster on 2016/12/15.
  */
object Pattern_Test {
  def user_fansNum_alis(fansNumStr: String): String = {
    var user_fansNum = ""
    val p: Pattern = Pattern.compile("\\d+")
    val str: Array[String] = p.split(fansNumStr)
    if (str.length == 2) {
      val reg = "[\u4e00-\u9fa5]"
      val p1: Pattern = Pattern.compile(reg)
      val m: Matcher = p1.matcher(fansNumStr)
      val repickStr = m.replaceAll("")
      user_fansNum = repickStr + "0000"
    } else {
      user_fansNum = fansNumStr
    }
    user_fansNum
  }
  def main(args: Array[String]): Unit = {
    val aa = user_fansNum_alis("456")
    println(aa)
  }
    /**
      * 将字符串中的汉字转换成阿拉伯数字
      */
//    var user_fansNum = ""
//    val user_fansNum_ = "123"
//    val p: Pattern = Pattern.compile("\\d+")
//    val str: Array[String] = p.split(user_fansNum_)
//    if (str.length == 2){
//      val reg = "[\u4e00-\u9fa5]"
//      val p1: Pattern = Pattern.compile(reg)
//      val m: Matcher = p1.matcher(user_fansNum_)
//      val repickStr = m.replaceAll("")
//      user_fansNum = repickStr+"0000"
//    }else{
//      user_fansNum = user_fansNum_
//    }
//
//    user_fansNum
    /**
      * 去掉数字后面的汉字
      */
    //    val p: Pattern = Pattern.compile("\\d+")
    //    val m: Matcher = p.matcher("123万")
    //    m.find()
    //    m.start()
    //    m.end()
    //    //m.group();//返回2223
    //    print(m.group())
    /**
      * 得到字符串中的阿拉伯数字
      */
//    val str = "123万"
//    val reg = "[\u4e00-\u9fa5]"
//    val p1: Pattern = Pattern.compile(reg)
//    val m: Matcher = p1.matcher(str)
//    val repickStr = m.replaceAll("")
//    println(repickStr)  //得到123


}
