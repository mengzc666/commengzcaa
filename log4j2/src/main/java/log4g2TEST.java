import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Mengzc
 * @create 2021-03-27 22:34
 */
public class log4g2TEST {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(log4g2TEST.class);
        //下面语句会根据log4j2.xml中的日志级别输出

        for (int i=0;i<10;i++){
            log.warn("debug信息))))KKKKBJBJHJ");
        }
      for (int i=0;i<100000;i++){
          log.debug("debug信息");
          log.info("info信息");
          log.warn("warn信息");
          log.error("error信息");
      }



    }



}
