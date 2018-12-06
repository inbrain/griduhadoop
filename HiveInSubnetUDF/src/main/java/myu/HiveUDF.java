package myu;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

// Description of the UDF
//@Description(
//        name="HiveUDF",
//        value="returns a lower case version of the input string.",
//        extended="select HiveUDF(deviceplatform) from hivesampletable limit 10;"
//)
public class HiveUDF extends UDF {
    public Boolean evaluate(String subnet, String ip) {
        return new SubnetUtils(subnet).getInfo().isInRange(ip);
    }
}