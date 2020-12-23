import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.shaded.akka.org.jboss.netty.util.internal.StringUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Created by root on 2020/8/22 0005.
 */
public class kafka_producer {
    public static String brokerList = "192.168.5.85:9092,192.168.5.86:9092,192.168.5.87:9092";
   // public static String brokerList = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static String topicName = "question-clazz-topic";
   // public static String topicName = "test";

    public static Properties props = new Properties();

    public static void main(String[] args) throws InterruptedException {
        props.put("bootstrap.servers", brokerList);
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        mongo_kafka();


    }

    public static void mongo_kafka() throws InterruptedException {
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i=0; i<1; i++){

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, getStr(i));
            producer.send(record);

            Thread.sleep(10);
        }


        producer.close();

    }

    public static String getStr(int i){
        System.out.println(i);

        String school_id = String.valueOf(i);
        String grade_id = String.valueOf(i%3);
        String class_id = String.valueOf(333);
        String student_id = String.valueOf(i%100);
        String question_id = UUID.randomUUID().toString().replaceAll("-","");
        String child_question_id = "1,2";
        String subject_id = "2";
        String code = "2C0";
        String judge = String.valueOf(i%2);
       // String answer = "[{\"index\":0,\"inputType\":1,\"isExhibition\":false,\"isRepeatRevise\":false,\"isSeekHelp\":false,\"isShowAnswerImg\":false,\"isShowProofreadRed\":true,\"isTchRepeatRevise\":false,\"proofreadResult\":2,\"score\":-1.0,\"sectionIndex\":-1,\"sectionProofreadResult\":-1,\"stuReply\":\"BC\"}]";
      // String answer="[{\\\"index\\\":0,\\\"inputType\\\":1,\\\"isExhibition\\\":false,\\\"isRepeatRevise\\\":false,\\\"isSeekHelp\\\":false,\\\"isShowAnswerImg\\\":false,\\\"isShowProofreadRed\\\":true,\\\"isTchRepeatRevise\\\":false,\\\"proofreadResult\\\":2,\\\"score\\\":-1.0,\\\"sectionIndex\\\":-1,\\\"sectionProofreadResult\\\":-1,\\\"stuReply\\\":\\\"D\\\"}]";
        String answer= "[{\"questionId\": \"5cb7e2e7468d4a0aad4b6ecf\",\"answerType\": 1,\"studentAnswer\": [{ \"index\": 0,\"proofreadResult\": 0,\"stuReply\": \"http://xhfs3.oss-cn-hangzhou.aliyuncs.com/CA103001/UploadMultiBatch/22ef805fc8b14669820eeadc6ba9ec90.json\"}]}]";

        long time = System.currentTimeMillis();



        JSONObject json = JSON.parseObject("{}");
        // 做题数据
        json.put("school_id", "10999");
        json.put("grade_id", 1);
        json.put("class_id", class_id);
        json.put("student_id", 67528);
        //json.put("question_id", question_id);
        json.put("question_id", "59a8193c2c8afb746bdbc4f2");
        json.put("child_question_id", child_question_id);
        json.put("subject_id", subject_id);
        json.put("code", code);
        json.put("judge", 1);
        json.put("answer", answer);
        json.put("time", time);
       // json.put("time", "20200827");
        json.put("data_type", 0);
        json.put("question_type", "1");

        // 便签数据
//        json.put("school_id", "10");
//        json.put("student_id", "1");
//        json.put("question_id", "80d00e139170459c9873e0c2623cd639");
//        json.put("subject_id", "0");
//        json.put("note", "url888");
//        json.put("data_type", 1);

        // 攻克数据
//        json.put("school_id", "10");
//        json.put("student_id", "1");
//        json.put("question_id", "80d00e139170459c9873e0c2623cd639");
//        json.put("subject_id", "0");
//        json.put("conquer_status", "1");
//        json.put("data_type", 2);

        return json.toJSONString();
    }
}
