import com.alibaba.fastjson.{JSONObject, JSONArray, JSON}

/**
  * Created by root on 2020/5/27.
  */
object test2 {
	def main(args: Array[String]) {
//		val str = "[{\"questionId\": \"5cb7e2e7468d4a0aad4b6ecf\",\"answerType\": 1,\"studentAnswer\": [{ \"index\": 0,\"proofreadResult\": 0,\"stuReply\": \"http://xhfs3.oss-cn-hangzhou.aliyuncs.com/CA103001/UploadMultiBatch/22ef805fc8b14669820eeadc6ba9ec90.json\"}]}]"
//		val json: JSONArray = JSON.parseArray(str)
//		val str1 = json.get(0).asInstanceOf[JSONObject].getString("studentAnswer")
//		val json2 = JSON.parseArray(str1)
//
//
//		println(json2.get(0).asInstanceOf[JSONObject].getString("stuReply"))
    var str="[{\"index\":0,\"inputType\":1,\"isExhibition\":false,\"isRepeatRevise\":false,\"isSeekHelp\":false,\"isShowAnswerImg\":false,\"isShowProofreadRed\":true,\"isTchRepeatRevise\":false,\"proofreadResult\":2,\"score\":-1.0,\"sectionIndex\":-1,\"sectionProofreadResult\":-1,\"stuReply\":\"D\"}]"
		val json: JSONArray = JSON.parseArray(str)
		val str1 = json.get(0).asInstanceOf[JSONObject].getString("stuReply")
		val json2 = JSON.parseArray(str1)

		JSON.parseArray(str).get(0).asInstanceOf[JSONObject].getString("stuReply")
		println(json2.get(0).asInstanceOf[JSONObject].getString("stuReply"))
	}

}
