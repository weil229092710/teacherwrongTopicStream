package com.xuehai.utils

import java.sql.ResultSet

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xuehai.utils.{Constants, MysqlUtils, Utils}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.SolrInputDocument

/**
	* Created by root on 2020/8/24
	*/
object AppMain extends Constants {
	def main(args: Array[String]) {
		readStreamData()
	}

	def readStreamData(): Unit ={
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.enableCheckpointing(60 * 1000)//开启checkPoint，并且每分钟做一次checkPoint保存
		env.setStateBackend(new FsStateBackend(checkPointPath))
		env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
		env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
		env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, Time.of(1, TimeUnit.MINUTES)))//设置重启策略，job失败后，每隔1分钟重启一次，尝试重启100次

		val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), props)
		env.addSource(kafkaConsumer)
			.map(x => {
		//		print(x)
				try{
					JSON.parseObject(x)
				}catch {
					case e: Exception => {
						//Utils.dingDingRobot("all", "教师端错题本实时数据异常：%s, %s".format(e, x))
						log.error("教师端错题本实时数据异常：%s, \\r\\n %s".format(e, x))
						JSON.parseObject("{}")
					}
				}
			})
			.filter(_.size()>3)
  	//	.filter(_.getString("child_question_id").size==0)
  		//	.print()
    	.addSink(new HBaseSink())

		env.execute(jobName)
	}
}

/**
	* 自定义HBase Sink
	*/
class HBaseSink extends RichSinkFunction[JSONObject] with Constants{
	var connection: Connection = null
	var solrServer: CloudSolrServer = null


	override def open(parameters: Configuration): Unit = {
		// HBase连接
		// 如果没有HBase配置文件，只配置一下zk即可，因为client主要是跟zk做交互
		val configuration = new HBaseConfiguration()
		configuration.set("hbase.zookeeper.quorum", zookeeperList)
		// HBase的connection是轻连接，所以不用连接池，但是用完记得关闭
		connection = ConnectionFactory.createConnection(configuration)

		// Solr连接
		// 创建一个集群的连接，应该使用CloudSolrServer创建
		solrServer = new CloudSolrServer(solrZkHost)
	}

	override def invoke(value: JSONObject): Unit = {
		var table: Table = null
		var table1: Table = null
		var mysqljudge= -1
		var mysqlanswer=""
		try{
			val dataType = value.getString("data_type")
			val student_id = value.getString("student_id")
			val question_id = value.getString("question_id")
			val subject_id = value.getString("subject_id")
			val class_id = value.getString("class_id")
			val judge = value.getString("judge")
			//	val index = schoolId.toInt % 10

			table = connection.getTable(TableName.valueOf(HBaseTableName))
			table1= connection.getTable(TableName.valueOf(HBaseTableName1))

			val rowKey = (subject_id+question_id+class_id).reverse

			val  rowKey1=(subject_id+question_id)
			val put: Put = new Put(rowKey.getBytes)
			val put1: Put = new Put(rowKey1.getBytes)

			dataType match {
				case "0" => {//做题数据

					val question_type = value.getString("question_type")
					val code = value.getString("code")

					val answer = value.getString("answer")
					val time = Utils.str2Day(value.getString("time"))   // 时间戳转日期20200617

					val result = table.get(new Get(rowKey.getBytes))
					val result1 = table1.get(new Get(rowKey1.getBytes))

					var answer1=""
					//如果是选择题就计算答案偏向
					if(question_type=="1") {
						answer1 = Utils.null2Str(JSON.parseArray(answer).get(0).asInstanceOf[JSONObject].getString("stuReply"))
					//  answer1="B"
					}
					val sql="select judge,answered from tea_wrong where question_id= '"+question_id+"' and subject_id="+subject_id+ " and student_id="+student_id
					val sql1="INSERT INTO `tea_wrong` VALUES ( '"+question_id+"',"+ subject_id+","+ student_id+","+ judge+",'"+ answer1+"',"+time+")"
					val updateJudgeSql="UPDATE tea_wrong set judge="+judge+" where  question_id='"+question_id+"'and subject_id="+subject_id+" and student_id="+student_id
					val updateAnswersql="UPDATE tea_wrong set answered= '"+answer1+"' where  question_id='"+question_id+"'and subject_id="+subject_id+" and student_id="+student_id

					val resultSet = MysqlUtils.select(sql)

					if(result1.size()>0){ //汇总中hbase有值

						var allCount = new String(result1.getValue("info".getBytes, "all_count".getBytes)) //总人数
						var falseCount = new String(result1.getValue("info".getBytes, "false_count".getBytes)) //错误人数
						val true_count = new String(result1.getValue("info".getBytes, "true_count".getBytes)) //正确人数
						val answer_distribution=new String(result1.getValue("info".getBytes, "answer_distribution".getBytes))//答案分布falseCount
						if (!resultSet.next()) {
							//此时mysql结果中为空，将数据插入到MySQL中
							MysqlUtils.update(sql1)
						 val	allCountM=(allCount.toInt+1).toString  //总人数加1
							put1.addColumn("info".getBytes, "all_count".getBytes, allCountM.getBytes)    //总人数


							//答案分布
							//答案分布
							//答案分布
						 //mysql没有值，直接将答案分布取出来，加1放进去。



								if(answer1!="") {
									val answerObject = JSON.parseObject(answer_distribution)
									val answerValue = Utils.null20(answerObject.getString(answer1)) + 1
									answerObject.put(answer1, answerValue)
									put1.addColumn("info".getBytes, "answer_distribution".getBytes, answerObject.toJSONString.getBytes)
								}




							if(judge=="0"){ //做错
								val	falseCountM = (falseCount.toInt+1).toString //错误人数加1
								put1.addColumn("info".getBytes, "false_count".getBytes, falseCountM.getBytes)  //错误数

								val  error_rate=(falseCountM.toDouble/allCountM.toDouble).formatted("%.4f")
								//if(error_rate)
								//保留两位小数

								put1.addColumn("info".getBytes, "error_rate".getBytes, error_rate.getBytes)  //错误率
							}

							else{  //作对
								val  error_rate=(falseCount.toDouble/allCountM.toDouble).formatted("%.4f")
								//保留两位小数

								put1.addColumn("info".getBytes, "error_rate".getBytes, error_rate.getBytes)  //错误率

								val	trueCountM = (true_count.toInt+1).toString //正确人数加1
								put1.addColumn("info".getBytes, "true_count".getBytes, trueCountM.getBytes)  //正确


							}
						} else {  //mysql中结果有值
							mysqljudge = resultSet.getInt(1)
							mysqlanswer= resultSet.getString(2)
							if(mysqljudge==1&&judge=="0"){
								//mysql中的状态是对的,新来的数据是错的，需要将对的人数减去1，错的人数加上1

								val	falseCountM = (Utils.null20(falseCount)+1).toString //错误人数加1
								put1.addColumn("info".getBytes, "false_count".getBytes, falseCountM.getBytes)  //错误数


								val	trueCountM = (Utils.null20(true_count)-1).toString //正确人数减去1
								put1.addColumn("info".getBytes, "true_count".getBytes, trueCountM.getBytes)  //正确


								val  error_rate=(falseCountM.toDouble/allCount.toDouble).formatted("%.4f")
								//保留两位小数

								put1.addColumn("info".getBytes, "error_rate".getBytes, error_rate.getBytes)  //错误率
								MysqlUtils.update(updateJudgeSql)
							}

							//答案分布  之前减一，新来的加上1
							if(answer1!="") {
								if (mysqlanswer != answer1) {
									MysqlUtils.update(updateAnswersql)
									val answerObject = JSON.parseObject(answer_distribution)
									val answerValue = Utils.null20(answerObject.getString(answer1)) + 1
									val mysqlanswer1 = Utils.null20(answerObject.getString(mysqlanswer)) - 1

									answerObject.put(answer1, answerValue)
									answerObject.put(mysqlanswer, mysqlanswer1)
									if (mysqlanswer1 <= 0) {
										answerObject.remove(mysqlanswer)
									}
									put1.addColumn("info".getBytes, "answer_distribution".getBytes, answerObject.toJSONString.getBytes)
								}
							}
						}
					}
					else { //hbase中没有值
						if (!resultSet.next()) {
							//此时mysql结果中为空，将数据插入到MySQL中
							MysqlUtils.update(sql1)
						}

						if(judge=="0"){ //做错
							put1.addColumn("info".getBytes, "all_count".getBytes, "1".getBytes)
							put1.addColumn("info".getBytes, "true_count".getBytes, "0".getBytes)
							put1.addColumn("info".getBytes, "false_count".getBytes, "1".getBytes)
							put1.addColumn("info".getBytes, "error_rate".getBytes, "1".getBytes)
						}
						else {         //做对
							put1.addColumn("info".getBytes, "all_count".getBytes, "1".getBytes)
							put1.addColumn("info".getBytes, "true_count".getBytes, "1".getBytes)
							put1.addColumn("info".getBytes, "false_count".getBytes, "0".getBytes)
							put1.addColumn("info".getBytes, "error_rate".getBytes, "0".getBytes)
						}
						put1.addColumn("info".getBytes, "subject_id".getBytes, subject_id.getBytes)
						put1.addColumn("info".getBytes, "question_id".getBytes, question_id.getBytes)

                val answer_distribution = JSON.parseObject("{}")
                answer_distribution.put(answer1, 1)
						    if(answer1=="") {
									answer_distribution.remove(answer1)
								}
                put1.addColumn("info".getBytes, "answer_distribution".getBytes, answer_distribution.toJSONString.getBytes)

          }
					if(result.size()>0){    // 此rowKey有值，做数据的更新
						// HBase数据
						val codeValue = new String(result.getValue("info".getBytes, "code".getBytes))
						val maxTimeValue = new String(result.getValue("info".getBytes, "max_time".getBytes))
						if(time > maxTimeValue){
							put.addColumn("info".getBytes, "max_time".getBytes, time.getBytes)
						}
						if(codeValue==""){
							put.addColumn("info".getBytes, "code".getBytes, code.getBytes)
						}else{
							if(!codeValue.contains(code)) put.addColumn("info".getBytes, "code".getBytes, (codeValue + "," + code).getBytes) // 应用场景累加
						}

						//从hbase里面查询之前的数据
						val allCount = new String(result.getValue("info".getBytes, "all_count".getBytes)) //总人数
						var falseCount = new String(result.getValue("info".getBytes, "false_count".getBytes)) //错误人数
						val trueStudent = new String(result.getValue("info".getBytes, "true_student".getBytes)) //正确学生
						val falseStudent= new String(result.getValue("info".getBytes, "false_student".getBytes))//错误学生
           	val allCountN=(allCount.toInt+1).toString
						put.addColumn("info".getBytes, "all_count".getBytes, allCountN.getBytes)
						if(judge=="0"){  //做错
							if(falseStudent==""){
								put.addColumn("info".getBytes, "false_student".getBytes, student_id.getBytes)
							}else{
								if(!falseStudent.contains(student_id)) put.addColumn("info".getBytes, "false_student".getBytes, (falseStudent + "," + student_id).getBytes) // 如果做错中不含有，则累加
							}
							val trueStudentN = Utils.remove(trueStudent,student_id)
							put.addColumn("info".getBytes, "true_student".getBytes, trueStudentN.getBytes)
							falseCount=(falseCount.toInt+1).toString
							put.addColumn("info".getBytes, "false_count".getBytes, falseCount.getBytes)
							var  error_frequency=(falseCount.toDouble/allCountN.toDouble).formatted("%.4f")
							error_frequency=	Utils.doubletoint(error_frequency)
							put.addColumn("info".getBytes, "error_frequency".getBytes, error_frequency.getBytes)

						}
						else { //做对
							if(trueStudent==""){
								put.addColumn("info".getBytes, "true_student".getBytes, student_id.getBytes)
							}else{
								if(!trueStudent.contains(student_id)) put.addColumn("info".getBytes, "true_student".getBytes, (trueStudent + "," + student_id).getBytes) // 如果做对中不含有，则累加
							}
							val falseStudentN = Utils.remove(falseStudent,student_id)
							put.addColumn("info".getBytes, "false_student".getBytes, falseStudentN.getBytes)
							var  error_frequency=(falseCount.toDouble/allCountN.toDouble).formatted("%.4f")
							error_frequency=	Utils.doubletoint(error_frequency)
							put.addColumn("info".getBytes, "error_frequency".getBytes, error_frequency.getBytes)
						}

					}else {  // 此rowKey无值，则插入数据，进行数据的初始化
						if(judge=="0"){ //做错
							put.addColumn("info".getBytes, "error_frequency".getBytes, "1".getBytes)
							put.addColumn("info".getBytes, "false_count".getBytes, "1".getBytes)
							put.addColumn("info".getBytes, "false_student".getBytes, student_id.getBytes)
							put.addColumn("info".getBytes, "true_student".getBytes, "".getBytes)
						}
						else {         //做对
							put.addColumn("info".getBytes, "error_frequency".getBytes, "0".getBytes)
							put.addColumn("info".getBytes, "false_count".getBytes, "0".getBytes)
							put.addColumn("info".getBytes, "false_student".getBytes, "".getBytes)
							put.addColumn("info".getBytes, "true_student".getBytes, student_id.getBytes)
						}

						put.addColumn("info".getBytes, "class_id".getBytes, class_id.getBytes)
						put.addColumn("info".getBytes, "question_id".getBytes, question_id.getBytes)
						put.addColumn("info".getBytes, "subject_id".getBytes, subject_id.getBytes)
						put.addColumn("info".getBytes, "min_time".getBytes, time.getBytes)
						put.addColumn("info".getBytes, "max_time".getBytes, time.getBytes)
						put.addColumn("info".getBytes, "code".getBytes, code.getBytes)
						put.addColumn("info".getBytes, "all_count".getBytes, "1".getBytes)

					}
					//solrUpdateIndex(table, rowKey, "note", value.getString("note"))
				}


				case _ => {}
			}

			if(put.size()>0) table.put(put)
			if(put1.size()>0) table1.put(put1)
		}catch {
			case e:Exception => {
				log.error("教师端错题本实时数据写入HBase异常：%s, %s".format(e, value.toJSONString))
				//	Utils.dingDingRobot("all", "教师端错题本实时数据写入HBase异常：%s, %s".format(e, value.toJSONString ))
			}
		}finally {
			if(null != table&&null != table1){
				table.close()
				table1.close()
			}
		}
	}

	override def close(): Unit = {
		connection.close()
		solrServer.shutdown()
	}

	/**
		* 手动更新便签、攻克状态数据
		* @param table
		* @param rowKey
		* @param indexName
		* @param indexValue
		* @return
		*/
	def solrUpdateIndex(table: Table, rowKey: String, indexName: String, indexValue: String) ={
		// 设置一个defaultCollection属性
		solrServer.setDefaultCollection(solrCollection)

		val get = new Get(rowKey.getBytes)
		val result: Result = table.get(get)
		val cells: Array[Cell] = result.rawCells()
		val document  = new SolrInputDocument ()
		document.setField("id", rowKey)

		for(cell <- cells){
			val lie = new String(CellUtil.cloneQualifier(cell))
			val value = new String(CellUtil.cloneValue(cell))

			if(lie != "create_time" && lie != "last_time"){
				document.setField(lie, value)
			}
		}

		document.setField(indexName, indexValue)
		solrServer.add(document)
		solrServer.commit()
	}
}