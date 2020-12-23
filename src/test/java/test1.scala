import java.util

import com.alibaba.fastjson.JSON
import com.xuehai.utils.{Constants, PropertiesUtil}
import org.apache.hadoop.hbase.{CellUtil, Cell, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.SolrDocumentList
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
  * Created by root on 2020/5/7.
  */
object test1 extends Constants{
	def main(args: Array[String]): Unit = {
		var solrServer: CloudSolrServer = null
		// Solr连接
		// 创建一个集群的连接，应该使用CloudSolrServer创建
		solrServer = new CloudSolrServer(solrZkHost)

		func(solrServer, "0")
		func(solrServer, "1")
		func(solrServer, "2")
		func(solrServer, "3")
		func(solrServer, "4")
		func(solrServer, "5")
		func(solrServer, "6")
		func(solrServer, "7")
		func(solrServer, "8")
		func(solrServer, "9")
	}

	def func(solrServer: CloudSolrServer, index: String): Unit ={
		solrServer.setDefaultCollection(solrCollection + index)

		val query = new SolrQuery() // 查询对象
		query.setQuery("*:*")

		val response = solrServer.query(query) // 请求查询
		val docs: SolrDocumentList = response.getResults()   // 查询结果
		println("查询文档总数" + docs.getNumFound()) // 查询文档总数
	}

}
