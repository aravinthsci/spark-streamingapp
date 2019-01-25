package com.streamapp.cassandra

import com.datastax.driver.core.{ResultSet, Session}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession
import com.streamapp.model.ProductSale

object Satement {
  def getConnector(spark: SparkSession): CassandraConnector ={
    val connector = CassandraConnector.apply(spark.sparkContext.getConf)
    connector
  }

  private def cql_update(Id: String, firstName: String, lastName: String, house: String, street: String, city: String, state: String, zip: String, prod: String, tag: String): String =
    s"""insert into test.product(id,firstname,lastname,house,street,city,state,zip,prod,tag) values ('$Id','$firstName','$lastName','$house','$street','$city','$state','$zip','$prod','$tag')""".stripMargin

  def updateProvinceSale(connector: CassandraConnector, value: ProductSale): ResultSet = {
    connector.withSessionDo { session =>
      session.execute(cql_update(value.Id, value.firstName, value.lastName, value.house, value.street, value.city, value.state, value.zip, value.prod, value.tag))
    }
  }

}