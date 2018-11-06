package it.agilelab.darwin.connector.mock

import com.typesafe.config.Config
import it.agilelab.darwin.common.Connector
import org.apache.avro.Schema

import scala.collection.mutable

class MockConnector(config: Config) extends Connector(config) {

  val _table: mutable.Map[Long, Schema] = {
    val alone = parseResource("test/MockClassAlone.avsc")
    val parent = parseResource("test/MockClassParent.avsc")
    mutable.Map(0L -> alone, 1L -> parent)
  }

  override def fullLoad(): Seq[(Long, Schema)] = _table.toSeq

  override def insert(schemas: Seq[(Long, Schema)]): Unit = {
    schemas.foreach { case(id, schema) =>
      _table(id) = schema
    }
  }

  protected def parseResource(path: String): Schema = {
    val p = new Schema.Parser()
    p.parse(getClass.getClassLoader.getResourceAsStream(path))
  }
}
