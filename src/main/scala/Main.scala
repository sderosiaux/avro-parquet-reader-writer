import java.io.{IOException, File, ByteArrayOutputStream}
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.SchemaBuilder
import parquet.avro.{AvroParquetReader, AvroParquetWriter}
import scala.util.control.Breaks.break

object Main {
  def main(args: Array[String]) {
    // Build a schema
    val schema = SchemaBuilder
      .record("person")
      .fields
      .name("name").`type`().stringType().noDefault()
      .name("ID").`type`().intType().noDefault()
      .endRecord

    // Build an object conforming to the schema
    val user1 = new GenericRecordBuilder(schema)
      .set("name", "Jeff")
      .set("ID", 1)
      .build

    // JSON encoding of the object (a single record)
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val baos = new ByteArrayOutputStream
    val jsonEncoder = EncoderFactory.get.jsonEncoder(schema, baos)
    writer.write(user1, jsonEncoder)
    jsonEncoder.flush
    println("JSON encoded record: " + baos)

    // binary encoding of the object (a single record)
    baos.reset
    val binaryEncoder = EncoderFactory.get.binaryEncoder(baos, null)
    writer.write(user1, binaryEncoder)
    binaryEncoder.flush
    println("Binary encoded record: " + baos.toByteArray)

    // Build another object conforming to the schema
    val user2 = new GenericRecordBuilder(schema)
      .set("name", "Sam")
      .set("ID", 2)
      .build

    // Write both records to an Avro object container file
    val file = new File("users.avro")
    //file.deleteOnExit
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(schema, file)
    dataFileWriter.append(user1)
    dataFileWriter.append(user2)
    dataFileWriter.close

    // Read the records back from the file
    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](file, datumReader)
    var user: GenericRecord = null;
    while (dataFileReader.hasNext) {
      user = dataFileReader.next(user)
      println("Read user from Avro file: " + user)
    }

    // Write both records to a Parquet file
    val tmp = File.createTempFile(getClass.getSimpleName, ".tmp")
    tmp.delete()
    println(tmp.getAbsolutePath)
    val tmpParquetFile = new org.apache.hadoop.fs.Path(tmp.getPath)
    val parquetWriter = new AvroParquetWriter[GenericRecord](tmpParquetFile, schema)
    parquetWriter.write(user1)
    parquetWriter.write(user2)
    parquetWriter.close

    // Read both records back from the Parquet file
    val parquetReader = new AvroParquetReader[GenericRecord](tmpParquetFile)
    while (true) {
      Option(parquetReader.read) match {
        case Some(matchedUser) => println("Read user from Parquet file: " + matchedUser)
        case None => println("Finished reading Parquet file"); break
      }
    }
  }
}