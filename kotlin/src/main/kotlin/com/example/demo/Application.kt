package com.example.demo

import example.avro.User
import example.avro.User2
import java.io.File
import java.lang.Exception
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.message.BinaryMessageDecoder
import org.apache.avro.message.SchemaStore
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.avro.AvroReadSupport

fun main(args: Array<String>) {
    // Set up some Avro models
    // The User model was generated via the Gradle Avro Plugin (https://github.com/davidmc24/gradle-avro-plugin)
    // from the `src/main/avro/User.avsc` schema file
    println("### SETTING UP DATA ###")

    val user1 = User()
    user1.setName("Alyssa")
    user1.favoriteNumber = 256
    // Leave favorite color null

    // Alternate constructor
    val user2 = User("Ben", 7, "red")

    // Construct via builder
    val user3 = User.newBuilder()
        .setName("Charlie")
        .setFavoriteColor("blue")
        .setFavoriteNumber(null)
        .build()

    println(user1)
    println(user2)
    println(user3)

    // Serializing to disk will write the schema directly into the file
    // If you inspect the `users.avro` file, you'll see the schema in text, followed by the records encoded as binary data
    println("")
    println("### SERIALIZING TO DISK, DESERIALIZING FROM DISK ###")

    // Serialize user1, user2 and user3 to disk
    val userDatumWriter: DatumWriter<User> = SpecificDatumWriter(User::class.java)
    val dataFileWriter = DataFileWriter(userDatumWriter)
    dataFileWriter.create(user1.schema, File("users.avro"))
    dataFileWriter.append(user1)
    dataFileWriter.append(user2)
    dataFileWriter.append(user3)
    dataFileWriter.close()

    // Deserialize Users from disk
    // Reading the Avro file from disk, we know the writer schema, which was written directly into the file itself
    // We also specify the reader schema as `User`, as the model in which we want to map the written data into.
    val userDatumReader: DatumReader<User> = SpecificDatumReader(User::class.java)
    val dataFileReader: DataFileReader<User> = DataFileReader<User>(File("users.avro"), userDatumReader)
    var user: User? = null
    while (dataFileReader.hasNext()) {
        // Reuse user object by passing it to next(). This saves us from
        // allocating and garbage collecting many objects for files with
        // many items.
        user = dataFileReader.next(user)
        println(user)
    }

    // The Avro spec also describes a way to encode a single object along with a fingerprint of its schema (instead of
    // the full schema). This was designed for messaging system use cases, where often, the schema itself is too large
    // to include in every single message.
    //
    // For more information, see:
    // - Avro spec: https://avro.apache.org/docs/current/spec.html#minitoc-area:~:text=effectively.-,Single%20object%20encoding%20specification
    // - Java usage example: https://github.com/apache/avro/blob/master/lang/java/avro/src/test/java/org/apache/avro/message/TestBinaryMessageEncoding.java
    println("")
    println("### SINGLE OBJECT ENCODING / DECODING ###")

    // Serialize and deserialize single messages as bytes
    println("")
    println("ENCODE AND DECODE THE SAME THING")
    val encoder = User.getEncoder()
    val decoder = User.getDecoder()
    val encoded = encoder.encode(user1)
    val decoded = decoder.decode(encoded)
    println(encoded)
    println(decoded)

    // As the writer schema itself isn't available alongside a message, we are not able to decode the message unless
    // we get the writer schema. Decoding will fail because the writer schema is not known.
    //
    // The following code illustrates this case. It will throw this exception:
    // org.apache.avro.message.MissingSchemaException: Cannot resolve schema for fingerprint: -3588479540582100558
    println("")
    println("DECODE TO NEW READER SCHEMA WITH UNKNOWN WRITER SCHEMA")

    val schema2 = SchemaBuilder.record("User").fields()
        .requiredString("name")
        .endRecord()
    val decoder2 = BinaryMessageDecoder<GenericData.Record>(GenericData.get(), schema2)

    try {
        val decodedWithDifferentSchema = decoder2.decode(encoded)
        println(decodedWithDifferentSchema)
    } catch (e: Exception) {
        println(e)
    }

    // Now let's tell the decoder know about the writer schema, and try decoding again. The decoder can now find a
    // schema that matches the schema fingerprint included in the message. Decoding should work now.
    println("")
    println("DECODE TO NEW READER SCHEMA WITH UNKNOWN WRITER SCHEMA, AFTER LETTING DECODER KNOW ABOUT THE SCHEMA")

    decoder2.addSchema(User.getClassSchema())
    val decodedWithDifferentSchema = decoder2.decode(encoded)
    println(decodedWithDifferentSchema)

    // Finding a schema when we only have a schema fingerprint is what we're interested in. It is abstracted as the
    // `SchemaStore` provider interface. The main purpose of a schema store is to look up writer schema given a schema
    // fingerprint.
    //
    // We can implement our own `SchemaStore` as necessary - we just need to implement the `findByFingerprint` function.
    //
    // See:
    // https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/message/SchemaStore.java
    println("")
    println("### USING SCHEMA STORE TO LOOK UP UNKNOWN WRITER SCHEMAS ###")

    println("")
    println("DECODE TO A GENERIC RECORD WITH READER SCHEMA DIFFERENT THAN WRITER SCHEMA")

    // Here, we just use the built in `SchemaScore.Cache` impl to simulate having a schema in the schema store.
    val schemaStore = SchemaStore.Cache()
    schemaStore.addSchema(User.getClassSchema())

    val decoder3 = BinaryMessageDecoder<GenericData.Record>(GenericData.get(), schema2, schemaStore)
    println(decoder3.decode(encoded))

    println("")
    println("DECODE TO A GENERATED CLASS")
    // try the same as above, but decoding to a generated class
    val schemaStore2 = SchemaStore.Cache()
    schemaStore2.addSchema(User.getClassSchema())

    val decoder4 = User2.createDecoder(schemaStore2)
    println(decoder4.decode(encoded))

    println("")
    println("DECODE TO A GENERIC RECORD WITH READER SCHEMA FROM WRITER SCHEMA")
    // After we've found the writer schema, we can even use the writer schema as a reader schema
    val schemaStore3 = SchemaStore.Cache()
    schemaStore3.addSchema(User.getClassSchema())
    val decoder5 = BinaryMessageDecoder<GenericData.Record>(GenericData.get(), null, schemaStore3)
    println(decoder5.decode(encoded))

    // To optimize large scale data analysis use cases, Avro can be converted into Parquet files.
    //
    // For more usage examples, see:
    // https://github.com/apache/parquet-mr/blob/master/parquet-avro/src/test/java/org/apache/parquet/avro/TestReadWrite.java
    // https://github.com/CohesionForce/avroToParquet
    println("")
    println("### CONVERT AVRO TO PARQUET ###")

    // TODO: not totally sure about the significance of these, yet
    val parquetConf = Configuration()
    parquetConf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)
    parquetConf.setBoolean("parquet.avro.add-list-element-records", false)
    parquetConf.setBoolean("parquet.avro.write-old-list-structure", false)

    val parquetPath = Path(File("users.parquet").toURI())
    val parquetWriter = AvroParquetWriter
        // .builder<GenericData.Record>(parquetPath)
        .builder<GenericRecord>(parquetPath)
        .withSchema(User.getClassSchema())
        .withConf(parquetConf)
        .build()
    val decodedUser = decoder5.decode(encoded)
    parquetWriter.write(decodedUser)
    parquetWriter.close()

    // TODO: resolve dependencies to fix this
    // val parquetPath2 = Path(File("users.parquet").toURI())
    // val avroParquetReader = AvroParquetReader
    //     .builder<GenericRecord>(parquetPath2)
    //     .withConf(parquetConf)
    //     .build()
    //
    // val nextRecord = avroParquetReader.read()
    // println(nextRecord)
}
