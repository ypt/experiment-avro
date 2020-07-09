require 'avro'

# write data into file
SCHEMA = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "verified", "type": "boolean", "default": false}
  ]
}
JSON

file = File.open('data.avr', 'wb')
schema = Avro::Schema.parse(SCHEMA)

puts("SCHEMA md5_fingerprint")
puts(schema.md5_fingerprint)

puts("SCHEMA sha256_fingerprint")
puts(schema.sha256_fingerprint)

puts("SCHEMA crc_64_avro_fingerprint")
puts(schema.crc_64_avro_fingerprint)

puts("SCHEMA single_object_schema_fingerprint")
puts(schema.single_object_schema_fingerprint)

puts("SCHEMA single_object_encoding_header")
puts(schema.single_object_encoding_header)

writer = Avro::IO::DatumWriter.new(schema)
dw = Avro::DataFile::Writer.new(file, writer, schema)
dw << {"username" => "john", "age" => 25, "verified" => true}
dw << {"username" => "ryan", "age" => 23, "verified" => false}
dw.close

# read all data from avro file
file = File.open('data.avr', 'r+')
dr = Avro::DataFile::Reader.new(file, Avro::IO::DatumReader.new)
dr.each { |record| p record }

# extract the username only from the avro serialized file
READER_SCHEMA = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"}
  ]
}
JSON

reader = Avro::IO::DatumReader.new(nil, Avro::Schema.parse(READER_SCHEMA))
dr = Avro::DataFile::Reader.new(file, reader)
dr.each { |record| p record }

# let's try reading into an incompatible schema
READER_SCHEMA_2 = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "hello", "type": "int"}
  ]
}
JSON
reader = Avro::IO::DatumReader.new(nil, Avro::Schema.parse(READER_SCHEMA_2))
dr = Avro::DataFile::Reader.new(file, reader)
begin
  dr.each { |record| p record }
rescue StandardError => e
  p e
end
# #<Avro::AvroError: Missing data for "int" with no default>

# let's try writing incompatible data
writer = Avro::IO::DatumWriter.new(schema)
dw = Avro::DataFile::Writer.new(file, writer, schema)
begin
  dw << {"hello" => "world"}
  dw.close
rescue StandardError => e
  p e
  dw.close
end

# write some data in binary to string buffer
writer = StringIO.new
encoder = Avro::IO::BinaryEncoder.new(writer)
datum_writer = Avro::IO::DatumWriter.new(schema)
datum_writer.write({"username" => "john", "age" => 25, "verified" => true}, encoder)

# read the data
reader = StringIO.new(writer.string())
decoder = Avro::IO::BinaryDecoder.new(reader)
datum_reader = Avro::IO::DatumReader.new(schema)
read_value = datum_reader.read(decoder)
p read_value