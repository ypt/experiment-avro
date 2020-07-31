require 'avro'

# write data into file
SCHEMA = <<-JSON
{"namespace": "example.avro",
  "type": "record",
  "name": "User",
  "fields": [
      {"name": "name", "type": "string"},
      {"name": "favorite_number",  "type": ["int", "null"]},
      {"name": "favorite_color", "type": ["string", "null"]}
  ]
}
JSON

file = File.open('data.avr', 'wb')
schema = Avro::Schema.parse(SCHEMA)

puts("SCHEMA md5_fingerprint")
p(schema.md5_fingerprint)

puts("SCHEMA sha256_fingerprint")
p(schema.sha256_fingerprint)

puts("SCHEMA crc_64_avro_fingerprint")
p(schema.crc_64_avro_fingerprint)

puts("SCHEMA single_object_schema_fingerprint")
p(schema.single_object_schema_fingerprint)

puts("SCHEMA single_object_encoding_header")
p(schema.single_object_encoding_header)

writer = Avro::IO::DatumWriter.new(schema)
dw = Avro::DataFile::Writer.new(file, writer, schema)
dw << {"name" => "john", "favorite_number" => 25, "favorite_color" => "blue"}
dw << {"name" => "ryan", "favorite_number" => 23, "favorite_color" => "red"}
dw.close

# read all data from avro file
file = File.open('data.avr', 'r+')
dr = Avro::DataFile::Reader.new(file, Avro::IO::DatumReader.new)
dr.each { |record| p record }

# extract the name only from the avro serialized file
READER_SCHEMA = <<-JSON
{ "namespace": "example.avro",
  "type": "record",
  "name": "User",
  "fields" : [
    {"name": "name", "type": "string"}
  ]
}
JSON

reader = Avro::IO::DatumReader.new(nil, Avro::Schema.parse(READER_SCHEMA))
dr = Avro::DataFile::Reader.new(file, reader)
dr.each { |record| p record }

# let's try reading into an incompatible schema
READER_SCHEMA_2 = <<-JSON
{ "namespace": "example.avro",
  "type": "record",
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
  p(e)
end
# #<Avro::AvroError: Missing data for "int" with no default>

# let's try writing incompatible data
writer = Avro::IO::DatumWriter.new(schema)
dw = Avro::DataFile::Writer.new(file, writer, schema)
begin
  dw << {"hello" => "world"}
  dw.close
rescue StandardError => e
  p(e)
  dw.close
end

# write some data in binary to string buffer
writer = StringIO.new
encoder = Avro::IO::BinaryEncoder.new(writer)
datum_writer = Avro::IO::DatumWriter.new(schema)
datum_writer.write({"name" => "Alyssa", "favorite_number" => 256, "favorite_color" => nil}, encoder)

# as hex string

def to_hex_string(byte)
  byte.to_s(16)
end

puts("SINGLE OBJECT ENCODING")
# https://avro.apache.org/docs/current/spec.html#single_object_encoding_spec

puts("SCHEMA FINGERPRINT - via schema.single_object_schema_fingerprint")
p(schema.single_object_schema_fingerprint.map {|byte| to_hex_string(byte)})

puts("HEADER FOR SINGLE OBJECT ENCODING - via schema.single_object_encoding_header")
p(schema.single_object_encoding_header.map {|byte| to_hex_string(byte)})

# Avro object
puts("ENCODED DATUM")
p(writer.string.bytes.map {|byte| to_hex_string(byte)})

puts("HEADER + DATUM")
single_object_encoded = schema.single_object_encoding_header + writer.string.bytes
p(single_object_encoded.map {|byte| to_hex_string(byte)})

puts("FIXING single_object_schema_fingerprint")
# I _think_ the Ruby Avro library's schema.single_object_schema_fingerprint _incorrectly_ reverses the returned array
# For example, compare the fingerprint byte array from the Ruby example to the Java example in this repo.
#
# Additional context:
# - Avro Ruby lib code: https://github.com/apache/avro/blob/release-1.10.0/lang/ruby/lib/avro/schema.rb#L180
# - PR with a fix: https://github.com/apache/avro/pull/937

# For now, here's an example that uses the fixed version
def fixed_single_object_schema_fingerprint(schema)
  working = schema.crc_64_avro_fingerprint
  bytes = Array.new(8)
  8.times do |i|
    # changed from this:
    # bytes[7 - i] = (working & 0xff)
    # to this:
    bytes[i] = (working & 0xff)

    working = working >> 8
  end
  bytes
end
SINGLE_OBJECT_MAGIC_NUMBER = [0xC3, 0x01]
def fixed_single_object_encoding_header(schema)
  [SINGLE_OBJECT_MAGIC_NUMBER, fixed_single_object_schema_fingerprint(schema)].flatten
end
fixed_single_object_encoded = fixed_single_object_encoding_header(schema) + writer.string.bytes
p(fixed_single_object_encoded.map {|byte| to_hex_string(byte)})

puts("DECODING ENCODED DATUM")
# read the data
reader = StringIO.new(writer.string())
decoder = Avro::IO::BinaryDecoder.new(reader)
datum_reader = Avro::IO::DatumReader.new(schema)
read_value = datum_reader.read(decoder)
p(read_value)

# It looks like there was some work in the past to implement a single-object message writer and a schema store interface
# in Ruby. But that work was unfinished and not merged. https://github.com/apache/avro/pull/43
#
# It probably won't be that much work to finish it up, building upon: 1) the PR #43 2) the fingerprinting code now in
# place for Ruby in schema.rb 3) the Java implementation of BinaryMessageEncoder and SchemaStore
#
# That said, the the header and encoded datum are accessible independently, so one could still leverage those outside of
# the Ruby Avro library.
