# Stream Realtime spark streaming Source


Description
-----------
Reads realtime data from a stream. If the format and schema of the stream are known,
they can be specified as well. The source will return a record for each stream event it reads.
Records will always contain a 'ts' field of type 'long' that contains the timestamp of the event,
as well as a 'headers' field of type 'map<string, string>' that contains the headers for
the event. Other fields output records are determined by the configured format and schema.

Properties
----------
**name:** Name of the stream. Must be a valid stream name. If the stream does not exist,
it will be created. (Macro-enabled)

**format:** Optional format of the stream. Any format supported by CDAP is also supported.
For example, a value of 'csv' will attempt to parse stream events as comma separated
values. If no format is given, event bodies will be treated as bytes, resulting in a
three-field schema: 'ts' of type long, 'headers' of type map of string to string, and 'body' of
type bytes.

**schema:** Optional schema for the body of stream events. Schema is used in conjunction
with format to parse stream events. Some formats like the avro format require schema,
while others do not. The schema given is for the body of the stream, so the final schema
of records output by the source will contain an additional field named 'ts' for the
timestamp and a field named 'headers' for the headers as the first and second fields of
the schema.


Example
-------
This example reads from a stream named 'devices':

    {
        "name": "Stream",
        "type": "batchsource",
        "properties": {
            "name": "devices",
            "format": "csv"
        }
    }

The stream contents will be parsed as 'csv' (Comma Separated Values), which will output
records with this schema:

    +======================================+
    | field name     | type                |
    +======================================+
    | ts             | long                |
    | headers        | map<string, string> |
    | device_id      | nullable string     |
    | device_name    | nullable string     |
    | user           | nullable string     |
    | request_time   | nullable string     |
    | request        | nullable string     |
    | status         | nullable string     |
    | content_length | nullable string     |
    +======================================+

The 'ts' and 'headers' fields will be always be present regardless of the stream format.
All other fields in this example come from the schema defined by the user.
