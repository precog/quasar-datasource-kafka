# quasar-datasource-kafka [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/pSSqJrr)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-datasource-kafka" % <version>
```

## Configuration

The configuration of the Kafka Datasource has the following JSON format:

```
{
 "bootstrapServers": Array of Strings "host:port",
 "groupId": String,
 "topics": Array of Strings,
 "decoder": < "RawKey" | "RawValue" >,
  "format": {
    "type": "json" | "separated-values"
    // for "json"
    "precise": Boolean,
    "variant" "array-wrapped" | "line-delimited"
    // for "separated-values", all strings must be one symbol length
    "header": Boolean,
    // The first char of row break
    "row1": String,
    // The second char of row break, empty string if row break has only one symbol
    "row2": String,
    // Column separator (char)
    "record": String,
    "openQuote": String,
    "closeQuote": String,
    "escape": String
  },
}

```

*
*
*

Example

```json
{
 "bootstrapServers": [ "localhost:9092" ],
 "groupId": "precog",
 "topics": [ "topic" ],
 "decoder": "RawValue",
 "format": {
   "type": "json",
   "variant": "line-delimited",
   "precise": false
 }
}
```

## Testing

