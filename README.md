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
  }
  [, "compressionScheme": "gzip"]
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

The simplest way to test is using Nix system and run subset of `.travis.yml`. One time only, generate an ssh key:

```bash
$ ssh-keygen -t rsa -N "passphrase" -f key_for_docker,
```

Then, when you want to test, run this:

```bash
$ docker swarm init
$ docker stack deploy -c docker-compose.yml teststack
$ it/scripts/run_test_data.sh
```

It starts multiple containers:
+ sshd with `root:root` with `22222` ssh port;
+ kafka_local a self-contained zookeeper and kafka server, advertising "localhost" as broken name
  and exposing port 9092 for non-tunneled integration testing;
+ kafka_ssh a self-contained zookeeper and kafka server.

You can stop it afterwards with

```bash
$ docker stack rm teststack
$ docker swarm leave --force  # Or keep it running if you are going to update the stack
```

