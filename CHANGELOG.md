# Changelog

## [1.1.0](https://github.com/markovarghese/micewriter-sdk-java/compare/v1.0.0-SNAPSHOT...v1.1.0) (2026-06-16)


### Features

* add Java SDK source and Maven config ([6d6a937](https://github.com/markovarghese/micewriter-sdk-java/commit/6d6a937f3c36385e220f75ed808bc5fa218a5b63))
* convert payload serialization to JSON and add UDS reconnect listener ([acacb48](https://github.com/markovarghese/micewriter-sdk-java/commit/acacb48c8fb0bd2ff3df7c6ec7a7571c6bcd33b5))
* Enforce 16 MB max payload size limit ([808789f](https://github.com/markovarghese/micewriter-sdk-java/commit/808789fd0cd176cb6d7679337e9bcb5a225a418b))
* Expose flushNow() SDK method for manual flushing ([312b619](https://github.com/markovarghese/micewriter-sdk-java/commit/312b619e5696b6077c27f82d99471b8ffe7632b2))
* implement async pipelining and document startup race condition ([da794c5](https://github.com/markovarghese/micewriter-sdk-java/commit/da794c50ddfd837b6e42461d845decaeb2780c98))
* implement ICEBERG list type support, sandbox migration, and load test updates ([b8d79a0](https://github.com/markovarghese/micewriter-sdk-java/commit/b8d79a0908bbd19aa58b31d59e5d01274f25f99f))
* implement multi-module versioning and extract api module ([5e7c118](https://github.com/markovarghese/micewriter-sdk-java/commit/5e7c1183693d0c5d36416e375c5fd7ba735baf55))
* **sdk:** bounded-async sendAsync pipelining for the UDS transport ([09092a9](https://github.com/markovarghese/micewriter-sdk-java/commit/09092a9ac4e417c976324ae60f7a9b3a8dddb47f))
* serialize INGEST_RECORD as Apache Arrow IPC instead of JSON ([5da0e1e](https://github.com/markovarghese/micewriter-sdk-java/commit/5da0e1e7cc199ea28ee4bd36dec31611216dcd64))


### Bug Fixes

* 1: Implement UDS connection reconnect and retry ([1c6ea83](https://github.com/markovarghese/micewriter-sdk-java/commit/1c6ea83a1ada6a7d84dd2b25542331ed61f9d3a0))
* order ack future under sendLock to prevent ACK misdelivery ([a30df7f](https://github.com/markovarghese/micewriter-sdk-java/commit/a30df7f7dc3fb0a2efae7c1585253202a536fcd9))


### Documentation

* add known issue for startup race condition ([4ead7b3](https://github.com/markovarghese/micewriter-sdk-java/commit/4ead7b31f9f7c51712def6f9ac50dc2bdb1169a7))
* update README for dual-branch architecture and BOM ([effe5d5](https://github.com/markovarghese/micewriter-sdk-java/commit/effe5d53b7f3ebebfb2513972994168c3eaca388))
* warn about ZonedDateTime in timestamptz columns ([076099f](https://github.com/markovarghese/micewriter-sdk-java/commit/076099f87d4e97456110bd80e1e9aa56fa152372))
