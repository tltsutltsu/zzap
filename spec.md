# zzap specification

## What is zzap?

zzap is a privacy and performance focused search backend.

zzap uses FHE to encrypt the data and perform search queries, so the data is never exposed to the server.

Because of the FHE, zzap cannot perform complex search queries.

### What zzap cannot do

- Correct user input mistakes
- Search for similar words
- Auto-complete a word

## Data model

zzap uses a simple data model.

zzap do not stores the data itself, only the metadata:

- id &mdash; the id of the data, which you will use to retrieve the full data of an item when the search query matches against it
- content &mdash; the content of the data, which will be used to match against the search query

`id` + `content` pairs are stored in the _collections_, which can be put into _buckets_, so you can have a bucket for each user to search only on their own data, or put everything into a `default` bucket.

## Protocol

zzap uses a simple protocol to send messages between server and client, just plain text over TCP.

You can just use telnet to send and receive messages.

At the moment, there is no authentication.

### Message format

Message format is very much inspired by Redis.

Message is composed of a command and arguments, followed by a newline character.

```plaintext
<command> <arg> [ <arg> ... ] \n
```

For arguments that may contain spaces or newlines, use a length-prefixed format:

```plaintext
<command> <arg1_length>:<arg1_data> <arg2_length>:<arg2_data> ...\n
```

### Responses

Responses follow a similar format:

```plaintext
+OK\n // Success with no data

-ERR <error_message>\n // Error

$<length>\n<data>\n // Bulk string response

<count>\n<response1>... // Array of responses
```

### Commands

#### `PING`

Arguments: none

Response: `+OK\n`

This command is used to test if the server is responsive. The server should reply with "PONG".

#### `SET <bucket> <collection> <id> <content> [key]`

Arguments:

- `bucket` &mdash; the bucket to store the data in
- `collection` &mdash; the collection to store the data in
- `id` &mdash; the id of the data
- `content` &mdash; the content of the data
- `key` &mdash; the key to use to encrypt the data

Response: `+OK\n` on success, `-ERR <message>\n` on error

This command is used to store data in a collection. If data with the same `id` already exists, it will be overwritten.

#### `GET <bucket> <collection> <id> [key]`

Arguments:

- `bucket` &mdash; the bucket to store the data in
- `collection` &mdash; the collection to store the data in
- `id` &mdash; the id of the data
- `key` &mdash; the key to use to decrypt the data

Response: `$<length>\n<content>\n` or `$-1\n` if not found

This command is used to get the `content` from a collection by its `id`.

#### `SEARCH <bucket> <collection> <query>`

Arguments:

- `bucket` &mdash; the bucket to search in
- `collection` &mdash; the collection to search in
- `query` &mdash; the query to search for

Response: Array of matching IDs

This command is used to search for data in a collection by its `content`.

## Encryption of data

### What is FHE?

FHE stands for Fully Homomorphic Encryption.

FHE is a type of encryption that allows you to perform operations on encrypted data without decrypting it.

So the server cannot read the data, or if any data is leaked, the data is still encrypted.

You may optionally send the key to the server, so the server can decrypt/encrypt the data for you, but the most secure way is to decrypt/encrypt the data on the client side.

We use [Concrete Boolean](https://github.com/zama-ai/concrete-boolean) as the FHE library. Thanks to [Zama](https://zama.ai/) for their work on this library.

### Security

Sending key over plain text TCP is not secure.

So if you use `key` argument, you should should not talk to server over Internet.

Instead, as of now, you should host zzap on the same machine as your application or host it on a private network, i.e. in k8s cluster, not exposed to Internet.

## Client libraries

There is client library for PHP, Node.js, Rust, and Go.

It provides convenient functions to send messages to the server and get responses, encrypting/decrypting the data as needed **on the client side**.

## Performance

zzap is designed to be performant.

It uses FHE, so some performance overhead is expected, but we expect it to be minimal and negligible compared to the performance of the server.

## Alternatives to this solution

### Why not use existing search engines?

They do not support FHE.

### Why not implement this as client-side library only?

One may suggest implementing this on top of Redis or other key-value store, providing a FHE layer as a client library only.

Although this is a valid approach, it has the drawback of that this key-value store (likely, generic one) would need to be optimized for search on such type of data.
