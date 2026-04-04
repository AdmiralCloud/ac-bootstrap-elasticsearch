# ac-bootstrap-elasticsearch

Initializes OpenSearch/ElasticSearch client instances and attaches them to `acapi.elasticSearch` for use across an AC application.

## Installation

```bash
yarn add ac-bootstrap-elasticsearch
```

## Usage

```js
const esBootstrap = require('ac-bootstrap-elasticsearch')(acapi)

// Initialize all configured indices (called once at app startup)
await esBootstrap.init()

// In tests: reset an index and optionally rebuild its mapping
await esBootstrap.prepareForTest({ instance: 'books', createMapping: myMappingFn })
```

## Configuration

The module reads from `acapi.config.elasticSearch`:

```js
acapi.config.elasticSearch = {
  timeout: 30000, // request timeout in ms (default: 30000)
  servers: [
    {
      server: 'cluster',       // unique name, referenced by indices
      host: 'localhost',
      port: 9243,
      protocol: 'https',
      auth: 'username:password', // optional
      awsCluster: false          // set true to enable AWS Sigv4 signing
    }
  ],
  indices: [
    {
      model: 'books',      // used to build the index name and look up the instance
      server: 'cluster',   // must match a server name above
      instance: 'books',   // key under acapi.elasticSearch where the client is stored
      indexInfix: 'books', // optional – overrides model in the index name
      global: false,       // true = no environment prefix in the index name
      omitInTest: false    // true = skip this index when environment === 'test'
    }
  ]
}
```

### Index naming

The resolved index name follows this pattern:

```
[environment_][NODE_TEST_ORIGIN_][indexInfix|model]
```

| Config | Result |
|---|---|
| `model: 'books'`, env `staging` | `staging_books` |
| `model: 'books'`, env `staging`, `NODE_TEST_ORIGIN=pr42` | `staging_pr42_books` |
| `model: 'books'`, `global: true` | `books` |
| `indexInfix: 'catalog'`, env `production` | `production_catalog` |

### AWS / OpenSearch Service

Set `awsCluster: true` on a server entry to enable AWS Sigv4 request signing. Credentials are resolved via the standard AWS SDK credential chain (`@aws-sdk/credential-provider-node`).

```js
{ server: 'aws', host: 'search-….es.amazonaws.com', port: 443, protocol: 'https', awsCluster: true }
```

### Local override

Set `acapi.config.localElasticSearch` to override the host/port/protocol for a local tunnel or dev environment:

```js
acapi.config.localElasticSearch = { host: '127.0.0.1', port: 9200, protocol: 'http' }
```

## API

### `init()`

Connects to all configured servers and attaches the clients to `acapi.elasticSearch[instance]`. Indices with `omitInTest: true` are skipped when `environment === 'test'`. Returns a log-collector array.

### `prepareForTest({ instance, createMapping })`

Resets an index for test isolation:

1. Checks for an in-progress cluster snapshot and waits if one is found.
2. Deletes all index variants matching `<indexName>*`.
3. If `createMapping` is provided, creates a fresh UUID-named index, calls `createMapping({ index, model })`, then adds an alias pointing back to the configured index name.

## Tests

```bash
yarn test
```

Tests use [Mocha](https://mochajs.org/) and [Sinon](https://sinonjs.org/) with Node's built-in `assert` module. No real OpenSearch connection is required.

## License

MIT, Copyright AdmiralCloud AG 
