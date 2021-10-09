# AdmiralCloud ElasticSearch Connector
This helper initialitzes ElastistSearch instances and makes them available on a global variable 

# Usage
```
const esOptions = {
  servers: [
    { server: 'cluster', host: 'localhost', port: 9243, auth: 'username:password', apiVersion: '7.1', protocol: 'https' }
  ],
  indices: [
    { model: 'books', type: 'item', indexInfix: 'books', server: 'cluster', resetForTest: true, instance: 'books' }
  ]
}


TBC