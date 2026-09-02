'use strict'

const assert = require('assert/strict')
const sinon = require('sinon')

// ---------------------------------------------------------------------------
// Minimal require-cache mock helper (replaces proxyquire)
// ---------------------------------------------------------------------------
function loadWithMocks(mocks) {
  const mainPath = require.resolve('../index')

  // Save and replace dependency entries in the require cache
  const saved = {}
  for (const [name, exports] of Object.entries(mocks)) {
    let resolvedPath
    try { resolvedPath = require.resolve(name) } catch { continue }
    saved[resolvedPath] = require.cache[resolvedPath]
    require.cache[resolvedPath] = { id: resolvedPath, filename: resolvedPath, loaded: true, exports }
  }

  // Clear the main module so it re-requires with the mocked dependencies
  delete require.cache[mainPath]
  const mod = require('../index')

  // Restore everything so other tests start clean
  delete require.cache[mainPath]
  for (const [path, original] of Object.entries(saved)) {
    if (original) { require.cache[path] = original }
    else { delete require.cache[path] }
  }

  return mod
}

// ---------------------------------------------------------------------------

describe('ac-bootstrap-elasticsearch', () => {
  let acapi
  let mockClient
  let MockClientConstructor
  let moduleFactory

  beforeEach(() => {
    mockClient = {
      cluster: {
        stats: sinon.stub().resolves({ body: { cluster_name: 'test-cluster', nodes: { versions: ['2.x'] } } })
      },
      indices: {
        exists: sinon.stub().resolves({}),
        delete: sinon.stub().resolves({ body: { acknowledged: true } }),
        updateAliases: sinon.stub().resolves({ body: { acknowledged: true } })
      },
      snapshot: {
        status: sinon.stub().resolves({ body: {} })
      },
      count: sinon.stub().resolves({ body: { count: 42 } })
    }

    MockClientConstructor = sinon.stub().returns(mockClient)

    moduleFactory = loadWithMocks({
      '@opensearch-project/opensearch': { Client: MockClientConstructor },
      '@opensearch-project/opensearch/aws': { AwsSigv4Signer: sinon.stub().returns({}) },
      '@aws-sdk/credential-provider-node': { defaultProvider: sinon.stub().returns(() => Promise.resolve({})) }
    })

    acapi = {
      config: {
        environment: 'test',
        elasticSearch: {
          indices: [],
          servers: [],
          timeout: 30000
        }
      },
      log: { warn: sinon.stub(), error: sinon.stub() },
      aclog: { serverInfo: sinon.stub().returns([{ field: 'Instance', value: 'main' }]) },
      elasticSearch: {}
    }
  })

  afterEach(() => {
    sinon.restore()
    delete process.env.NODE_TEST_ORIGIN
  })

  // ─── init ─────────────────────────────────────────────────────────────────

  describe('init', () => {
    it('returns an empty log collector when no indices are configured', async () => {
      const { init } = moduleFactory(acapi)
      const result = await init()
      assert.deepEqual(result, [])
    })

    it('returns an empty log collector when indices is undefined', async () => {
      delete acapi.config.elasticSearch.indices
      const { init } = moduleFactory(acapi)
      const result = await init()
      assert.deepEqual(result, [])
    })

    it('filters out indices with omitInTest=true in test environment', async () => {
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'omitted', server: 'main', instance: 'main', omitInTest: true },
        { model: 'kept',    server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      // Only one index should have been processed → one cluster.stats call
      assert.equal(mockClient.cluster.stats.callCount, 1)
    })

    it('throws when a server config is missing for an index', async () => {
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'missing-server', instance: 'docs' }
      ]

      const { init } = moduleFactory(acapi)
      await assert.rejects(init(), Error)
    })

    it('prefixes the index name with the environment', async () => {
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      assert.equal(acapi.config.elasticSearch.indices[0].index, 'test_docs')
    })

    it('does not prefix the index name when the index is global', async () => {
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'shared', server: 'main', instance: 'main', global: true }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      assert.equal(acapi.config.elasticSearch.indices[0].index, 'shared')
    })

    it('includes NODE_TEST_ORIGIN in the index name when set', async () => {
      process.env.NODE_TEST_ORIGIN = 'pr123'
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      assert.equal(acapi.config.elasticSearch.indices[0].index, 'test_pr123_docs')
    })

    it('uses indexInfix instead of model when provided', async () => {
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main', indexInfix: 'documents' }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      assert.equal(acapi.config.elasticSearch.indices[0].index, 'test_documents')
    })

    it('reuses an existing client instance for the same instance name', async () => {
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs',     server: 'main', instance: 'main' },
        { model: 'articles', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      // Client constructor should only be called once despite two indices sharing the same instance
      assert.equal(MockClientConstructor.callCount, 1)
    })

    it('collects DocCount for each index', async () => {
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      const result = await init()

      const entry = result.find(e => e.field === 'DocCount')
      assert.ok(entry)
      assert.equal(entry.value, 42)
    })

    it('falls back to the error message when DocCount call fails', async () => {
      mockClient.count.rejects(new Error('connection refused'))
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      const result = await init()

      const entry = result.find(e => e.field === 'DocCount')
      assert.ok(entry)
      assert.equal(entry.value, 'connection refused')
    })

    it('falls back to "Error" when DocCount error has no message property', async () => {
      mockClient.count.rejects({})
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      const result = await init()

      const entry = result.find(e => e.field === 'DocCount')
      assert.ok(entry)
      assert.equal(entry.value, 'Error')
    })

    it('sets rejectUnauthorized to false in non-production environments', async () => {
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      const esConfig = MockClientConstructor.firstCall.args[0]
      assert.equal(esConfig.node.ssl.rejectUnauthorized, false)
    })

    it('sets rejectUnauthorized to true in production', async () => {
      acapi.config.environment = 'production'
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      const esConfig = MockClientConstructor.firstCall.args[0]
      assert.equal(esConfig.node.ssl.rejectUnauthorized, true)
    })

    it('uses AwsSigv4Signer when server has awsCluster=true', async () => {
      const mockCredentials = { accessKeyId: 'test', secretAccessKey: 'test' }
      const credentialsProvider = sinon.stub().resolves(mockCredentials)
      const defaultProviderStub = sinon.stub().returns(credentialsProvider)
      const AwsSigv4SignerStub = sinon.stub().callsFake(({ getCredentials }) => {
        getCredentials()
        return { connector: 'aws' }
      })
      const awsModuleFactory = loadWithMocks({
        '@opensearch-project/opensearch': { Client: MockClientConstructor },
        '@opensearch-project/opensearch/aws': { AwsSigv4Signer: AwsSigv4SignerStub },
        '@aws-sdk/credential-provider-node': { defaultProvider: defaultProviderStub }
      })

      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'aws.example.com', port: 443, awsCluster: true }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = awsModuleFactory(acapi)
      await init()

      assert.ok(AwsSigv4SignerStub.calledOnce)
      assert.ok(defaultProviderStub.calledOnce)
      assert.ok(credentialsProvider.calledOnce)
    })

    it('uses localElasticSearch config for host, port, and protocol', async () => {
      acapi.config.localElasticSearch = { protocol: 'http', host: 'local-host', port: 9201 }
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'remote-host', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      await init()

      const esConfig = MockClientConstructor.firstCall.args[0]
      assert.equal(esConfig.node.url.hostname, 'local-host')
      assert.equal(esConfig.node.url.port, '9201')
      assert.equal(esConfig.node.url.protocol, 'http:')
    })

    it('continues init when cluster.stats throws', async () => {
      mockClient.cluster.stats.rejects(new Error('cluster unavailable'))
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'main' }
      ]

      const { init } = moduleFactory(acapi)
      const result = await init()

      assert.ok(Array.isArray(result))
    })
  })

  // ─── prepareForTest ────────────────────────────────────────────────────────

  describe('prepareForTest', () => {
    beforeEach(async () => {
      acapi.config.elasticSearch.servers = [{ server: 'main', host: 'localhost', port: 9200 }]
      acapi.config.elasticSearch.indices = [
        { model: 'docs', server: 'main', instance: 'docs' }
      ]
      const { init } = moduleFactory(acapi)
      await init()
    })

    it('deletes the index and returns acknowledged', async () => {
      const { prepareForTest } = moduleFactory(acapi)
      const result = await prepareForTest({ instance: 'docs' })

      assert.ok(mockClient.indices.delete.calledOnce)
      const entry = result.find(e => e.field === 'Deleted index')
      assert.equal(entry.value, true)
    })

    it('calls createMapping and updates the alias when createMapping is provided', async () => {
      const createMapping = sinon.stub().resolves()

      const { prepareForTest } = moduleFactory(acapi)
      const result = await prepareForTest({ instance: 'docs', createMapping })

      assert.ok(createMapping.calledOnce)
      assert.ok(mockClient.indices.updateAliases.calledOnce)
      assert.ok(result.find(e => e.field === 'Updated alias'))
    })

    it('passes a uuid-suffixed index name to createMapping', async () => {
      const createMapping = sinon.stub().resolves()

      const { prepareForTest } = moduleFactory(acapi)
      await prepareForTest({ instance: 'docs', createMapping })

      const { index } = createMapping.firstCall.args[0]
      assert.match(index, /^test_docs_[0-9a-f-]{36}$/)
    })

    it('waits and retries when a snapshot is in progress', async () => {
      mockClient.snapshot.status
        .onFirstCall().resolves({ body: { error: { root_cause: [{ type: 'snapshot_in_progress_exception' }] } } })
        .onSecondCall().resolves({ body: {} })

      const clock = sinon.useFakeTimers()

      const { prepareForTest } = moduleFactory(acapi)
      const promise = prepareForTest({ instance: 'docs' })

      await clock.tickAsync(1100)
      await promise

      assert.equal(mockClient.snapshot.status.callCount, 2)
      clock.restore()
    })

    it('throws checkForSnapshotFailed when the snapshot check itself errors', async () => {
      mockClient.snapshot.status.rejects(new Error('network error'))

      const { prepareForTest } = moduleFactory(acapi)
      await assert.rejects(prepareForTest({ instance: 'docs' }), { message: 'checkForSnapshotFailed' })
    })
  })
})
