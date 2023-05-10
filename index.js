const _ = require('lodash') 
const { v4: uuidv4 } = require('uuid')

const { Client, Transport, Connection } = require('@opensearch-project/opensearch')
const { fromNodeProviderChain } = require('@aws-sdk/credential-providers')

const aws4 = require('aws4');
const crypto = require('crypto');

// origin: https://github.com/opensearch-project/opensearch-js/blob/main/lib/aws/AwsSigv4Signer.js
// reduced to support only AWS SDKv3
// enhanced to always check the credentials (so they can be rotated in the background)
function AwsSigv4Signer(opts = {}) {
  const credentialsState = {
    credentials: null,
  }
  if (!opts.region) {
    throw new Error('Region cannot be empty')
  }
  if (!opts.service) {
    opts.service = 'es'
  }

  function buildSignedRequestObject(request = {}) {
    request.service = opts.service
    request.region = opts.region
    request.headers = request.headers || {}
    request.headers['host'] = request.hostname
    const signed = aws4.sign(request, credentialsState.credentials)
    signed.headers['x-amz-content-sha256'] = crypto
      .createHash('sha256')
      .update(request.body || '', 'utf8')
      .digest('hex')
    return signed
  }

  class AwsSigv4SignerConnection extends Connection {
    buildRequestObject(params) {
      const request = super.buildRequestObject(params)
      return buildSignedRequestObject(request)
    }
  }

  class AwsSigv4SignerTransport extends Transport {
    request(params, options = {}, callback = undefined) {
      // options is optional so if options is a function, it's the callback.
      if (typeof options === 'function') {
        callback = options
        options = {}
      }

      // For AWS SDK V3 or when the client has not acquired credentials yet.
      if (typeof callback === 'undefined') {
        return opts.getCredentials().then((credentials) => {
          credentialsState.credentials = credentials
          return super.request(params, options)
        })
      } 
      else {
        opts
          .getCredentials()
          .then((credentials) => {
            credentialsState.credentials = credentials
            super.request(params, options, callback)
          })
          .catch(callback)
      }
    }
  }

  return {
    Transport: AwsSigv4SignerTransport,
    Connection: AwsSigv4SignerConnection,
    buildSignedRequestObject,
  }
}

/**
 * https://docs.aws.amazon.com/opensearch-service/latest/developerguide/request-signing.html#request-signing-node
 * https://www.npmjs.com/package/aws-opensearch-connector
 * https://opensearch-project.github.io/opensearch-js/2.1/index.html
 * @param options.createMapping FUNCTION OPTION - function to call if you want to rebuild ES indices during test
 **/

module.exports = (acapi) => {

  const getClient = async ({ instance, server, index, region = 'eu-central-1', profile = process.env['profile'], debug = false }) => {
    const protocol = _.get(acapi.config, 'localElasticSearch.protocol') || _.get(server, 'protocol', 'https')
    const host = _.get(acapi.config, 'localElasticSearch.host') ||  _.get(server, 'host', 9200)
    const port =  _.get(acapi.config, 'localElasticSearch.port') ||  _.get(server, 'port')
    const url =  `${protocol}://${host}:${port}`

    const esConfig = {
      node: {
        url: new URL(url),
        ssl: {
          // allow different certificate for SSH tunnel on NON-production system
          rejectUnauthorized: acapi.config.environment === 'production'
        }  
      },
      auth: _.get(server, 'auth'),
      requestTimeout: acapi.config.elasticSearch.timeout,
    }

    if (!acapi.config.localElasticSearch && _.get(server, 'awsCluster')) {
      const osConnector = AwsSigv4Signer({
        service: 'es',
        region,
        getCredentials: async() => {
          const credentialsProvider = fromNodeProviderChain({ profile, ignoreCache: true })
          if (debug) {
            const credentials = await credentialsProvider()
            console.log('ac-bootstrap-elasticsearch | AWS session | AccessKey %s', credentials?.accessKeyId)
          }
          return credentialsProvider();
        }
      })
      _.merge(esConfig, osConnector)
    }
    acapi.elasticSearch[instance] = new Client(esConfig)
    
    const serverInfo = {
      instance,
      index: _.get(index, 'index'),
      host,
      port
    }
    // check version
    try {
      const result = await acapi.elasticSearch[instance].cluster.stats()
      serverInfo.cluster = _.get(result, 'body.cluster_name')
      serverInfo.clusterVersion = _.get(result, 'body.nodes.versions[0]')

      // check if index exists
      await acapi.elasticSearch[instance].indices.exists({ index: _.get(index, 'index') })
    }
    catch(e) {
      console.log(84, e)
    }

    acapi.aclog.serverInfo(serverInfo)
  }

  const getIndexStats = async({ index }) => {
    const response = await acapi.elasticSearch[index.instance].count({
      index: index.index
    })
    return _.get(response, 'body.count')
  }

  const init = async() => {
    acapi.aclog.headline({ headline: 'ELASTICSEARCH' })
    
    // init multiple instances for different purposes
    acapi.elasticSearch = {}
    
    let indices = acapi.config.elasticSearch.indices || []
    // filter out indices with omitInTest = true
    if (acapi.config.environment === 'test') {
      indices = _.filter(indices, index => {
        if (!index.omitInTest) return index
      })
    }
  
    for (const index of indices) {
      if (acapi.config.environment === 'test' && _.get(index, 'omitInTest')) return 
      let instance = _.get(index, 'instance')
      let server = _.find(acapi.config.elasticSearch.servers, { server: _.get(index, 'server') })
      if (!server) throw new Error({ message: 'serverConfigurationMissingForES' })

      // update config with environment if not global
      let pos = _.findIndex(indices, { model: index.model })
      index.index = (!index.global ? acapi.config.environment + '_' : '') + (process.env.NODE_TEST_ORIGIN ? process.env.NODE_TEST_ORIGIN + '_' : '' ) + (index.indexInfix || index.model)
      indices.splice(pos, 1, index)

      // check if instance is already created - do not instanciate multiple times, we can re-use it
      if (_.has(acapi.elasticSearch, instance)) {
        acapi.aclog.serverInfo({
          instance,
          index: _.get(index, 'index')
        })
      }
      else {
        // create an elasticsearch client for your Amazon ES
        await getClient({ instance, server, index, debug: _.get(index, 'debug') })
      }
      const docCount = await getIndexStats({ index })
      acapi.aclog.listing({
        field: 'DocCount',
        value: docCount
      })
    }
  }

  const checkForSnapshot = async({ instance }) => {
    let response = await acapi.elasticSearch[instance].snapshot.status()
    return _.get(response, 'body.error.root_cause[0].type') 
  }
  

  const prepareForTest = async({ instance, createMapping }) => {
    const index = _.find(acapi.config.elasticSearch.indices, { model: instance })

    // reset ES in for tests
    // check for snapshot and wait (if not localDevelopment) - otherwise tests will fail
    try {
      const snapshotStatus = await checkForSnapshot({ instance })
      if (snapshotStatus ===  'snapshot_in_progress_exception') {
        acapi.log.warn('Bootstrap | ES | Cluster is snapshotting... we are waiting | %j', snapshotStatus)  
        await new Promise(resolve => setTimeout(resolve, 1000))
        await prepareForTest({ instance })                  
      }
    }
    catch(e) {
      acapi.log.error('Bootstrap | ES | checkForSnapshot | Failed %j', e.message)
      throw new Error('checkForSnapshotFaild')
    }
    
    const response = await acapi.elasticSearch[instance].indices.delete({
      index: `${index.index}*`,
      ignore_unavailable: true
    })
    acapi.aclog.listing({
      field: 'Deleted index',
      value: _.get(response, 'body.acknowledged')
    })

    // only create mapping if the function is async
    if (_.isFunction(createMapping)) {
      let uuidIndex = `${index.index}_${uuidv4()}`
      acapi.aclog.listing({
        field: 'Creating',
        value: uuidIndex
      })
      await createMapping({ index: uuidIndex, model: index.model })
      // create alias
      let actions = [{
        add: { index: uuidIndex, alias: index.index }
      }]
      const response = await acapi.elasticSearch[instance].indices.updateAliases({
        body: {
          actions
        }
      })
      acapi.aclog.listing({
        field: 'Updated alias',
        value: index.index
      })
      acapi.aclog.listing({
        field: 'Index ready',
        value: _.get(response, 'body.acknowledged')
      })
    }
  }

  return {
    init,
    prepareForTest
  }
}