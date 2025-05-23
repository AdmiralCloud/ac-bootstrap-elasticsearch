const _ = require('lodash') 
const { v4: uuidv4 } = require('uuid')

const { defaultProvider } = require('@aws-sdk/credential-provider-node')
const { Client } = require('@opensearch-project/opensearch')
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws')

const https = require('https');
const keepAliveAgent = new https.Agent({
  keepAlive: true
});

/**
 * https://docs.aws.amazon.com/opensearch-service/latest/developerguide/request-signing.html#request-signing-node
 * https://www.npmjs.com/package/aws-opensearch-connector
 * https://opensearch-project.github.io/opensearch-js/2.1/index.html
 * @param options.createMapping FUNCTION OPTION - function to call if you want to rebuild ES indices during test
 **/

module.exports = (acapi) => {


  const getClient = async ({ instance, server, index, region = 'eu-central-1', keepAlive = true }) => {
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
      requestTimeout: _.get(acapi.config, 'elasticSearch.timeout', 30000),
    }
    if (keepAlive) esConfig.agent = keepAliveAgent

    if (!acapi.config.localElasticSearch && _.get(server, 'awsCluster')) {
      const osConnector = AwsSigv4Signer({
        service: 'es',
        region,
        // Example with AWS SDK V3:
        getCredentials: () => {
          // Any other method to acquire a new Credentials object can be used.
          const credentialsProvider = defaultProvider()
          return credentialsProvider()
        },
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
      console.error(84, e)
    }

    serverInfo.collectOnly = true
    const logCollector = acapi.aclog.serverInfo(serverInfo)
    return logCollector
  }

  const getIndexStats = async({ index }) => {
    const response = await acapi.elasticSearch[index.instance].count({
      index: index.index
    })
    return _.get(response, 'body.count')
  }

  const init = async() => {
    let logCollector = []
    
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
        logCollector = logCollector.concat(acapi.aclog.serverInfo({
          instance,
          index: _.get(index, 'index'),
          collectOnly: true
        }))
      }
      else {
        // create an elasticsearch client for your Amazon ES
        const infoLogs = await getClient({ instance, server, index, debug: _.get(index, 'debug') })
        logCollector = logCollector.concat(infoLogs)
      }
      try {
        const docCount = await getIndexStats({ index })
        logCollector.push({
          field: 'DocCount',
          value: docCount
        })
      } 
      catch (err) {
        logCollector.push({
          field: 'DocCount',
          value: _.get(err, 'message', 'Error')
        })
      }
      logCollector.push({
        line: true
      })
    }
    return logCollector
  }

  const checkForSnapshot = async({ instance }) => {
    let response = await acapi.elasticSearch[instance].snapshot.status()
    return _.get(response, 'body.error.root_cause[0].type') 
  }
  

  const prepareForTest = async({ instance, createMapping }) => {
    const index = _.find(acapi.config.elasticSearch.indices, { model: instance })
    const logCollector = []

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
    logCollector.push({
      field: 'Deleted index',
      value: _.get(response, 'body.acknowledged')
    })

    // only create mapping if the function is async
    if (_.isFunction(createMapping)) {
      let uuidIndex = `${index.index}_${uuidv4()}`
      logCollector.push({
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
      logCollector.push({
        field: 'Updated alias',
        value: index.index
      })
      logCollector.push({
        field: 'Index ready',
        value: _.get(response, 'body.acknowledged')
      })
    }
    return logCollector
  }

  return {
    init,
    prepareForTest
  }
}