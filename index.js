const async = require('async')
const _ = require('lodash') 
const { v4: uuidv4 } = require('uuid');

const { Client, Connection } = require('@elastic/elasticsearch')
const AWS = require('aws-sdk')

/**
 * 
 * @param options.providerConfigType STRING OPTIONAL - set type of aws accessKeys, defaults to secrets
 * @param options.createMapping FUNCTION OPTION - function to call if you want to rebuild ES indices during test
 **/

module.exports = (acapi) => {

  const init = function(options, cb) {
    const providerConfig = _.find(_.get(acapi.config, 'aws.accessKeys'), { default: true, type: _.get(options, 'providerConfigType', 'secrets') })

    acapi.aclog.headline({ headline: 'ELASTICSEARCH' })
    
    // init multiple instances for different purposes
    acapi.elasticSearch = {}
  
        // init multiple instances for different purposes
    async.eachSeries(acapi.config.elasticSearch.indices, (index, itDone) => {
      if (acapi.config.environment === 'test' && _.get(index, 'omitInTest')) return itDone()
      let instance = _.get(index, 'instance')
      let server = _.find(acapi.config.elasticSearch.servers, { server: _.get(index, 'server') })
      if (!server) return itDone('serverConfigurationMissingForES')

      // update config with environment if not global
      let pos = _.findIndex(acapi.config.elasticSearch.indices, { model: index.model })
      index.index = (!index.global ? acapi.config.environment + '_' : '') + (process.env.NODE_TEST_ORIGIN ? process.env.NODE_TEST_ORIGIN + '_' : '' ) + (index.indexInfix || index.model)
      acapi.config.elasticSearch.indices.splice(pos, 1, index)

      // check if instance is already created
      if (_.has(acapi.elasticSearch, instance)) {
        acapi.aclog.serverInfo({
          instance,
          index: _.get(index, 'index')
        })
        return itDone()
      }

      // instanciate ES for this database/index

      let url = (_.get(acapi.config, 'localElasticSearch.protocol') || _.get(server, 'protocol', 'https')) + '://' +(_.get(acapi.config, 'localElasticSearch.host') ||  _.get(server, 'host', 9200)) + ':' + (_.get(acapi.config, 'localElasticSearch.port') ||  _.get(server, 'port'))
      let esConfig = {
        node: {
          url: new URL(url),
          ssl: {
            // allow different certificate for SSH tunnel on NON-production system
            rejectUnauthorized: acapi.config.environment === 'production'
          }  
        },
        auth: _.get(server, 'auth'),
        requestTimeout: acapi.config.elasticSearch.timeout,
        providerConfig
      }

      class AwsConnector extends Connection {
        async request(params, callback) {
          try {
            const creds = providerConfig
            const req = this.createRequest(params);
      
            const { request: signedRequest } = this.signRequest(req, creds);
            super.request(signedRequest, callback);
          }
          catch (error) {
            acapi.log.error('AWSESConnector | Request failed %j', error)
            throw error;
          }
        }
      
        createRequest(params) {
          const endpoint = new AWS.Endpoint(this.url.href);
          let req = new AWS.HttpRequest(endpoint);

          Object.assign(req, params);
          if (req.querystring) {
            req.path += `/?${req.querystring}`;
            delete req.querystring;  
          }

          req.region = _.get(providerConfig, 'region', 'eu-central-1')
      
          if (!req.headers) {
            req.headers = {};
          }
      
          let body = params.body;
      
          if (body) {
            let contentLength = Buffer.isBuffer(body)
              ? body.length
              : Buffer.byteLength(body);
            req.headers['Content-Length'] = contentLength;
            req.body = body;
          }
          req.headers['Host'] = (acapi.config.environment !== 'production' ? 'localhost' : endpoint.host)        
          return req;
        }
      
        signRequest(request, creds) {
          const signer = new AWS.Signers.V4(request, 'es');
          signer.addAuthorization(creds, new Date());
          return signer;
        }
      }
      if (!acapi.config.localElasticSearch && _.get(server, 'awsCluster')) _.set(esConfig, 'Connection', AwsConnector)

      // create an elasticsearch client for your Amazon ES
      acapi.elasticSearch[instance] = new Client(esConfig)

      let esData = {}
      async.series({
        checkVersion: (done) => {
          acapi.elasticSearch[instance].cluster.stats((err, result) => {
            if (err) {
              acapi.log.error('ES | Method %s | Path %s', _.get(err, 'meta.meta.request.params.method'), _.get(err, 'meta.meta.request.params.path'))
              acapi.log.error('ES | Error %j', _.get(err, 'meta.body'))   
              return done(err)
            }
            _.merge(esData, _.get(result, 'body'))
            return done()
          })
        },
        checkIndex: (done) => {
          acapi.elasticSearch[instance].indices.exists({ index: _.get(index, 'index') }, done)
        }
      }, (err) => {
        if (err) return itDone(err)
        acapi.aclog.serverInfo({
          instance,
          index: _.get(index, 'index'),
          host: _.get(server, 'host'),
          port: _.get(server, 'port'),
          cluster: _.get(esData, 'cluster_name'),
          clusterVersion: _.get(esData, 'version.number')
        })
        return itDone()
      })
    }, cb)
  }

  const prepareForTest = (params, cb) => {
    const instance = _.get(params, 'instance')
    const index = _.find(acapi.config.elasticSearch.indices, { model: instance })

    // reset ES in for tests
    // check for snapshot and wait (if not localDevelopment) - otherwise tests will fail
    let esReady = false
    async.whilst(
      (wcb) => {
        return wcb(null, !esReady)
      },
      (callback) => {
        setTimeout(() => {
          acapi.elasticSearch[instance].snapshot.status({
          }, (err, result) => {
            if (err) return callback(err)
            if (_.get(result, 'error.root_cause[0].type') ===  'snapshot_in_progress_exception') {
              acapi.log.warn('Bootstrap | ES | Cluster is snapshotting... we are waiting | %j', result)                    
            }
            else {
              esReady = true
            }
            return callback()
          })
        }, 1000)
      }, (err) => {
        if (err) return cb(err)

        acapi.log.info('Bootstrap | ES | prepareForTest | Deleting index | %s', `${index.index}*`)            
        acapi.elasticSearch[instance].indices.delete({
          index: `${index.index}*`,
          ignore_unavailable: true
        }, (err) => {
          if (err) return cb(err)
          if (_.isFunction(_.get(params, 'createMapping'))) {
            let uuidIndex = `${index.index}_${uuidv4()}`
            acapi.log.info('Bootstrap | ES | prepareForTest | Create index | Model %s | %s', `${index.model}`, uuidIndex)            
            _.get(params, 'createMapping')({ index: uuidIndex, model: index.model }, err => {
              if (err) return cb(err)
              // create alias
              let actions = [{
                add: { index: uuidIndex, alias: index.index }
              }]
              acapi.log.info('Bootstrap | ES | prepareForTest | Create alias | %j', actions)            
              acapi.elasticSearch[instance].indices.updateAliases({
                body: {
                  actions
                }
              }, cb)
            })
          }
          else {
            return cb()
          }
        })    
      })
  }


  return {
    init,
    prepareForTest
  }
}