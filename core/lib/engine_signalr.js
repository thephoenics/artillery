/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

'use strict';

const async = require('async');
const _ = require('lodash');
const url = require('url');

const request = require('request');
const SignalrClient = require('signalr-client').client;

const deepEqual = require('deep-equal');
const debug = require('debug')('signalr-client');
const engineUtil = require('./engine_util');
const EngineHttp = require('./engine_http');
const template = engineUtil.template;

class SignalREngine {
  constructor(script, ee, helpers) {
    this.script = script;
    this.ee = ee;
    this.helpers = helpers;

    this.config = script.config;
    this.signalrOpts = _.extend({
      target: this._getSignalrTarget(this.config.target),
      hubs: null,
      reconnectTimeout: 10,
      autoconnect: false,
      query: {},
      retryCount: -1
    }, this.config.signalr);

    this.httpDelegate = new EngineHttp(script);
    return this;
  }

  createScenario(scenarioSpec, ee) {
    let tasks = _.map(scenarioSpec.flow, (rs) => {
      if (rs.think) {
        return engineUtil.createThink(rs, _.get(this.config, 'defaults.think', {}));
      }

      return this.step(rs, ee);
    });

    return this.compile(tasks, scenarioSpec.flow, ee);
  };

  initializeClient(ee, hub, context, cb) {
    context.clients = context.clients || {};

    let client = context.clients[hub] || context.clients[context.clientId] || null;
    if (client) {
      return cb(null, client);
    }

    if (!hub && !this.signalrOpts.autoconnect) {
      return cb(null, null);
    }

    const signalrOpts = template(this.signalrOpts, context);
    client = new SignalrClient(signalrOpts.target, [hub], signalrOpts.reconnectTimeout, true);
    client.queryString = signalrOpts.query;

    client.serviceHandlers = {
      bound: () => this._addCounter(ee, 'bound', 1, {
        hub
      }),
      connectFailed: (error) => {
        this._addCounter(ee, 'connectFailed', 1, {
          hub,
          error
        });
        cb(error, null);
      },
      connected: () => {},
      connectionLost: () => this._addCounter(ee, 'connectionLost', 1, _.extend({hub}, context.metricsContext || {})),
      disconnected: () => this._addCounter(ee, 'disconnected', 1, _.extend({hub}, context.metricsContext || {})),
      onerror: (error) => this._addCounter(ee, 'error', 1, _.extend({hub, error}, context.metricsContext || {})),
      messageReceived: (message) => {
        context.__receivedMessageCount++;
      },
      bindingError: (error) => this._addCounter(ee, 'bindingError', 1, _.extend({hub, error}, context.metricsContext || {})),
      onUnauthorized: (res) => this._addCounter(ee, 'unauthorized', 1, _.extend({hub, res}, context.metricsContext || {})),
      reconnecting: (retry) => {
        const continueRetry = this.signalrOpts.retryCount == -1 || retry.count <= this.signalrOpts.retryCount;
        this._addCounter(ee, 'reconnecting', 1, _.extend({
            hub,
            attempt: retry.count,
            continueRetry
          }, context.metricsContext || {}));

        return !continueRetry;
      },
      reconnected: (connection) => this._addCounter(ee, 'reconnected', 1, _.extend({hub}, context.metricsContext || {})),
    };

    context.clients[hub] = client;

    if (signalrOpts.autoconnect) {
      const startedAt = process.hrtime();

      client.serviceHandlers.connected = () => {
        const endedAt = process.hrtime(startedAt);
        const delta = (endedAt[0] * 1e9) + endedAt[1];

        this._addHistogram(ee, 'connected', delta, {
          hub
        });
        cb(null, client);
      };

      client.start();
    } else {
      cb(null, client);
    }
  }

  step(requestSpec, ee) {
    if (requestSpec.loop) {
      let steps = _.map(requestSpec.loop, (rs) => {
        // If it is not a step that SignalR engine can process, delegate it to HTTP engine
        if (!this._getSignalrStep(rs)) {
          return this.httpDelegate.step(rs, ee);
        }

        return this.step(rs, ee);
      });

      return engineUtil.createLoopWithCount(
        requestSpec.count || -1,
        steps, {
          loopValue: requestSpec.loopValue,
          overValues: requestSpec.over,
          whileTrue: this.config.processor ?
            this.config.processor[requestSpec.whileTrue] : undefined
        }
      );
    }

    const stepFunction = (context, callback) => {
      // Delegate think to the common think utility
      if (requestSpec.think) {
        return engineUtil.createThink(requestSpec, _.get(this.config, 'defaults.think', {}));
      }

      // Delegate other non-signalr steps to HTTP engine
      if (!this._getSignalrStep(requestSpec)) {
        const delegateFunc = this.httpDelegate.step(requestSpec, ee);
        return delegateFunc(context, callback);
      }
    };

    const step = this._getSignalrStep(requestSpec);

    const initializeStep = (context, callback) => {
      step.hub = template(step.hub, context) || null;

      this.initializeClient(ee, step.hub, context, (err, client) => {
        if (err) {
          debug(err);
          ee.emit('error', err.message);
          return callback(err, context);
        }

        if (requestSpec.connect) {
          return this._onStepConnect(requestSpec, ee, client, context, callback);
        } else if (requestSpec.invoke) {
          return this._onStepInvoke(requestSpec, ee, client, context, callback);
        } else if (requestSpec.listen) {
          return this._onStepListen(requestSpec, ee, client, context, callback);
        } else if (requestSpec.disconnect) {
          return this._onStepDisconnect(requestSpec, ee, client, context, callback);
        }

        return stepFunction(context, callback);
      });
    };

    if (step) {
      return initializeStep;
    } else {
      return stepFunction;
    }
  }

  shutdown(context) {
    for (const hub in context.clients) {
      context.clients[hub].end();
      delete context.clients[hub];
    }
  }

  compile(tasks, scenarioSpec, ee) {
    const initialTask = (callback, context) => {
      ee.emit('started');

      this.initializeClient(ee, '', context, (err) => {
        if (err) {
          ee.emit('error', err);
          return callback(err, context);
        }

        return callback(null, context);
      });
    }

    return (initialContext, callback) => {
      initialContext._successCount = 0;
      initialContext._jar = request.jar();
      initialContext._pendingRequests = _.size(
        _.reject(scenarioSpec, (rs) => (typeof rs.think === 'number')));

      let steps = _.flatten([
        (cb) => initialTask(cb, initialContext),
        tasks
      ]);

      async.waterfall(
        steps,
        (err, context) => {
          if (err) {
            debug(err);
          }
          if (context) {
            this.shutdown(context);
          }
          return callback(err, context);
        });
    };
  }

  _getSignalrTarget(target) {
    const parsedUrl = url.parse(target);
    let protocol = parsedUrl.protocol;

    if (protocol === 'https:') {
      protocol = 'wss:';
    }

    return `${protocol}//${parsedUrl.host}/signalr`;
  }

  _getSignalrStep(requestSpec) {
    return requestSpec.connect || requestSpec.invoke || requestSpec.listen || requestSpec.disconnect || null;
  }

  _addHistogram(ee, key, value, metricsContext) {
    ee.emit('histogram', `signalr.${key}`, value, metricsContext);
  }

  _addCounter(ee, key, count, metricsContext) {
    ee.emit('counter', `signalr.${key}`, count, metricsContext);
  }

  _onStepConnect(requestSpec, ee, client, context, callback) {
    const step = requestSpec.connect;

    const startedAt = process.hrtime();

    client.serviceHandlers.connected = () => {
      const endedAt = process.hrtime(startedAt);
      const delta = (endedAt[0] * 1e9) + endedAt[1];

      this._addHistogram(ee, 'connected', delta, {
        hub: step.hub
      });
      return callback(null, context);
    };

    client.start();
  }

  _onStepInvoke(requestSpec, ee, client, context, callback) {
    const step = requestSpec.invoke;
    const params = step.params || [];

    const metricsContext = _.extend(
      step.name ? {name: step.name} : {},
      {
        hub: step.hub,
        action: 'invoke',
        method: step.method
      }, requestSpec.metricsContext || {}, context.metricsContext);

    const startedAt = process.hrtime();
    ee.emit('request', metricsContext);
    
    const endCallback = (err, result) => {
      if (err) {
        debug(err);
        ee.emit('error', err, metricsContext);
        return callback(err, context);
      }

      const endedAt = process.hrtime(startedAt);
      const delta = (endedAt[0] * 1e9) + endedAt[1];
      this._addHistogram(ee, 'invoke', delta, metricsContext);

      return this._handleResponseValidation(step, ee, result, context, callback, metricsContext);
    };
    
    client.call(step.hub, step.method, ...params)
      .done(endCallback);
  }

  _onStepListen(requestSpec, ee, client, context, callback) {
    const step = requestSpec.listen;

    const metricsContext = _.extend(
      step.name ? {name: step.name} : {},
      {
        hub: step.hub,
        action: 'listen',
        method: step.method
      }, requestSpec.metricsContext || {}, context.metricsContext);

    client.on(step.hub, step.method, (...result) => {
      this._addCounter(ee, 'listen', 1, metricsContext);
      const clientCallback = step.wait ?  callback : () => {};
      return this._handleResponseValidation(step, ee, result, context, clientCallback, metricsContext);
    });

    if (!step.wait) {
      return callback(null, context);
    }
  }

  _handleResponseValidation(stepSpec, ee, result, context, callback, metricsContext) {
    if (this._needResponseValidation(stepSpec)) {
      const response = {
        data: template(stepSpec.response.data, context),
        capture: template(stepSpec.response.capture, context),
        match: template(stepSpec.response.match, context)  
      };

      this._processResponse(ee, result, response, context, (err) => {
        if (err) {
          debug(err);
          return callback(err, context);
        }

        return callback(null, context);
      });
    } else {
      return callback(null, context);
    }
  }

  _onStepDisconnect(requestSpec, ee, client, context, callback) {
    client.end();
    delete context.clients[requestSpec.disconnect.hub];
    return callback(null, context);
  }

  _processResponse(ee, data, response, context, callback, metricsContext) {
    // Do we have supplied data to validate?
    if (response.data && !deepEqual(data, response.data)) {
      debug(data);
      let err = 'data is not valid';
      ee.emit('error', err, metricsContext);
      return callback(err, context);
    }

    // If no capture or match specified, then we consider it a success at this point...
    if (!response.capture && !response.match) {
      return callback(null, context);
    }

    // Construct the (HTTP) response...
    let fauxResponse = {
      body: JSON.stringify(data)
    };

    // Handle the capture or match clauses...
    engineUtil.captureOrMatch(response, fauxResponse, context, function (err, result) {
      // Were we unable to invoke captureOrMatch?
      if (err) {
        debug(data);
        ee.emit('error', err, metricsContext);
        return callback(err, context);
      }

      // Do we have any failed matches?
      let failedMatches = _.filter(result.matches, (v, k) => !v.success);

      // How to handle failed matches?
      if (failedMatches.length) {
        // TODO: Should log the details of the match somewhere
        ee.emit('error', 'Failed match', metricsContext);
        return callback(new Error('Failed match'), context);
      } else {
        // Emit match events...
        _.each(result.matches, (v, k) => {
          ee.emit('match', v.success, {
            expected: v.expected,
            got: v.got,
            expression: v.expression
          }, metricsContext);
        });

        // Populate the context with captured values
        _.each(result.captures, (v, k) => context.vars[k] = v);

        // Replace the base object context
        // Question: Should this be JSON object or String?
        context.vars.$ = fauxResponse.body;

        // Increment the success count...
        context._successCount++;

        return callback(null, context);
      }
    });
  }

  _needResponseValidation(stepSpec) {
    return stepSpec && stepSpec.response;
  }
}


module.exports = SignalREngine;