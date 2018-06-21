const should = require('should')
const async = require('async')
const _ = require('underscore')
// todo do we need this?
require('./proxy')
const uuid = require('uuid').v4
const AMQP = require('../src/amqp')
const {MaxFrameSize} = require('../src/lib/config').constants

describe('Publisher', function () {
  this.timeout(15000)
  it('test we can publish a message in confirm mode', function (done) {
    var amqp, queue
    amqp = null
    queue = uuid()
    return async.series([
      (next) => {
        return amqp = new AMQP({host: 'localhost'},
          (e) => {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.publish('amq.direct',
          queue,
          'test message',
          {
            confirm: true
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      }
    ], done)
  })
  it('we can publish a series of messages in confirm mode', function (done) {
    var amqp, queue
    amqp = null
    queue = uuid()
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return async.forEach((function () {
            var results = []
            for (var k = 0; k < 100; k++) { results.push(k) }
            return results
          }).apply(this),
          function (i,
                    done) {
            return amqp.publish('amq.direct',
              queue,
              'test message',
              {
                confirm: true
              },
              function (e,
                        r) {
                should.not.exist(e)
                return done()
              })
          },
          next)
      }
    ], done)
  })
  it('we can agressivly publish a series of messages in confirm mode 214', function (done) {
    var amqp, queue
    amqp = null
    queue = uuid()
    done = _.once(done)
    return amqp = new AMQP({
      host: 'localhost'
    }, function (e, r) {
      should.not.exist(e)
      return amqp.queue({queue}, function (e, q) {
        return q.declare(function () {
          return q.bind('amq.direct', queue, function () {
            var i, j, results
            i = 0
            j = 0
            results = []
            while (i <= 100) {
              amqp.publish('amq.direct', queue, {
                b: Buffer.allocUnsafe(500)
              }, {
                deliveryMode: 2,
                confirm: true
              }, function (e, r) {
                should.not.exist(e)
                j++
                if (j >= 100) {
                  return done()
                }
              })
              results.push(i++)
            }
            return results
          })
        })
      })
    })
  })
  it('test we can publish a message a string', function (done) {
    var amqp, queue
    amqp = null
    queue = uuid()
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.publish('amq.direct',
          queue,
          'test message',
          {},
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      }
    ], done)
  })
  it('test we can publish without waiting for a connection', function (done) {
    var amqp, queue
    amqp = null
    queue = uuid()
    amqp = new AMQP({
      host: 'localhost'
    }, function (e, r) {
      return should.not.exist(e)
    })
    return amqp.publish('amq.direct', queue, 'test message', {}, function (e, r) {
      should.not.exist(e)
      return done()
    })
  })
  // it 'test we can publish a big string message', (done)->
  //   amqp = null
  //   queue = uuid()
  //   async.series [
  //     (next)->
  //       amqp = new AMQP {host:'localhost'}, (e, r)->
  //         should.not.exist e
  //         next()

  //     (next)->
  //       amqp.publish "amq.direct", queue, "test message #{Buffer.alloc(10240000).toString()}", {confirm: true}, (e,r)->
  //         should.not.exist e
  //         next()

  //   ], done
  it('test we can publish a JSON message', function (done) {
    var amqp, queue
    amqp = null
    queue = uuid()
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.publish('amq.direct',
          queue,
          {
            look: 'im jason',
            jason: 'nope'
          },
          {},
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      }
    ], done)
  })
  it('test we can publish a string message 413', function (done) {
    var amqp, content, queueName
    amqp = null
    queueName = uuid()
    content = 'ima string'
    amqp = null
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.queue({
            queue: queueName
          },
          function (err,
                    queue) {
            queue.declare()
            return queue.bind('amq.direct',
              queueName,
              next)
          })
      },
      function (next) {
        return amqp.publish('amq.direct',
          queueName,
          content,
          {},
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.consume(queueName,
          {},
          function (message) {
            message.data.should.eql(content)
            return next()
          })
      }
    ], done)
  })
  it('test we can publish a buffer message', function (done) {
    var amqp, queue
    amqp = null
    queue = uuid()
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.publish('amq.direct',
          queue,
          Buffer.alloc(15),
          {},
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      }
    ], done)
  })
  it('test we can publish a buffer message that need to be multiple data packets', function (done) {
    var amqp, packetSize, queue
    amqp = null
    queue = uuid()
    packetSize = MaxFrameSize * 2.5
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.publish('amq.direct',
          uuid(),
          Buffer.alloc(packetSize),
          {},
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      }
    ], done)
  })
  it('test we can publish a message size 344', function (done) {
    var amqp, packetSize, queue
    amqp = null
    queue = uuid()
    packetSize = 344
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.publish('amq.direct',
          uuid(),
          Buffer.alloc(packetSize),
          {
            confirm: true
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      }
    ], done)
  })
  it('test we can publish a lots of messages in confirm mode 553', function (done) {
    var amqp, packetSize, queue
    this.timeout(5000)
    amqp = null
    queue = uuid()
    packetSize = 344
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return async.forEach((function () {
            var results = []
            for (var k = 0; k < 1000; k++) { results.push(k) }
            return results
          }).apply(this),
          function (i,
                    next) {
            return amqp.publish('amq.direct',
              `queue-${i}`,
              Buffer.alloc(packetSize),
              {
                confirm: true
              },
              function (e,
                        r) {
                should.not.exist(e)
                return next()
              })
          },
          next)
      }
    ], done)
  })
  it('test we can publish a lots of messages in confirm mode quickly 187', function (done) {
    var amqp, packetSize, queue
    this.timeout(5000)
    amqp = null
    queue = uuid()
    packetSize = 256837
    return amqp = new AMQP({
      host: 'localhost'
    }, function (e, r) {
      should.not.exist(e)
      return async.forEach([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], function (i, next) {
        return amqp.publish('amq.direct', `queue-${i}`, Buffer.alloc(packetSize), {
          confirm: true
        }, function (e, r) {
          should.not.exist(e)
          return next()
        })
      }, done)
    })
  })
  it('test we can publish a mandatory message to a invalid route and not crash 188', function (done) {
    var amqp, queue
    amqp = null
    queue = null
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.publish('amq.direct',
          'idontExist',
          Buffer.alloc(50),
          {
            confirm: true,
            mandatory: true
          },
          function (e,
                    r) {
            should.exist(e)
            e.replyCode.should.eql(312)
            return next()
          })
      }
    ], done)
  })
  it('test we can publish many mandatory messages to a some invalid routes 189', function (done) {
    var amqp, queue, queueName
    amqp = null
    queue = null
    queueName = uuid()
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.queue({
            queue: queueName
          },
          function (err,
                    queue) {
            queue.declare()
            return queue.bind('amq.direct',
              queueName,
              next)
          })
      },
      function (next) {
        return async.parallel([
            function (next) {
              return async.forEachSeries((function () {
                  var results = []
                  for (var k = 0; k < 100; k++) { results.push(k) }
                  return results
                }).apply(this),
                function (i,
                          next) {
                  return amqp.publish('amq.direct',
                    'idontExist',
                    Buffer.alloc(50),
                    {
                      confirm: true,
                      mandatory: true
                    },
                    function (e,
                              r) {
                      should.exist(e)
                      e.replyCode.should.eql(312)
                      return next()
                    })
                },
                next)
            },
            function (next) {
              return async.forEachSeries((function () {
                  var results = []
                  for (var k = 0; k < 100; k++) { results.push(k) }
                  return results
                }).apply(this),
                function (i,
                          next) {
                  return amqp.publish('amq.direct',
                    queueName,
                    Buffer.alloc(50),
                    {
                      confirm: true,
                      mandatory: true
                    },
                    function (e,
                              r) {
                      should.not.exist(e)
                      return next()
                    })
                },
                next)
            }
          ],
          next)
      }
    ], done)
  })
  it('test we can publish quickly to multiple queues shared options 1891', function (done) {
    var amqp, queue, queueName1, queueName2
    amqp = null
    queue = null
    queueName1 = 'testpublish1'
    queueName2 = 'testpublish2'
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return amqp.queue({
            queue: queueName1
          },
          function (err,
                    queue) {
            queue.declare()
            return queue.bind('amq.direct',
              queueName1,
              next)
          })
      },
      function (next) {
        return amqp.queue({
            queue: queueName2
          },
          function (err,
                    queue) {
            queue.declare()
            return queue.bind('amq.direct',
              queueName2,
              next)
          })
      },
      function (next) {
        var options
        options = {
          confirm: true,
          mandatory: false
        }
        return async.forEach([0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
          function (i,
                    next) {
            return amqp.publish('amq.direct',
              ['testpublish',
                (i % 2) + 1].join(''),
              Buffer.alloc(50),
              options,
              function (e,
                        r) {
                should.not.exist(e)
                return next()
              })
          },
          next)
      },
      function (next) {
        var consumer,
          consumer2,
          messageProcessor,
          q1count,
          q2count
        q1count = 0
        q2count = 0
        messageProcessor = function (message) {
          if (message.routingKey === queueName1) {
            q1count++
          }
          if (message.routingKey === queueName2) {
            q2count++
          }
          if (q1count === 5 && q2count === 5) {
            return next()
          }
        }
        consumer = amqp.consume(queueName1,
          {},
          messageProcessor,
          function (e,
                    r) {
            return should.not.exist(e)
          })
        return consumer2 = amqp.consume(queueName2,
          {},
          messageProcessor,
          function (e,
                    r) {
            return should.not.exist(e)
          })
      }
    ], function (e, res) {
      amqp.close()
      return done()
    })
  })
  it('test when be publishing and an out of order op happens we recover', function (done) {
    var amqp, consumer, messageProcessor, messagesRecieved, q, queue, testData
    this.timeout(10000)
    amqp = null
    testData = {
      test: 'message'
    }
    amqp = null
    queue = uuid()
    messagesRecieved = 0
    consumer = null
    q = null
    messageProcessor = function (m) {
      m.data.should.eql(testData)
      messagesRecieved++
      if (messagesRecieved === 3) {
        q.connection.crashOOO()
      }
      if (messagesRecieved === 55) {
        done()
      }
      return m.ack()
    }
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return q = amqp.queue({
            queue,
            autoDelete: false
          },
          function (e,
                    q) {
            return q.declare(function () {
              return q.bind('amq.direct',
                queue,
                next)
            })
          })
      },
      function (next) {
        return async.forEach([0, 1, 2, 3, 4],
          function (i,
                    done) {
            return amqp.publish('amq.direct',
              queue,
              testData,
              {
                confirm: true
              },
              function (err,
                        res) {
                if (err == null) {
                  return done()
                } else {
                  return setTimeout(function () {
                      return amqp.publish('amq.direct',
                        queue,
                        testData,
                        {
                          confirm: true
                        },
                        function (err,
                                  res) {
                          // console.error "*#{i}", err, res
                          return done(err,
                            res)
                        })
                    },
                    200)
                }
              })
          },
          next)
      },
      function (next) {
        return consumer = amqp.consume(queue,
          {
            prefetchCount: 1
          },
          messageProcessor,
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return async.forEach((function () {
            var results = []
            for (var k = 0; k < 50; k++) { results.push(k) }
            return results
          }).apply(this),
          function (i,
                    done) {
            return amqp.publish('amq.direct',
              queue,
              testData,
              {
                confirm: true
              },
              function (err,
                        res) {
                if (err == null) {
                  return done()
                } else {
                  return setTimeout(function () {
                      return amqp.publish('amq.direct',
                        queue,
                        testData,
                        {
                          confirm: true
                        },
                        function (err,
                                  res) {
                          // console.error "*#{i}", err, res
                          return done(err,
                            res)
                        })
                    },
                    200)
                }
              })
          },
          next)
      }
    ], function (err, res) {
      // console.error "DONE AT THE END HERE", err, res
      return should.not.exist(err)
    })
  })
  return it('test when an out of order op happens while publishing large messages we recover 915', function (done) {
    var amqp, consumer, messageProcessor, messagesRecieved, q, queue, testData
    this.timeout(10000)
    amqp = null
    testData = {
      test: 'message',
      size: Buffer.alloc(1000)
    }
    amqp = null
    queue = uuid()
    messagesRecieved = 0
    consumer = null
    q = null
    messageProcessor = function (m) {
      // m.data.should.eql testData
      messagesRecieved++
      if (messagesRecieved === 100) {
        q.connection.crashOOO()
      }
      if (messagesRecieved === 500) {
        done()
      }
      return m.ack()
    }
    return async.series([
      function (next) {
        return amqp = new AMQP({
            host: 'localhost'
          },
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return q = amqp.queue({
            queue,
            autoDelete: false
          },
          function (e,
                    q) {
            return q.declare(function () {
              return q.bind('amq.direct',
                queue,
                next)
            })
          })
      },
      function (next) {
        return consumer = amqp.consume(queue,
          {
            prefetchCount: 1
          },
          messageProcessor,
          function (e,
                    r) {
            should.not.exist(e)
            return next()
          })
      },
      function (next) {
        return async.forEach((function () {
            var results = []
            for (var k = 0; k < 500; k++) { results.push(k) }
            return results
          }).apply(this),
          function (i,
                    done) {
            return amqp.publish('amq.direct',
              queue,
              testData,
              {
                confirm: true
              },
              function (err,
                        res) {
                if (err == null) {
                  return done()
                } else {
                  return setTimeout(function () {
                      return amqp.publish('amq.direct',
                        queue,
                        testData,
                        {
                          confirm: true
                        },
                        function (err,
                                  res) {
                          return done(err,
                            res)
                        })
                    },
                    200)
                }
              })
          },
          next)
      }
    ], function (err, res) {
      // console.error "DONE AT THE END HERE", err, res
      return should.not.exist(err)
    })
  })
})
