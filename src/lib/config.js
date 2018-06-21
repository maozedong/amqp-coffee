const constants = require('./constants')
const protocol = require('./protocol')('../amqp-definitions-0-9-1')
const DEBUG = require('debug')
const DEBUG_LEVEL = parseInt(process.env.AMQP)
const debuggers = {}

/**
 *
 * @param {string} name
 * // todo how should I define return type here?
 * @returns {*}
 */
function debug (name) {
  if (DEBUG_LEVEL == null) {
    return function () {}
  } else {
    // do nothing
    /**
     *
     * @param {number|any} level
     * @param {any=} message
     * @returns {*}
     */
    function debugWorker (level, message) {
      if ((message == null) && (level != null)) {
        message = level
        level = 1
      }
      if (level <= DEBUG_LEVEL) {
        if (!debuggers[name]) {
          debuggers[name] = DEBUG(name)
        }
        if (typeof message === 'function') {
          message = message()
        }
        return debuggers[name](message)
      } else {
        return function () {}
      }
    }
    return debugWorker
  }
}

// do nothing
module.exports = {constants, protocol, debug}
