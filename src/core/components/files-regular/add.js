'use strict'

const callbackify = require('callbackify')
const all = require('async-iterator-all')

module.exports = function (self) {
  return callbackify((data, options) => {
    return all(self._addAsyncIterator(data, options))
  })
}
