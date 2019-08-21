'use strict'

const callbackify = require('callbackify')
const globSource = require('../../utils/files/glob-source')
const all = require('async-iterator-all')

module.exports = self => {
  return callbackify.variadic(async (...args) => {
    const options = typeof args[args.length - 1] === 'string' ? {} : args.pop()

    return all(self._addAsyncIterator(globSource(...args, options), options))
  })
}
