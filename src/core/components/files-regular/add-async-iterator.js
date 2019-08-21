'use strict'

const importer = require('ipfs-unixfs-importer')
const isSource = require('is-pull-stream').isSource
const validateAddInput = require('ipfs-utils/src/files/add-input-validation')
const { parseChunkerString } = require('./utils')
const pipe = require('it-pipe')
const { supportsFileReader } = require('ipfs-utils/src/supports')
const toAsyncIterator = require('pull-stream-to-async-iterator')
const log = require('debug')('ipfs:add')
log.error = require('debug')('ipfs:add:error')

function noop () {}

module.exports = function (self) {
  // Internal add func that gets used by all add funcs
  return async function * addAsyncIterator (source, options) {
    options = options || {}

    let chunkerOptions = parseChunkerString(options.chunker)

    const opts = Object.assign({}, {
      shardSplitThreshold: self._options.EXPERIMENTAL.sharding
        ? 1000
        : Infinity
    }, options, {
      chunker: chunkerOptions.chunker,
      chunkerOptions: chunkerOptions.chunkerOptions
    })

    // CID v0 is for multihashes encoded with sha2-256
    if (opts.hashAlg && opts.cidVersion !== 1) {
      opts.cidVersion = 1
    }

    let total = 0

    const prog = opts.progress || noop
    const progress = (bytes) => {
      total += bytes
      prog(total)
    }

    opts.progress = progress

    if (Buffer.isBuffer(source) || typeof source === 'string') {
      source = [
        source
      ]
    }

    const iterator = pipe(
      source,
      validateInput(),
      normalizeInput(opts),
      doImport(self, opts),
      prepareFile(self, opts),
      preloadFile(self, opts),
      pinFile(self, opts)
    )

    const releaseLock = await self._gcLock.readLock()

    try {
      yield * iterator
    } finally {
      releaseLock()
    }
  }
}

function validateInput () {
  return async function * (source) {
    for await (const data of source) {
      validateAddInput(data)

      yield data
    }
  }
}

function normalizeContent (content) {
  if (supportsFileReader && kindOf(content) === 'file') {
    return streamFromFileReader(content)
  }

  // pull stream source
  if (isSource(content)) {
    return toAsyncIterator(content)
  }

  if (typeof content === 'string') {
    return Buffer.from(content)
  }

  if (Array.isArray(content) && content.length && !Array.isArray(content[0])) {
    return [content]
  }

  return content
}

function normalizeInput (opts) {
  return async function * (source) {
    for await (let data of source) {
      if (data.content) {
        data.content = normalizeContent(data.content)
      } else {
        data = {
          path: '',
          content: normalizeContent(data)
        }
      }

      if (opts.wrapWithDirectory && !data.path) {
        throw new Error('Must provide a path when wrapping with a directory')
      }

      yield data
    }
  }
}

function doImport (ipfs, opts) {
  return function (source) {
    return importer(source, ipfs._ipld, opts)
  }
}

function prepareFile (ipfs, opts) {
  return async function * (source) {
    for await (const file of source) {
      let cid = file.cid
      const hash = cid.toBaseEncodedString()
      let path = file.path ? file.path : hash

      if (opts.wrapWithDirectory && !file.path) {
        path = ''
      }

      if (opts.onlyHash) {
        yield {
          path,
          hash,
          size: file.unixfs.fileSize()
        }

        return
      }

      const node = await ipfs.object.get(file.cid, Object.assign({}, opts, { preload: false }))

      if (opts.cidVersion === 1) {
        cid = cid.toV1()
      }

      let size = node.size

      if (Buffer.isBuffer(node)) {
        size = node.length
      }

      yield {
        path,
        hash,
        size
      }
    }
  }
}

function preloadFile (ipfs, opts) {
  return async function * (source) {
    for await (const file of source) {
      const isRootFile = !file.path || opts.wrapWithDirectory
        ? file.path === ''
        : !file.path.includes('/')

      const shouldPreload = isRootFile && !opts.onlyHash && opts.preload !== false

      if (shouldPreload) {
        ipfs._preload(file.hash)
      }

      yield file
    }
  }
}

function pinFile (ipfs, opts) {
  return async function * (source) {
    for await (const file of source) {
      // Pin a file if it is the root dir of a recursive add or the single file
      // of a direct add.
      const pin = 'pin' in opts ? opts.pin : true
      const isRootDir = !file.path.includes('/')
      const shouldPin = pin && isRootDir && !opts.onlyHash && !opts.hashAlg

      if (shouldPin) {
        await ipfs.pin.add(file.hash, { preload: false })
      }

      yield file
    }
  }
}
