/**
 * A real node:http server for the binary wire forms: it records every request
 * with its body as bytes (kv-unit/_planServer.js reads bodies as text, which a
 * binary push body does not survive) and answers what `answer(hit)` returns:
 * `{ status, contentType, body }`, body a Buffer or a string.
 */

import { createServer } from 'node:http'

export function withWireServer(answer, run) {
  const hits = []

  const server = createServer((req, res) => {
    const chunks = []
    req.on('data', chunk => chunks.push(chunk))
    req.on('end', () => {
      const hit = { method: req.method, url: req.url, headers: req.headers, raw: Buffer.concat(chunks) }
      hits.push(hit)
      const { status = 200, contentType = 'application/json', body = '' } = answer(hit) || {}
      if (status === 204) {
        res.writeHead(204)
        res.end()
        return
      }
      res.writeHead(status, { 'Content-Type': contentType })
      res.end(body)
    })
  })

  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      const teardown = () => new Promise(r => server.close(r))
      Promise.resolve()
        .then(() => run(`http://127.0.0.1:${port}`, hits))
        .then(
          async (value) => { await teardown(); resolve(value) },
          async (err) => { await teardown(); reject(err) }
        )
    })
  })
}

export function query(url) {
  const i = url.indexOf('?')
  return new URLSearchParams(i < 0 ? '' : url.slice(i + 1))
}
