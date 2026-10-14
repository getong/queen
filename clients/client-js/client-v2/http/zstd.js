/**
 * zstd for the binary push body and the framed pop answer.
 *
 * It comes from node:zlib (Node 22.15 and later), loaded on first use through
 * a guarded dynamic import, as HttpClient loads undici: a bundle for a runtime
 * without it still loads, and fails only when a push is compressed or a framed
 * pop holds a compressed record.
 */

let loading = null

async function load() {
  if (loading === null) {
    loading = import('node:zlib').then(
      (zlib) => (typeof zlib.zstdCompressSync === 'function' && typeof zlib.zstdDecompressSync === 'function' ? zlib : null),
      () => null
    )
  }
  const zlib = await loading
  if (!zlib) {
    throw new Error('zstd is not available in this runtime (node:zlib of Node 22.15 or later): pushCompression and framedPop need it')
  }
  return zlib
}

/**
 * One zstd frame of `raw`, at the fastest level. The frame carries its content
 * size, which the broker asks of a frame it stores as it came.
 */
export async function zstdCompress(raw) {
  const zlib = await load()
  return zlib.zstdCompressSync(raw, {
    params: { [zlib.constants.ZSTD_c_compressionLevel]: 1 },
    pledgedSrcSize: raw.length
  })
}

export async function zstdDecompress(frame) {
  const zlib = await load()
  return zlib.zstdDecompressSync(frame)
}
