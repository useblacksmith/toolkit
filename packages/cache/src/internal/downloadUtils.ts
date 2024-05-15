import * as core from '@actions/core'
import {HttpClient, HttpClientResponse} from '@actions/http-client'
import {BlockBlobClient} from '@azure/storage-blob'
import {TransferProgressEvent} from '@azure/ms-rest-js'
import * as buffer from 'buffer'
import * as fs from 'fs'
import * as stream from 'stream'
import * as util from 'util'

import {SocketTimeout} from './constants'
import {DownloadOptions} from '../options'
import {retryHttpClientResponse} from './requestUtils'

import {AbortController} from '@azure/abort-controller'
import axiosRetry from 'axios-retry'
import axios, {AxiosResponse} from 'axios'

/**
 * Pipes the body of a HTTP response to a stream
 *
 * @param response the HTTP response
 * @param output the writable stream
 */
async function pipeResponseToStream(
  response: HttpClientResponse,
  output: NodeJS.WritableStream,
  progress?: DownloadProgress
): Promise<void> {
  const pipeline = util.promisify(stream.pipeline)
  const reportProgress = new stream.Transform({
    transform(chunk, _encoding, callback) {
      if (progress) {
        progress.setReceivedBytes(progress.getTransferredBytes() + chunk.length)
      }
      this.push(chunk)
      callback()
    }
  })
  await pipeline(response.message, reportProgress, output)
}

async function pipeAxiosResponseToStream(
  response: AxiosResponse,
  output: NodeJS.WritableStream,
  progress?: DownloadProgress
): Promise<void> {
  const reportProgress = new stream.Transform({
    transform(chunk, _encoding, callback) {
      if (progress) {
        progress.setReceivedBytes(progress.getTransferredBytes() + chunk.length)
      }
      this.push(chunk)
      callback()
    }
  })
  await response.data.pipe(reportProgress).pipe(output)
}

/**
 * Class for tracking the download state and displaying stats.
 */
export class DownloadProgress {
  contentLength: number
  segmentIndex: number
  segmentSize: number
  segmentOffset: number
  receivedBytes: number
  startTime: number
  displayedComplete: boolean
  timeoutHandle?: ReturnType<typeof setTimeout>

  constructor(contentLength: number) {
    this.contentLength = contentLength
    this.segmentIndex = 0
    this.segmentSize = 0
    this.segmentOffset = 0
    this.receivedBytes = 0
    this.displayedComplete = false
    this.startTime = Date.now()
  }

  /**
   * Progress to the next segment. Only call this method when the previous segment
   * is complete.
   *
   * @param segmentSize the length of the next segment
   */
  nextSegment(segmentSize: number): void {
    this.segmentOffset = this.segmentOffset + this.segmentSize
    this.segmentIndex = this.segmentIndex + 1
    this.segmentSize = segmentSize
    this.receivedBytes = 0

    core.debug(
      `Downloading segment at offset ${this.segmentOffset} with length ${this.segmentSize}...`
    )
  }

  /**
   * Sets the number of bytes received for the current segment.
   *
   * @param receivedBytes the number of bytes received
   */
  setReceivedBytes(receivedBytes: number): void {
    this.receivedBytes = receivedBytes
  }

  /**
   * Returns the total number of bytes transferred.
   */
  getTransferredBytes(): number {
    return this.segmentOffset + this.receivedBytes
  }

  /**
   * Returns true if the download is complete.
   */
  isDone(): boolean {
    return this.getTransferredBytes() === this.contentLength
  }

  /**
   * Prints the current download stats. Once the download completes, this will print one
   * last line and then stop.
   */
  display(): void {
    if (this.displayedComplete) {
      return
    }

    const transferredBytes = this.segmentOffset + this.receivedBytes
    const percentage = (100 * (transferredBytes / this.contentLength)).toFixed(
      1
    )
    const elapsedTime = Date.now() - this.startTime
    const downloadSpeed = (
      transferredBytes /
      (1024 * 1024) /
      (elapsedTime / 1000)
    ).toFixed(1)

    core.info(
      `Received ${transferredBytes} of ${this.contentLength} (${percentage}%), ${downloadSpeed} MBs/sec`
    )

    if (this.isDone()) {
      this.displayedComplete = true
    }
  }

  /**
   * Returns a function used to handle TransferProgressEvents.
   */
  onProgress(): (progress: TransferProgressEvent) => void {
    return (progress: TransferProgressEvent) => {
      this.setReceivedBytes(progress.loadedBytes)
    }
  }

  /**
   * Starts the timer that displays the stats.
   *
   * @param delayInMs the delay between each write
   */
  startDisplayTimer(delayInMs = 1000): void {
    const displayCallback = (): void => {
      this.display()

      if (!this.isDone()) {
        this.timeoutHandle = setTimeout(displayCallback, delayInMs)
      }
    }

    this.timeoutHandle = setTimeout(displayCallback, delayInMs)
  }

  /**
   * Stops the timer that displays the stats. As this typically indicates the download
   * is complete, this will display one last line, unless the last line has already
   * been written.
   */
  stopDisplayTimer(): void {
    if (this.timeoutHandle) {
      clearTimeout(this.timeoutHandle)
      this.timeoutHandle = undefined
    }

    this.display()
  }
}

export async function downloadCacheAxiosMultiPart(
  archiveLocation: string,
  archivePath: string
): Promise<void> {
  const CONCURRENCY = 10
  core.info(`Downloading with ${CONCURRENCY} concurrent requests`)
  // Open a file descriptor for the cache file
  const fdesc = await fs.promises.open(archivePath, 'w+')
  // Set file permissions so that other users can untar the cache
  await fdesc.chmod(0o644)
  let progressLogger

  // Configure axios-retry
  axiosRetry(axios, {
    retries: 3,
    // No retry delay for axios-retry.
    shouldResetTimeout: true,
    retryCondition: error => {
      // Retry on all errors except 404.
      return error.response?.status !== 404
    }
  })

  try {
    core.debug(`Downloading from ${archiveLocation} to ${archivePath}`)
    const metadataResponse: AxiosResponse = await axios.get(archiveLocation, {
      headers: {Range: 'bytes=0-1'}
    })

    const contentRangeHeader = metadataResponse.headers['content-range']
    if (!contentRangeHeader) {
      throw new Error(
        'Content-Range is not defined; unable to determine file size'
      )
    }

    // Parse the total file size from the Content-Range header
    const fileSize = parseInt(contentRangeHeader.split('/')[1])
    if (isNaN(fileSize)) {
      throw new Error(
        `Content-Range is not a number; unable to determine file size: ${contentRangeHeader}`
      )
    }
    core.info(`Cached file size: ${fileSize}`)

    // Truncate the file to the correct size
    await fdesc.truncate(fileSize)
    await fdesc.sync()

    // Now that we've truncated the file to the correct size, we can close the file descriptor.
    await fdesc.close()

    progressLogger = new DownloadProgress(fileSize)
    progressLogger.startDisplayTimer()

    core.info(`Downloading ${archivePath}`)
    // Divvy up the download into chunks based on CONCURRENCY
    const chunkSize = Math.ceil(fileSize / CONCURRENCY)
    const chunkRanges: string[] = []
    for (let i = 0; i < CONCURRENCY; i++) {
      const start = i * chunkSize
      const end = i === CONCURRENCY - 1 ? fileSize - 1 : (i + 1) * chunkSize - 1
      chunkRanges.push(`bytes=${start}-${end}`)
    }

    const downloads = chunkRanges.map(async range => {
      core.debug(`Downloading range: ${range}`)
      const response: AxiosResponse = await axios.get(archiveLocation, {
        headers: {Range: range},
        responseType: 'stream'
      })
      const reportProgress = new stream.Transform({
        transform(chunk, _encoding, callback) {
          if (progressLogger) {
            progressLogger.setReceivedBytes(
              progressLogger.getTransferredBytes() + chunk.length
            )
          }
          this.push(chunk)
          callback()
        }
      })

      const chunkFileDesc = await fs.promises.open(archivePath, 'r+')
      try {
        const finished = util.promisify(stream.finished)
        const writer = fs.createWriteStream(archivePath, {
          fd: chunkFileDesc.fd,
          start: parseInt(range.split('=')[1].split('-')[0]),
          autoClose: false
        })
        await response.data.pipe(reportProgress).pipe(writer)
        await finished(writer)
      } catch (err) {
        core.warning(`Range ${range} failed to download: ${err.message}`)
        throw err
      } finally {
        if (chunkFileDesc) {
          try {
            await chunkFileDesc.close()
          } catch (err) {
            core.warning(`Failed to close file descriptor: ${err}`)
          }
        }
      }
    })

    await Promise.all(downloads)
  } catch (err) {
    core.warning(`Failed to download cache: ${err.message}`)
    throw err
  } finally {
    progressLogger?.stopDisplayTimer(true)
  }
}

/**
 * Download the cache using the Actions toolkit http-client
 *
 * @param archiveLocation the URL for the cache
 * @param archivePath the local path where the cache is saved
 */
export async function downloadCacheHttpClient(
  archiveLocation: string,
  archivePath: string
): Promise<void> {
  const CONCURRENCY = 1
  const fdesc = await fs.promises.open(archivePath, 'w+')
  // Set file permissions so that other users can untar the cache
  await fdesc.chmod(0o644)
  let progressLogger
  try {
    core.debug(`Downloading from ${archiveLocation} to ${archivePath}`)
    const httpClient = new HttpClient('useblacksmith/cache')
    const metadataResponse = await retryHttpClientResponse(
      'downloadCache',
      async () =>
        httpClient.get(archiveLocation, {
          Range: 'bytes=0-1'
        })
    )
    // Abort download if no traffic received over the socket.
    metadataResponse.message.socket.setTimeout(SocketTimeout, () => {
      metadataResponse.message.destroy()
      core.debug(
        `Aborting download, socket timed out after ${SocketTimeout} ms`
      )
    })

    const contentRangeHeader = metadataResponse.message.headers['content-range']
    if (!contentRangeHeader) {
      throw new Error(
        'Content-Range is not defined; unable to determine file size'
      )
    }

    // Parse the total file size from the Content-Range header
    const fileSize = parseInt(contentRangeHeader.split('/')[1])
    if (isNaN(fileSize)) {
      throw new Error(
        `Content-Range is not a number; unable to determine file size: ${contentRangeHeader}`
      )
    }
    core.debug(`fileSize: ${fileSize}`)

    // Truncate the file to the correct size
    await fdesc.truncate(fileSize)
    await fdesc.sync()

    progressLogger = new DownloadProgress(fileSize)
    progressLogger.startDisplayTimer()

    core.info(`Downloading ${archivePath}`)
    // Divvy up the download into chunks based on CONCURRENCY
    const chunkSize = Math.ceil(fileSize / CONCURRENCY)
    const chunkRanges: string[] = []
    for (let i = 0; i < CONCURRENCY; i++) {
      const start = i * chunkSize
      const end = i === CONCURRENCY - 1 ? fileSize - 1 : (i + 1) * chunkSize - 1
      chunkRanges.push(`bytes=${start}-${end}`)
    }

    const downloads = chunkRanges.map(async range => {
      core.debug(`Downloading range: ${range}`)
      const response = await retryHttpClientResponse(
        'downloadCache',
        async () =>
          httpClient.get(archiveLocation, {
            Range: range
          })
      )
      const writeStream = fs.createWriteStream(archivePath, {
        fd: fdesc.fd,
        start: parseInt(range.split('=')[1].split('-')[0]),
        autoClose: false
      })
      await pipeResponseToStream(response, writeStream, progressLogger)
      core.debug(`Finished downloading range: ${range}`)
    })

    await Promise.all(downloads)
  } catch (err) {
    core.warning(`Failed to download cache: ${err}`)
    throw err
  } finally {
    // Stop the progress logger regardless of whether the download succeeded or failed.
    // Not doing this will cause the entire action to halt if the download fails.
    progressLogger?.stopDisplayTimer()
    try {
      // NB: We're unsure why we're sometimes seeing a "EBADF: Bad file descriptor" error here.
      //     It seems to be related to the fact that the file descriptor is closed before all
      //     the chunks are written to it. This is a workaround to avoid the error.
      await new Promise(resolve => setTimeout(resolve, 1000))
      await fdesc.close()
    } catch (err) {
      // Intentionally swallow any errors in closing the file descriptor.
      core.warning(`Failed to close file descriptor: ${err}`)
    }
  }
}

/**
 * Download the cache using the Actions toolkit http-client concurrently
 *
 * @param archiveLocation the URL for the cache
 * @param archivePath the local path where the cache is saved
 */
export async function downloadCacheHttpClientConcurrent(
  archiveLocation: string,
  archivePath: fs.PathLike,
  options: DownloadOptions
): Promise<void> {
  const archiveDescriptor = await fs.promises.open(archivePath, 'w+')
  // Set file permissions so that other users can untar the cache
  await archiveDescriptor.chmod(0o644)
  core.debug(`Downloading from ${archiveLocation} to ${archivePath}`)
  const httpClient = new HttpClient('actions/cache', undefined, {
    socketTimeout: options.timeoutInMs,
    keepAlive: true
  })
  try {
    const res = await retryHttpClientResponse(
      'downloadCacheMetadata',
      async () => await httpClient.request('HEAD', archiveLocation, null, {})
    )

    const lengthHeader = res.message.headers['content-length']
    if (lengthHeader === undefined || lengthHeader === null) {
      throw new Error('Content-Length not found on blob response')
    }

    const length = parseInt(lengthHeader)
    if (Number.isNaN(length)) {
      throw new Error(`Could not interpret Content-Length: ${length}`)
    }

    const downloads: {
      offset: number
      promiseGetter: () => Promise<DownloadSegment>
    }[] = []
    const blockSize = 4 * 1024 * 1024

    for (let offset = 0; offset < length; offset += blockSize) {
      const count = Math.min(blockSize, length - offset)
      downloads.push({
        offset,
        promiseGetter: async () => {
          return await downloadSegmentRetry(
            httpClient,
            archiveLocation,
            offset,
            count
          )
        }
      })
    }

    // reverse to use .pop instead of .shift
    downloads.reverse()
    let actives = 0
    let bytesDownloaded = 0
    const progress = new DownloadProgress(length)
    progress.startDisplayTimer()
    const progressFn = progress.onProgress()

    const activeDownloads: {[offset: number]: Promise<DownloadSegment>} = []
    let nextDownload:
      | {offset: number; promiseGetter: () => Promise<DownloadSegment>}
      | undefined

    const waitAndWrite: () => Promise<void> = async () => {
      const segment = await Promise.race(Object.values(activeDownloads))
      await archiveDescriptor.write(
        segment.buffer,
        0,
        segment.count,
        segment.offset
      )
      actives--
      delete activeDownloads[segment.offset]
      bytesDownloaded += segment.count
      progressFn({loadedBytes: bytesDownloaded})
    }

    while ((nextDownload = downloads.pop())) {
      activeDownloads[nextDownload.offset] = nextDownload.promiseGetter()
      actives++

      if (actives >= (options.downloadConcurrency ?? 10)) {
        await waitAndWrite()
      }
    }

    while (actives > 0) {
      await waitAndWrite()
    }
  } finally {
    httpClient.dispose()
    await archiveDescriptor.close()
  }
}

async function downloadSegmentRetry(
  httpClient: HttpClient,
  archiveLocation: string,
  offset: number,
  count: number
): Promise<DownloadSegment> {
  const retries = 5
  let failures = 0

  while (true) {
    try {
      const timeout = 30000
      const result = await promiseWithTimeout(
        timeout,
        downloadSegment(httpClient, archiveLocation, offset, count)
      )
      if (typeof result === 'string') {
        throw new Error('downloadSegmentRetry failed due to timeout')
      }

      return result
    } catch (err) {
      if (failures >= retries) {
        throw err
      }

      failures++
    }
  }
}

async function downloadSegment(
  httpClient: HttpClient,
  archiveLocation: string,
  offset: number,
  count: number
): Promise<DownloadSegment> {
  const partRes = await retryHttpClientResponse(
    'downloadCachePart',
    async () =>
      await httpClient.get(archiveLocation, {
        Range: `bytes=${offset}-${offset + count - 1}`
      })
  )

  if (!partRes.readBodyBuffer) {
    throw new Error('Expected HttpClientResponse to implement readBodyBuffer')
  }

  return {
    offset,
    count,
    buffer: await partRes.readBodyBuffer()
  }
}

declare class DownloadSegment {
  offset: number
  count: number
  buffer: Buffer
}

/**
 * Download the cache using the Azure Storage SDK.  Only call this method if the
 * URL points to an Azure Storage endpoint.
 *
 * @param archiveLocation the URL for the cache
 * @param archivePath the local path where the cache is saved
 * @param options the download options with the defaults set
 */
export async function downloadCacheStorageSDK(
  archiveLocation: string,
  archivePath: string,
  options: DownloadOptions
): Promise<void> {
  const client = new BlockBlobClient(archiveLocation, undefined, {
    retryOptions: {
      // Override the timeout used when downloading each 4 MB chunk
      // The default is 2 min / MB, which is way too slow
      tryTimeoutInMs: options.timeoutInMs
    }
  })

  const properties = await client.getProperties()
  const contentLength = properties.contentLength ?? -1

  if (contentLength < 0) {
    // We should never hit this condition, but just in case fall back to downloading the
    // file as one large stream
    core.debug(
      'Unable to determine content length, downloading file with http-client...'
    )

    await downloadCacheHttpClient(archiveLocation, archivePath)
  } else {
    // Use downloadToBuffer for faster downloads, since internally it splits the
    // file into 4 MB chunks which can then be parallelized and retried independently
    //
    // If the file exceeds the buffer maximum length (~1 GB on 32-bit systems and ~2 GB
    // on 64-bit systems), split the download into multiple segments
    // ~2 GB = 2147483647, beyond this, we start getting out of range error. So, capping it accordingly.

    // Updated segment size to 128MB = 134217728 bytes, to complete a segment faster and fail fast
    const maxSegmentSize = Math.min(134217728, buffer.constants.MAX_LENGTH)
    const downloadProgress = new DownloadProgress(contentLength)

    const fd = fs.openSync(archivePath, 'w')

    try {
      downloadProgress.startDisplayTimer()
      const controller = new AbortController()
      const abortSignal = controller.signal
      while (!downloadProgress.isDone()) {
        const segmentStart =
          downloadProgress.segmentOffset + downloadProgress.segmentSize

        const segmentSize = Math.min(
          maxSegmentSize,
          contentLength - segmentStart
        )

        downloadProgress.nextSegment(segmentSize)
        const result = await promiseWithTimeout(
          options.segmentTimeoutInMs || 3600000,
          client.downloadToBuffer(segmentStart, segmentSize, {
            abortSignal,
            concurrency: options.downloadConcurrency,
            onProgress: downloadProgress.onProgress()
          })
        )
        if (result === 'timeout') {
          controller.abort()
          throw new Error(
            'Aborting cache download as the download time exceeded the timeout.'
          )
        } else if (Buffer.isBuffer(result)) {
          fs.writeFileSync(fd, result)
        }
      }
    } finally {
      downloadProgress.stopDisplayTimer()
      fs.closeSync(fd)
    }
  }
}

const promiseWithTimeout = async <T>(
  timeoutMs: number,
  promise: Promise<T>
): Promise<T | string> => {
  let timeoutHandle: NodeJS.Timeout
  const timeoutPromise = new Promise<string>(resolve => {
    timeoutHandle = setTimeout(() => resolve('timeout'), timeoutMs)
  })

  return Promise.race([promise, timeoutPromise]).then(result => {
    clearTimeout(timeoutHandle)
    return result
  })
}
