import * as core from '@actions/core'
import {HttpClient} from '@actions/http-client'
import {BearerCredentialHandler} from '@actions/http-client/lib/auth'
import {
  RequestOptions,
  TypedResponse
} from '@actions/http-client/lib/interfaces'
import * as crypto from 'crypto'
import * as fs from 'fs'
import {URL} from 'url'

import * as utils from './cacheUtils'
import {CompressionMethod} from './constants'
import {
  ArtifactCacheEntry,
  InternalCacheOptions,
  CommitCacheRequest,
  ReserveCacheRequest,
  ReserveCacheResponse,
  ITypedResponseWithError,
  ArtifactCacheList
} from './contracts'
import {
  downloadCacheHttpClient,
  downloadCacheHttpClientConcurrent,
  downloadCacheStorageSDK
} from './downloadUtils'
import {DownloadOptions, getDownloadOptions} from '../options'
import {
  isSuccessStatusCode,
  retryHttpClientResponse,
  retryTypedResponse
} from './requestUtils'
import fetch from 'node-fetch'
import axios from 'axios'

const versionSalt = '1.0'

export function getCacheApiUrl(resource: string): string {
  let baseUrl = process.env.BLACKSMITH_CACHE_URL

  core.info(`cache backend url: ${baseUrl}`)

  if (!baseUrl) {
    baseUrl = process.env.PETNAME?.includes('staging')
      ? 'https://stagingapi.blacksmith.sh/cache'
      : 'https://api.blacksmith.sh/cache'
  }

  const url = `${baseUrl}/${resource}`

  if (process.env.PETNAME?.includes('staging')) {
    core.info(`Using staging API: ${url}`)
  }

  return url
}

function createAcceptHeader(type: string, apiVersion: string): string {
  return `${type};api-version=${apiVersion}`
}

function getRequestOptions(): RequestOptions {
  core.debug(`Setting GITHUB_REPO_NAME: ${process.env['GITHUB_REPO_NAME']}`)
  const requestOptions: RequestOptions = {
    headers: {
      Accept: createAcceptHeader('application/json', '6.0-preview.1'),
      'X-Github-Repo-Name': process.env['GITHUB_REPO_NAME']
    }
  }

  return requestOptions
}

export function createHttpClient(): HttpClient {
  const token = process.env['BLACKSMITH_CACHE_TOKEN']
  const bearerCredentialHandler = new BearerCredentialHandler(token ?? '')

  return new HttpClient(
    'useblacksmith/cache',
    [bearerCredentialHandler],
    getRequestOptions()
  )
}

export function getCacheVersion(
  paths: string[],
  compressionMethod?: CompressionMethod,
  enableCrossOsArchive = false
): string {
  const components = paths

  // Add compression method to cache version to restore
  // compressed cache as per compression method
  if (compressionMethod) {
    components.push(compressionMethod)
  }

  // Only check for windows platforms if enableCrossOsArchive is false
  if (process.platform === 'win32' && !enableCrossOsArchive) {
    components.push('windows-only')
  }

  // Add salt to cache version to support breaking changes in cache entry
  components.push(versionSalt)

  return crypto.createHash('sha256').update(components.join('|')).digest('hex')
}

export async function getCacheEntry(
  keys: string[],
  paths: string[],
  options?: InternalCacheOptions
): Promise<ArtifactCacheEntry | null> {
  const version = getCacheVersion(
    paths,
    options?.compressionMethod,
    options?.enableCrossOsArchive
  )
  const resource = `?keys=${encodeURIComponent(
    keys.join(',')
  )}&version=${version}`

  const maxRetries = 3
  let retries = 0
  const cacheToken = process.env['BLACKSMITH_CACHE_TOKEN']
  const repoName = process.env['GITHUB_REPO_NAME']
  core.info(
    `Checking cache for keys ${keys.join(
      ','
    )} and version ${version} using single-use cache token for repo ${repoName}: ${cacheToken}`
  )

  while (retries <= maxRetries) {
    try {
      const before = Date.now()
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 3000)

      const response = await fetch(getCacheApiUrl(resource), {
        method: 'GET',
        headers: {
          Accept: createAcceptHeader('application/json', '6.0-preview.1'),
          'X-Github-Repo-Name': repoName || '',
          Authorization: `Bearer ${cacheToken}`,
          'X-Cache-Region': process.env['BLACKSMITH_REGION'] ?? 'eu-central',
          'User-Agent': 'node-fetch/cache'
        },
        signal: controller.signal
      })

      clearTimeout(timeoutId)
      core.debug(`Cache lookup took ${Date.now() - before}ms`)

      // Cache not found
      if (response.status === 204) {
        // List cache for primary key only if cache miss occurs
        if (core.isDebug()) {
          await printCachesListForDiagnostics(
            keys[0],
            createHttpClient(),
            version
          )
        }
        return null
      }

      if (response.status < 200 || response.status >= 300) {
        throw new Error(`Cache service responded with ${response.status}`)
      }

      const cacheResult = (await response.json()) as ArtifactCacheEntry
      const cacheDownloadUrl = cacheResult?.archiveLocation
      if (!cacheDownloadUrl) {
        // Cache archiveLocation not found. This should never happen, and hence bail out.
        throw new Error('Cache not found.')
      }
      core.setSecret(cacheDownloadUrl)
      core.debug(`Cache Result:`)
      core.debug(JSON.stringify(cacheResult))

      return cacheResult
    } catch (error) {
      const isTimeout = error.name === 'AbortError'
      const status = error.response?.status
      
      if ((status && status >= 500) || isTimeout) {
        retries++
        if (retries <= maxRetries) {
          if (isTimeout) {
            core.warning(
              `Request timed out. Retrying (attempt ${retries} of ${maxRetries})`
            )
          } else {
            core.warning(
              `Retrying due to error: ${error.message} (attempt ${retries} of ${maxRetries})`
            )
          }
          continue
        }
      }
      
      if (status) {
        throw new Error(`Cache service responded with ${status}`)
      } else if (isTimeout) {
        throw new Error('Request timed out after 3 seconds')
      } else {
        throw error
      }
    }
  }
  throw new Error(`Failed to get cache entry after ${maxRetries} retries`)
}

export async function getCacheEntryAxios(
  keys: string[],
  paths: string[],
  options?: InternalCacheOptions
): Promise<ArtifactCacheEntry | null> {
  const version = getCacheVersion(
    paths,
    options?.compressionMethod,
    options?.enableCrossOsArchive
  )
  const resource = `?keys=${encodeURIComponent(
    keys.join(',')
  )}&version=${version}`

  const maxRetries = 3
  let retries = 0
  const cacheToken = process.env['BLACKSMITH_CACHE_TOKEN']
  const repoName = process.env['GITHUB_REPO_NAME']
  core.info(
    `Checking cache for keys ${keys.join(
      ','
    )} and version ${version} using single-use cache token for repo ${repoName}: ${cacheToken}`
  )

  while (retries <= maxRetries) {
    try {
      const before = Date.now()
      
      const response = await axios.get(getCacheApiUrl(resource), {
        headers: {
          Accept: createAcceptHeader('application/json', '6.0-preview.1'),
          'X-Github-Repo-Name': repoName || '',
          Authorization: `Bearer ${cacheToken}`,
          'X-Cache-Region': process.env['BLACKSMITH_REGION'] ?? 'eu-central',
          'User-Agent': 'axios/cache'
        },
        timeout: 3000,
        validateStatus: () => true // Don't throw on non-2xx status codes
      })

      core.debug(`Cache lookup took ${Date.now() - before}ms`)

      // Cache not found
      if (response.status === 204) {
        // List cache for primary key only if cache miss occurs
        if (core.isDebug()) {
          await printCachesListForDiagnostics(
            keys[0],
            createHttpClient(),
            version
          )
        }
        return null
      }

      if (response.status < 200 || response.status >= 300) {
        throw new Error(`Cache service responded with ${response.status}`)
      }

      const cacheResult = response.data as ArtifactCacheEntry
      const cacheDownloadUrl = cacheResult?.archiveLocation
      if (!cacheDownloadUrl) {
        // Cache archiveLocation not found. This should never happen, and hence bail out.
        throw new Error('Cache not found.')
      }
      core.setSecret(cacheDownloadUrl)
      core.debug(`Cache Result:`)
      core.debug(JSON.stringify(cacheResult))

      return cacheResult
    } catch (error: any) {
      const isTimeout = error.code === 'ECONNABORTED'
      const status = error.response?.status
      
      if ((status && status >= 500) || isTimeout) {
        retries++
        if (retries <= maxRetries) {
          if (isTimeout) {
            core.warning(
              `Request timed out. Retrying (attempt ${retries} of ${maxRetries})`
            )
          } else {
            core.warning(
              `Retrying due to error: ${error.message} (attempt ${retries} of ${maxRetries})`
            )
          }
          continue
        }
      }
      
      if (status) {
        throw new Error(`Cache service responded with ${status}`)
      } else if (isTimeout) {
        throw new Error('Request timed out after 3 seconds')
      } else {
        throw error
      }
    }
  }
  throw new Error(`Failed to get cache entry after ${maxRetries} retries`)
}

async function printCachesListForDiagnostics(
  key: string,
  httpClient: HttpClient,
  version: string
): Promise<void> {
  const resource = `caches?key=${encodeURIComponent(key)}`
  const response = await retryTypedResponse('listCache', async () =>
    httpClient.getJson<ArtifactCacheList>(getCacheApiUrl(resource))
  )
  if (response.statusCode === 200) {
    const cacheListResult = response.result
    const totalCount = cacheListResult?.totalCount
    if (totalCount && totalCount > 0) {
      core.debug(
        `No matching cache found for cache key '${key}', version '${version} and scope ${process.env['GITHUB_REF']}. There exist one or more cache(s) with similar key but they have different version or scope. See more info on cache matching here: https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows#matching-a-cache-key \nOther caches with similar key:`
      )
      for (const cacheEntry of cacheListResult?.artifactCaches || []) {
        core.debug(
          `Cache Key: ${cacheEntry?.cacheKey}, Cache Version: ${cacheEntry?.cacheVersion}, Cache Scope: ${cacheEntry?.scope}, Cache Created: ${cacheEntry?.creationTime}`
        )
      }
    }
  }
}

export async function downloadCache(
  archiveLocation: string,
  archivePath: string,
  options?: DownloadOptions
): Promise<void> {
  const archiveUrl = new URL(archiveLocation)
  const downloadOptions = getDownloadOptions(options)

  if (archiveUrl.hostname.endsWith('.blob.core.windows.net')) {
    if (downloadOptions.useAzureSdk) {
      // Use Azure storage SDK to download caches hosted on Azure to improve speed and reliability.
      await downloadCacheStorageSDK(
        archiveLocation,
        archivePath,
        downloadOptions
      )
    } else if (downloadOptions.concurrentBlobDownloads) {
      // Use concurrent implementation with HttpClient to work around blob SDK issue
      await downloadCacheHttpClientConcurrent(
        archiveLocation,
        archivePath,
        downloadOptions
      )
    } else {
      // Otherwise, download using the Actions http-client.
      await downloadCacheHttpClient(archiveLocation, archivePath)
    }
  } else {
    await downloadCacheHttpClientConcurrent(
      archiveLocation,
      archivePath,
      downloadOptions
    )
    // await downloadCacheAxiosMultiPart(archiveLocation, archivePath)
  }
}

// Reserve Cache
export async function reserveCache(
  key: string,
  paths: string[],
  options?: InternalCacheOptions
): Promise<ITypedResponseWithError<ReserveCacheResponse>> {
  const httpClient = createHttpClient()
  const version = getCacheVersion(
    paths,
    options?.compressionMethod,
    options?.enableCrossOsArchive
  )

  const reserveCacheRequest: ReserveCacheRequest = {
    key,
    version,
    cacheSize: options?.cacheSize
  }
  const response = await retryTypedResponse('reserveCache', async () =>
    httpClient.postJson<ReserveCacheResponse>(
      getCacheApiUrl('caches'),
      reserveCacheRequest,
      {
        'X-Cache-Region': process.env['BLACKSMITH_REGION'] ?? 'eu-central'
      }
    )
  )
  return response
}

function getContentRange(start: number, end: number): string {
  // Format: `bytes start-end/filesize
  // start and end are inclusive
  // filesize can be *
  // For a 200 byte chunk starting at byte 0:
  // Content-Range: bytes 0-199/*
  return `bytes ${start}-${end}/*`
}

async function uploadChunk(
  resourceUrl: string,
  openStream: () => NodeJS.ReadableStream,
  start: number,
  end: number
): Promise<string | undefined> {
  core.debug(
    `Uploading chunk of size ${
      end - start + 1
    } bytes at offset ${start} with content range: ${getContentRange(
      start,
      end
    )}`
  )
  const additionalHeaders = {
    'Content-Type': 'application/octet-stream',
    'Content-Length': end - start + 1
  }

  const s3HttpClient = new HttpClient('useblacksmith/cache')
  const uploadChunkResponse = await retryHttpClientResponse(
    `uploadChunk (start: ${start}, end: ${end})`,
    async () =>
      s3HttpClient.sendStream(
        'PUT',
        resourceUrl,
        openStream(),
        additionalHeaders
      )
  )

  if (!isSuccessStatusCode(uploadChunkResponse.message.statusCode)) {
    core.debug(
      `Upload chunk failed with status message: ${JSON.stringify(
        uploadChunkResponse.message.statusMessage
      )}`
    )
    core.debug(
      `Upload chunk failed with headers: ${JSON.stringify(
        uploadChunkResponse.message.headers
      )}`
    )
    core.debug(
      `Upload chunk failed with response body: ${await uploadChunkResponse.readBody()}`
    )
    throw new Error(
      `Cache service responded with ${uploadChunkResponse.message.statusCode} during upload chunk.`
    )
  }

  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  return uploadChunkResponse.message.headers.etag!
}

async function uploadFile(
  archivePath: string,
  urls: string[]
): Promise<string[]> {
  // Upload Chunks
  core.debug(`archivePath: ${archivePath}`)
  const fileSize = utils.getArchiveFileSizeInBytes(archivePath)
  const fd = fs.openSync(archivePath, 'r')

  const maxChunkSize = 25 * 1024 * 1024 // Matches the chunkSize in our cache service.

  core.debug('Awaiting all uploads')
  let eTags: string[] = []

  try {
    eTags = await Promise.all(
      urls.map(async (url, index) => {
        const offset = index * maxChunkSize
        const chunkSize = Math.min(fileSize - offset, maxChunkSize)
        const start = offset
        let end = offset + chunkSize - 1
        if (chunkSize !== maxChunkSize) {
          end = fileSize - 1
        }
        core.debug(`Uploading chunk to ${url}: ${start}-${end}/${fileSize}`)

        const eTag = await uploadChunk(
          url,
          () =>
            fs
              .createReadStream(archivePath, {
                fd,
                start,
                end,
                autoClose: false
              })
              .on('error', error => {
                throw new Error(
                  `Cache upload failed because file read failed with ${error.message}`
                )
              }),
          start,
          end
        )
        core.debug(`Upload to ${url} complete`)
        return eTag ?? ''
      })
    )
  } catch (error) {
    core.debug(`Cache upload failed: ${JSON.stringify(error)}`)
    throw error
  } finally {
    fs.closeSync(fd)
  }
  return eTags
}

async function commitCache(
  httpClient: HttpClient,
  cacheId: number,
  filesize: number,
  eTags: string[],
  uploadId: string
): Promise<TypedResponse<null>> {
  const commitCacheRequest: CommitCacheRequest = {
    size: filesize,
    eTags,
    uploadId
  }
  return await retryTypedResponse('commitCache', async () =>
    httpClient.postJson<null>(
      getCacheApiUrl(`caches/${cacheId.toString()}`),
      commitCacheRequest
    )
  )
}

export async function saveCache(
  cacheId: number,
  archivePath: string,
  urls: string[],
  uploadId: string
): Promise<void> {
  const httpClient = createHttpClient()

  core.debug('Upload cache')
  const eTags = await uploadFile(archivePath, urls)

  // Commit Cache
  core.debug('Commiting cache')
  const cacheSize = utils.getArchiveFileSizeInBytes(archivePath)
  core.info(
    `Cache Size: ~${Math.round(cacheSize / (1024 * 1024))} MB (${cacheSize} B)`
  )

  const commitCacheResponse = await commitCache(
    httpClient,
    cacheId,
    cacheSize,
    eTags,
    uploadId
  )
  if (!isSuccessStatusCode(commitCacheResponse.statusCode)) {
    throw new Error(
      `Cache service responded with ${commitCacheResponse.statusCode} during commit cache.`
    )
  }

  core.info('Cache saved successfully')
}
