import * as http from 'http'
import {AddressInfo} from 'net'
import fetch from 'node-fetch'
import {getCacheEntry, reserveCache} from '../src/internal/cacheHttpClient'

jest.mock('node-fetch', () => jest.fn())
jest.mock('../src/internal/downloadUtils', () => ({}))

test.each([undefined, 'enterprise'])(
  'sends instance slug on reserve and restore (%s)',
  async slug => {
    const previous = {...process.env}
    let receivedSlug: string | string[] | undefined
    const server = http.createServer((req, res) => {
      receivedSlug = req.headers['x-blacksmith-instance-slug']
      req.resume()
      res.writeHead(200, {'Content-Type': 'application/json'})
      res.end(JSON.stringify({cacheId: 1}))
    })
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
    try {
      if (slug) process.env.BLACKSMITH_INSTANCE_SLUG = slug
      else delete process.env.BLACKSMITH_INSTANCE_SLUG
      process.env.BLACKSMITH_CACHE_URL = `http://127.0.0.1:${
        (server.address() as AddressInfo).port
      }`
      process.env.GITHUB_REPO_NAME = 'owner/repo'
      process.env.BLACKSMITH_CACHE_TOKEN = 'test-token'
      await reserveCache('key', ['node_modules'])
      expect(receivedSlug).toBe(slug || 'github')
      ;(fetch as jest.MockedFunction<typeof fetch>).mockResolvedValue({
        status: 204
      } as any)
      await getCacheEntry(['key'], ['node_modules'])
      expect(fetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            'X-Blacksmith-Instance-Slug': slug || 'github'
          })
        })
      )
    } finally {
      process.env = previous
      await new Promise<void>((resolve, reject) =>
        server.close(err => (err ? reject(err) : resolve()))
      )
    }
  }
)
