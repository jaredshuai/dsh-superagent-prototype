/** The Multica REST calls, against a fake fetch. */
import { describe, expect, it } from 'vitest'
import { MulticaApi } from '../src/executor/multica-api.ts'

describe('MulticaApi', () => {
  it('creates an issue even when an active issue has the same title, as two tickets may (live, #5)', async () => {
    const requests: { url: string; body: unknown }[] = []
    const fetcher = (async (url: URL, init: RequestInit) => {
      requests.push({ url: String(url), body: JSON.parse(String(init.body)) })
      return new Response(JSON.stringify({ id: 'i1', identifier: 'JAR-1' }), { status: 201 })
    }) as unknown as typeof fetch
    const api = new MulticaApi({ serverUrl: 'https://multica.test', workspaceId: 'w', token: 'mul_x' }, fetcher)
    await expect(api.createIssue({ title: 'Add a file', description: 'd', agentId: 'a1' })).resolves.toMatchObject({ id: 'i1' })
    expect(requests).toEqual([{ url: 'https://multica.test/api/issues?workspace_id=w',
      body: { title: 'Add a file', description: 'd', assignee_id: 'a1', assignee_type: 'agent', allow_duplicate: true } }])
  })
  it('changes an issue status without starting a run, as `multica issue status --no-start` does', async () => {
    const requests: { method?: string; url: string; body: unknown }[] = []
    const fetcher = (async (url: URL, init: RequestInit) => {
      requests.push({ method: init.method, url: String(url), body: JSON.parse(String(init.body)) })
      return new Response(JSON.stringify({ id: 'i1', status: 'done' }), { status: 200 })
    }) as unknown as typeof fetch
    const api = new MulticaApi({ serverUrl: 'https://multica.test', workspaceId: 'w', token: 'mul_x' }, fetcher)
    await api.setStatus('i1', 'done')
    expect(requests).toEqual([{ method: 'PUT', url: 'https://multica.test/api/issues/i1?workspace_id=w', body: { status: 'done', suppress_run: true } }])
  })
})
