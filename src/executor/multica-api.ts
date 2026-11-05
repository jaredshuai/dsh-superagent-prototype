/**
 * REST client for the Multica server, limited to what MulticaExecutor uses: issues with metadata, their runs and comments,
 * and cancelling a run. Every request carries the workspace as a query parameter, which the server requires.
 */
import { z } from 'zod'

/** Where and as whom the executor reaches Multica. */
export interface MulticaConnection {
  /** Such as `https://api.multica.ai`. */
  serverUrl: string
  workspaceId: string
  /** A personal access token (`mul_…`). */
  token: string
}

const RunSchema = z.object({
  id: z.string(), issue_id: z.string().nullish(), agent_id: z.string().nullish(), status: z.string(),
  attempt: z.number().nullish(), max_attempts: z.number().nullish(),
  error: z.string().nullish(), can_supplement: z.boolean().nullish(), trigger_comment_id: z.string().nullish(),
  delivered_comment_ids: z.array(z.string()).nullish(), started_at: z.string().nullish(), completed_at: z.string().nullish(),
  result: z.object({ output: z.string().nullish() }).passthrough().nullish(),
}).passthrough()
export type Run = z.infer<typeof RunSchema>
const CommentSchema = z.object({
  id: z.string(), issue_id: z.string(), author_type: z.string(), author_id: z.string().nullish(), content: z.string(),
  parent_id: z.string().nullish(), created_at: z.string(), source_task_id: z.string().nullish(),
}).passthrough()
export type Comment = z.infer<typeof CommentSchema>
const IssueSchema = z.object({ id: z.string(), identifier: z.string().nullish(), status: z.string().nullish(), assignee_id: z.string().nullish(),
  metadata: z.record(z.string(), z.unknown()).nullish() }).passthrough()
export type Issue = z.infer<typeof IssueSchema>
const PostedCommentSchema = CommentSchema.extend({
  trigger_outcomes: z.array(z.object({ status: z.string().optional(), reason_code: z.string().optional(), target_id: z.string().optional() })
    .passthrough()).nullish() })
export type PostedComment = z.infer<typeof PostedCommentSchema>

const AgentSchema = z.object({ id: z.string(), status: z.string(), runtime_id: z.string().nullish(), archived_at: z.string().nullish() })
  .passthrough()
export type Agent = z.infer<typeof AgentSchema>
const RuntimeSchema = z.object({ id: z.string(), status: z.string() }).passthrough()
export type Runtime = z.infer<typeof RuntimeSchema>

/** A failed Multica request, with its HTTP status (0 when the request did not get a response). */
export class MulticaError extends Error {
  constructor(message: string, readonly status: number) { super(message) }
}

/** Typed calls to the Multica REST API; `fetch` is injectable for tests. */
export class MulticaApi {
  constructor(private readonly connection: MulticaConnection, private readonly fetcher: typeof fetch = fetch) {}
  private async request<T>(method: string, path: string, schema: z.ZodType<T>, body?: unknown, signal?: AbortSignal): Promise<T> {
    const url = new URL(path, this.connection.serverUrl)
    url.searchParams.set('workspace_id', this.connection.workspaceId)
    let response: Response
    try {
      response = await this.fetcher(url, { method, signal: signal ?? AbortSignal.timeout(30_000),
        headers: { authorization: `Bearer ${this.connection.token}`, ...body === undefined ? {} : { 'content-type': 'application/json' } },
        ...body === undefined ? {} : { body: JSON.stringify(body) } })
    } catch (error) { throw new MulticaError(`Multica ${method} ${url.pathname} failed: ${error instanceof Error ? error.message : String(error)}`, 0) }
    const text = await response.text()
    if (!response.ok) throw new MulticaError(`Multica ${method} ${url.pathname} returned ${response.status}: ${text.slice(0, 300)}`, response.status)
    return schema.parse(text ? JSON.parse(text) : null)
  }
  /** Creates an issue assigned to `agentId`, which starts its first run. */
  createIssue(input: { title: string; description: string; agentId: string }, signal?: AbortSignal): Promise<Issue> {
    return this.request('POST', '/api/issues', IssueSchema,
      { title: input.title, description: input.description, assignee_id: input.agentId, assignee_type: 'agent' }, signal)
  }
  async setMetadata(issueId: string, key: string, value: string, signal?: AbortSignal): Promise<void> {
    await this.request('PUT', `/api/issues/${encodeURIComponent(issueId)}/metadata/${encodeURIComponent(key)}`, z.unknown(), { value }, signal)
  }
  getIssue(issueId: string, signal?: AbortSignal): Promise<Issue> {
    return this.request('GET', `/api/issues/${encodeURIComponent(issueId)}`, IssueSchema, undefined, signal)
  }
  /** Issues whose metadata holds every pair of `metadata`. */
  async findIssues(metadata: Record<string, string>, signal?: AbortSignal): Promise<Issue[]> {
    const result = await this.request('GET', `/api/issues?metadata=${encodeURIComponent(JSON.stringify(metadata))}`,
      z.object({ issues: z.array(IssueSchema) }).passthrough(), undefined, signal)
    return result.issues
  }
  /** Every run of an issue, newest first. */
  runs(issueId: string, signal?: AbortSignal): Promise<Run[]> {
    return this.request('GET', `/api/issues/${encodeURIComponent(issueId)}/task-runs`, z.array(RunSchema), undefined, signal)
  }
  comments(issueId: string, signal?: AbortSignal): Promise<Comment[]> {
    return this.request('GET', `/api/issues/${encodeURIComponent(issueId)}/comments`, z.array(CommentSchema), undefined, signal)
  }
  /** Posts a comment; with `steer`, into those running runs instead of a follow-up run where they accept it. */
  addComment(issueId: string, content: string, options: { parentId?: string; steer?: string[] } = {}, signal?: AbortSignal): Promise<PostedComment> {
    return this.request('POST', `/api/issues/${encodeURIComponent(issueId)}/comments`, PostedCommentSchema,
      { content, ...options.parentId ? { parent_id: options.parentId } : {}, ...options.steer?.length ? { steer_task_ids: options.steer } : {} }, signal)
  }
  async cancelRun(runId: string, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/api/tasks/${encodeURIComponent(runId)}/cancel`, z.unknown(), {}, signal)
  }
  /** The workspace's agents, with their status and the runtime each is bound to. */
  agents(signal?: AbortSignal): Promise<Agent[]> { return this.request('GET', '/api/agents', z.array(AgentSchema), undefined, signal) }
  /** The workspace's runtimes (one per workstation and CLI), with whether each is online. */
  runtimes(signal?: AbortSignal): Promise<Runtime[]> { return this.request('GET', '/api/runtimes', z.array(RuntimeSchema), undefined, signal) }
}
