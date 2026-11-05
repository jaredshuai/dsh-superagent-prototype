/** The Multica executor's configuration from the environment. */
import { describe, expect, it } from 'vitest'
import { multicaConfigFromEnv } from '../src/executor/multica.ts'

const base = { SUPERAGENT_EXECUTOR: 'multica', MULTICA_TOKEN: 'mul_x', MULTICA_WORKSPACE_ID: 'w' }

describe('multicaConfigFromEnv', () => {
  it('keeps the single agent of MULTICA_AGENT_ID, which gets every ticket without a health check', () => {
    expect(multicaConfigFromEnv({ ...base, MULTICA_AGENT_ID: ' a1 ' })).toMatchObject({ agents: ['a1'], pickHealthy: false })
  })

  it('reads an ordered candidate list from MULTICA_AGENT_IDS, which picks by health', () => {
    expect(multicaConfigFromEnv({ ...base, MULTICA_AGENT_IDS: 'a2, a1,,a3 ' })).toMatchObject({ agents: ['a2', 'a1', 'a3'], pickHealthy: true })
  })

  it('prefers the candidate list when both are set', () => {
    expect(multicaConfigFromEnv({ ...base, MULTICA_AGENT_ID: 'a1', MULTICA_AGENT_IDS: 'a2,a3' })).toMatchObject({ agents: ['a2', 'a3'] })
  })

  it('names what is missing, with either agent variable', () => {
    expect(() => multicaConfigFromEnv({ SUPERAGENT_EXECUTOR: 'multica', MULTICA_AGENT_IDS: ' , ' }))
      .toThrow('MULTICA_TOKEN, MULTICA_WORKSPACE_ID, MULTICA_AGENT_ID or MULTICA_AGENT_IDS')
  })

  it('keeps the DSH executor unless the Multica one is selected', () => {
    expect(multicaConfigFromEnv({ MULTICA_AGENT_ID: 'a1' })).toBeUndefined()
  })
})
