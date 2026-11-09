## 禁止触碰上游

**禁止向上游 `deepseek-ai/dsh-superagent-prototype` 发 issue、开 PR、评论或推送**，包括为了「回馈修复」「减少同步冲突」。所有改动只留在本 fork（`jaredshuai/dsh-superagent-prototype`）。`upstream` remote 只用于 `git fetch` 同步。见 [ADR 0005](docs/adr/0005-isolate-fork-changes.md)。

## Agent skills

### Issue tracker

Issues live in GitHub Issues of the `jaredshuai/dsh-superagent-prototype` fork (always use `gh -R`, never the upstream). See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
