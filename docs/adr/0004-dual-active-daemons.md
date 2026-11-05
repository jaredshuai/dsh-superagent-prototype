# 同一工作站的桌面端与 CLI daemon 双活，跨工作站改派等 5 分钟以上

不要求每台工作站只留一个 daemon：两个 daemon 在 Multica 里共用一个身份、都能认领任务，同机双活由 Multica 提供。但实测关掉桌面 App 后 runtime 会显示 offline 约 2–3 分钟才被另一个 daemon 拉回（任务只排队、不失败），所以「runtime offline」不能当作工作站宕机：任务超过 5 分钟没人认领才改派到别的 Multica agent；而 CLI 登录过期这类不可重试的失败直接改派。

## Consequences

- 改派会带来空窗内的等待，换来的是正常关 App 不会触发不必要的改派。
