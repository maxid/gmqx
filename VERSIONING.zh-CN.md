[English](VERSIONING.md) | 简体中文

# 版本策略

开发阶段策略：

- Feature/docs/refactor 提交**不**升版本、不打 tag。
- 跨仓库开发使用 Go pseudo-version 或 `replace`。
- **开发期不打 tag。** 消费方使用 commit pseudo-version。
- API/行为稳定后，第一个 tag 是稳定版 `v1.0.0`。
- `v1.0.0` 之后承诺 API 稳定；0.x 可能包含破坏性变更。
- 已有 tag 作为历史保留。
