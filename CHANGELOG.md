# Changelog

## 0.2.0 - Unreleased

### Added

- Start tracking changes for the next release (from 2026-01-23).
- Send: `wacli send file --filename` to override display name for uploads. (#7 — thanks @plattenschieber)
- Auth: allow optional `WACLI_DEVICE_LABEL` and `WACLI_DEVICE_PLATFORM` overrides for linked device identity. (#4 — thanks @zats)

### Fixed

- Build: preserve existing `CGO_CFLAGS` when adding GCC 15+ workaround. (#8 — thanks @ramarivera)

## 0.1.0 - 2026-01-01

### Added

- Auth: `wacli auth` QR login, bootstrap sync, optional follow, idle-exit, background media download, contacts/groups refresh.
- Sync: non-interactive `wacli sync` once/follow, never shows QR, idle-exit, background media download, optional contacts/groups refresh.
- Messages: list/search/show/context with chat/sender/time/media filters; FTS5 search with LIKE fallback and snippets.
- Send: text and file (image/video/audio/document) with caption and MIME override.
- Media: download by chat/id, resolves output paths, and records downloaded media in the DB.
- History: on-demand backfill per chat with request count, wait, and idle-exit.
- Contacts: search/show; import from WhatsApp store; local alias and tag management.
- Chats: list/show with kind and last message timestamp.
- Groups: list/refresh/info/rename; participants add/remove/promote/demote; invite link get/revoke; join/leave.
- Diagnostics: `wacli doctor` for store path, lock status/info, auth/connection check, and FTS status.
- CLI UX: human-readable output by default with `--json`, global `--store`/`--timeout`, plus `wacli version`.
- Storage: default `~/.wacli`, lock file for single-instance safety, SQLite DB with FTS5, WhatsApp session store, and media directory.
