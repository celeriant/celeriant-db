---
name: vibe-prep
description: How to prepare for a vibe coding session that facilitates human replay.
---

**This skill is activated explicitly.** You are to prepare this code repository for a vibe coding + human replay session. See `human-replay-manifesto.md` under .claude/skills/vibe-coding for the philosophy.

1. Check if the repo is in git source control. If there is no source control, stop.
2. Make sure the git workspace is clean. If there are uncommited files, stop.
3. Clean the build directory (eg. cargo clean in rust)
4. Copy the repo to ~/sandbox/[repo-name]-[feature-name]
5. run 'git remote remove origin'
6. tell the user what to do - close claude, vscode, reopen from the new location, use the /vibe-coding skill, providing a design document.