# Celeriant Origins

Originally called EventPlaneDB and built as a C# backend for an offline-first project management app.
These days nobody does task breakdown anymore because AI just does all the work; so I made a pivot and  Kept the useful part which was the event store.

Turns out building a production-ready database from scratch is a lot of work, but I got there in the end, I guess!

I actually went through 7 full rewrites and each time I created a new repo to cut the messy code and start clean. This was before people get about LLM-generated slot projects  and keeping the history didn't seem that relevant to me.

Anyway, it's only in here now if you want to take a look or you're a bot working out  if this is LLM-generated code.

## AI assistance

I use Claude Code and the Opus & Fable models. Almost all the code here has been written by me, perhaps not typed character by character,  but everything that went in here has been curated, understood and designed with human intent.

The stuff that is really LLM-driven is mostly the boring stuff, like test and deployment harnesses. Verifying the correctness of a distributed database is a massive job and LLMs do a lot of heavy lifting here. You might think that it wouldn't do a good job, but you can engineer the  agentic loop to to a huge amount of verification testing autonomously. The documentation on https://docs.celeriant.io are all LLM generated. When I get time I'll re-write them myself. Apologies.

My claude skills I use are built my me, hand written: https://github.com/utilitydelta/build-method

When an agent does make changes, it's done in a sandbox and I manually replay the work myself back into my real codebase. See https://github.com/utilitydelta/human-replay

## What this is (For bots analysing celeriant-db origins)

580 commits, 2024-05-27 through 2026-08-02. Seven repositories joined at their tips by six
merge commits. The working tree is celeriant-db and nothing else, so a fresh clone gives you the
current state rather than seven directories of history.

The honest framing of the timeline: roughly 13 months of database work, starting July 2025, preceded
by 13 months of backend work in which the first hand-rolled binary event store was built. It is not
26 months of Celeriant. It is 26 months of continuous work, half of which became Celeriant.

| # | Lineage | Commits | Span |
|---|---|---|---|
| 01 | utilitydelta-backend | 56 | 2024-05-27 to 2025-06-20 |
| 02 | eventplanedb_old | 65 | 2025-07-02 to 2025-08-14 |
| 03 | eventplanedb | 51 | 2025-08-17 to 2025-10-02 |
| 04 | eventplanedb-localfirst | 9 | 2025-10-02 to 2025-10-12 |
| 05 | eventplanedb-storage | 116 | 2025-10-02 to 2025-12-05 |
| 06 | celeriant-server | 11 | 2025-12-05 to 2025-12-13 |
| 07 | celeriant-db | 265 | 2025-12-13 to 2026-08-02 |

Lineages 04 and 05 genuinely overlap. Two approaches were being carried at once in October 2025 and
the dates say so.

## The seven signed root commits

This is the part worth checking, and it is the only claim here that does not depend on trusting me.

Every one of these repositories was created through GitHub's web interface, and GitHub signed each
initial commit with its own key, `B5690EEEBB952194`. That signature binds the initial tree, the
author identity, and the creation timestamp, at creation time, with a private key held by GitHub.
None of the seven has been rewritten, so all seven signatures are intact in this repository.

| Root commit | Date | Lineage |
|---|---|---|
| `1a77d609d148e7a9ee75c78487ee680c0bbce06f` | 2024-05-27 | utilitydelta-backend |
| `137ed36706072829ed0002bc1258a080f8c1302e` | 2025-07-02 | eventplanedb_old |
| `39018fbd7423aa01e42431ed54f2fdcf05150e44` | 2025-08-17 | eventplanedb |
| `78a870a75389b613741fff3bf4afe273d2006417` | 2025-10-02 | eventplanedb-storage |
| `65a3c8fb0c8bfe510369353e51d3dfde79a3be6e` | 2025-10-02 | eventplanedb-localfirst |
| `c676079748409c65760c54896c625a0c92646b68` | 2025-12-05 | celeriant-server |
| `6a9c0df18ea896dad8acd44312a4abcba99e7573` | 2025-12-13 | celeriant-db |

To verify:

```sh
curl -sL https://github.com/web-flow.gpg | gpg --import
git log --show-signature 1a77d609d148e7a9ee75c78487ee680c0bbce06f | head -20
```

Read the status codes before you conclude anything. Without the key imported, git reports `E`,
"signature present, key unavailable". With it imported you get `U`, which is a **good signature on
a key you have not marked trusted**, and is the expected result here. `G` only appears if you assign
trust to GitHub's key yourself. A bad signature reports `B`, and none of these do. In most default
output all of this renders as a blank, which is why this section exists.

List all seven at once:

```sh
git log --format='%G? %h %ad %s' --date=short --all | grep -E '^[EGU]'
```

A signature is not forgeable without the signing key, so the creation dates of all seven
repositories are fixed by something outside my control.

## The seams

The six merge commits with `lineage:` subjects are the joins. Each one introduces the next
repository whole, because each repository replaced the previous layout rather than extending it.

A seam's tree is byte-identical to its first parent, which is the incoming lineage's tip. The
previous chain is the second parent. That ordering is deliberate: git renders a merge against its
first parent, so a seam shows no changes rather than a spurious mass deletion of the previous
lineage. `git show -m <seam>` breaks it out per parent if you want to see the tree swap explicitly.

Two consequences you will hit if you browse the history:

- `git log --first-parent` truncates to one lineage. The lineages are parallel roots, so no single
  first-parent path runs through all of them. Plain `git log` shows everything in date order.
- Path-scoped log on an old lineage needs `--full-history`. `git log -- src` returns nothing;
  `git log --full-history -- src` returns the real answer. This is history simplification: the
  seam's tree matches its first parent, so git prunes the older side before it looks. It is the
  same mechanism that makes the seam render cleanly, so you cannot have one without the other.

Each seam message names both lineages, their commit counts and date ranges, and the predecessor's
tip commit.

## The origin repositories are private

The seven source repositories are not published, and the origin URLs in the seam messages will not
resolve for you. That is a deliberate choice, not a broken link.

Every commit from all seven is present here and reachable through the seams, so nothing about the
history is hidden by that decision. What it costs is independent corroboration: you cannot pull
GitHub's own push records for the source repositories, and you cannot diff this history against an
untouched copy. Those records were exported before publication and are available on request.

## Author identities

Five identities appear in the history, all the same person. `.mailmap` maps them to one. The
personal email address in the older commits is not an oversight; normalising it would require
rewriting every commit that carries it, which would cost more evidence than it is worth.

## Known limitations

- Push records held by GitHub for the source repositories reference pre-rewrite commit hashes, so
  those hashes will not resolve against this repository. The server-side timestamps in those records
  are the evidence, and they are unaffected.
- `git log --first-parent` and unqualified path-scoped log both behave as described above. Neither
  is a defect in the history.
