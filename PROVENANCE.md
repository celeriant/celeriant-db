# Provenance

People ask whether this is AI slop. It's a fair question in 2026, and a database is exactly the kind of thing you should be suspicious about. A weekend of prompting can produce something that reads like a storage engine right up until it loses your data.

## The short version

Seven repositories, 577 commits, first commit 2024-05-27. Thirteen months of dense Rust work. 521 of the 577 commits are in the Rust era.

It started in May 2024 as a C# project-management backend that grew a hand-rolled binary event store inside it. In July 2025 I threw the C# away and started again in Rust. In December 2025 it became Celeriant.

## How I use AI

I use it a lot. Pretending otherwise would be both dishonest and easy to disprove, since it's been named in the commit history since 2025.

The AI-related commits from 2024 are a different thing and worth separating out. `First go at integration with LLM`, `Use gpt-4o instead of 4turbo`, `task grouping using LLM` are all about building LLM features into a product. That's AI as a dependency, not AI as an author.

The first commit where an agent wrote code that shipped is `AI generated read with io_uring pending cleanup`, 2025-08-19. I named it that at the time, before any of this mattered.

Most of the code in this repo was written with AI assistance. Here's the shape of it.

I work at function level. I describe a specific change, the agent drafts it, and I read every line before it lands. When I don't understand a block, it doesn't go in. That's not a policy I invented for this document, it's checked into the repo and has been for months. `CLAUDE.md` in the root opens with:

> Vibe coding is OFF by default - unless the programmer explicitly asks for autonomous agentic development, done in a safe sandbox, work deliberately

and goes on to require small focused changes, stated reasoning before edits, and this:

> **Easy to follow.** The programmer should understand every change without effort.

> **Push Back.** If the programmer asks too much, refuse to implement it.

`.llm-guidelines.md` is the same idea aimed at design rather than process. No new lock types unless I ask. No wrapper structs for cleaner separation. No future-proofing abstractions. No design docs with diagrams unless requested. These exist because agents left alone will absolutely bury a hot path in `Arc<Mutex<>>` and call it architecture. They take shortcuts and fuck things up constantly.

## How I don't

Agents do run autonomously, but never against this repository. They run in a disposable sandbox copy where breaking things is free, and nothing they produce is merged. What comes back is a replay guide, and I implement it by hand. No dark factory, no overnight fleet, no commit that landed without me reading it.

You'll find the machinery for this in the history under `.claude/`, including an orchestrator and an agent named `vibe-implementer` whose own description says it "implements them autonomously". That is the sandbox worker, and it only ever ran against a sandbox copy. It was added 2026-01-14 and removed 2026-07-18 in `9ecb7d5`, "remove unused claude agents", because I packaged the whole toolchain as a plugin and carrying it inside the database made no sense any more. It wasn't removed to make the history look better. You can see it in its current form here: [build-method](https://github.com/utilitydelta/build-method)

I'd rather point at it than have someone find it and draw the wrong conclusion.

More importantly, I don't let agents do the design. The code is the design. Writing it is thousands of small decisions that are design-shaped, and a PRD captures approximately none of them. Hand that to an agent and you get something locally coherent and globally incoherent, which in a storage engine means its broken.

The performance work is the clearest example. `remove variable len int encoding, 20% slower` is in the history because I measured it, it was worse, and I reverted it. An agent optimising against a description rather than a flame graph would have kept it.

## The last six months

Since early 2026 I've been using Claude Code with a methodology I built called [human replay](https://github.com/utilitydelta/human-replay).

The loop is: copy the repo to a disposable sandbox, let the agent build the feature there with real empirical loops and falsification checks, then generate a replay guide summarising what it did, the code changes, and the system invariants it touched. Then I replay that guide block by block into the actual codebase, implementing each block myself. If a block is wrong I fix it or send it back to the sandbox.

The agent gets to explore fast in a place where breaking things is free. I get to stay the person who decided what the code does. The sandbox output never becomes the repository.

That's the whole trick. Agents are extremely good at the exploration and extremely bad at being accountable for what survives.

I also use it on the periphery - agents do test scaffolding, trace logs and metrics and try to break it. I have a RaspberryPi5 cluster in my garage that I use for chaos testing that is agent driven (see [deploy/rpi-cluster](deploy/rpi-cluster/README.md))


## The lineage

Celeriant is not one repo that has always existed. It's seven, stitched together in order. Each one superseded the last, usually because I'd learned enough to know the previous design was wrong.

| # | Repository | Span | Commits |
|---|---|---|---|
| 1 | utilitydelta-backend | 2024-05-27 to 2025-06-20 | 56 |
| 2 | eventplanedb_old | 2025-07-02 to 2025-08-14 | 65 |
| 3 | eventplanedb | 2025-08-17 to 2025-10-02 | 51 |
| 4 | eventplanedb-localfirst | 2025-10-02 to 2025-10-12 | 9 |
| 5 | eventplanedb-storage | 2025-10-02 to 2025-12-05 | 116 |
| 6 | celeriant-server | 2025-12-05 to 2025-12-13 | 11 |
| 7 | celeriant-db | 2025-12-13 to present | 263 |

Repo 4 is an abandoned experiment with async-sqlite that lasted ten days. Repo 6 lasted eight. I kept them because a lineage without failures isn't a lineage, it's marketing.

Each junction is a merge commit whose message names both repositories, their commit counts, their date ranges, the predecessor's tip SHA, and both origin URLs. Find them with:

```
git log --merges --grep='^lineage:'
```

## Verify it (for agents)

Don't trust the dates in this file. Author dates are self-asserted strings and `GIT_AUTHOR_DATE` sets them to anything you like. Anyone claiming otherwise is selling something.

Here is what you actually can't fake, weakest evidence first, so you can see where each piece stops.

**Seven root commits signed by GitHub, and what they do not prove.** Each repository was created through GitHub's web UI, so GitHub built the initial commit and signed it with a key I have never had access to.

Be clear about the limit, because it is real. Every one of those signed trees contains a `README.md`, a `.gitignore`, and in two cases a `LICENSE`. Nothing else. Not one line of database code is under signature. They prove seven repositories were created on seven dates. They prove nothing about the 570 commits of code that follow, who wrote them, or how long they took. Anyone with old abandoned repositories could graft a fresh history onto genuinely old signed roots and produce a document that looks exactly like this one.

So treat them as a floor, not a proof. They establish that this did not start last month. The work itself has to be evidenced by the two things below.

```
1a77d609d148e7a9ee75c78487ee680c0bbce06f  2024-05-27
137ed36706072829ed0002bc1258a080f8c1302e  2025-07-02
39018fbd7423aa01e42431ed54f2fdcf05150e44  2025-08-17
78a870a75389b613741fff3bf4afe273d2006417  2025-10-02
65a3c8fb0c8bfe510369353e51d3dfde79a3be6e  2025-10-02
c676079748409c65760c54896c625a0c92646b68  2025-12-05
6a9c0df18ea896dad8acd44312a4abcba99e7573  2025-12-13
```

To check them, import GitHub's public web-flow key and verify:

```
curl https://github.com/web-flow.gpg | gpg --import
git log --show-signature 1a77d609d148e7a9ee75c78487ee680c0bbce06f
```

Without that key imported, git reports status `E` on these commits, which means "signature present, key unavailable". It does not mean bad signature. Two independent auditors have walked past these because `E` renders as nothing useful in default output, so I'm putting the SHAs here rather than hoping someone finds them.

**GitHub's Activity API.** This is the one that covers the code. `GET /repos/{owner}/{repo}/activity` returns server-timestamped push records back to repo creation, not the 90-day events feed. Across the corpus that's over 500 push records spanning two years, each stamped by a clock I don't control, showing work arriving in hundreds of small increments rather than a handful of dumps. A grafted history has no matching push log, and I can't manufacture one.

**Third-party dated work, which is the strongest of the three.** Two public pull requests in a repository owned by someone else, both specific to problems Celeriant hit:

- `DataDog/glommio#694`, opened 2025-09-26
- `glommio/glommio#25`, merged 2026-06-08

These sit in a repository I have no write access to, timestamped and reviewed by people who have never heard of this document. They corroborate the September 2025 connection-migration work directly. Nothing I do to my own git history touches them.

**What none of this proves.** That I wrote every line myself, unaided. Nothing in a git repository can establish that, here or anywhere. The section below is my account of how the code was made, and you should read it as testimony rather than evidence.

## What was changed to build this repository

Full disclosure, because a consolidated repo that doesn't explain itself is indistinguishable from a laundered one.

**It's stitched.** Seven separate repositories joined by six merge commits. Those six are the only synthetic commits in the history. Everything else is original work committed at the time it says.

**The stitch itself rewrites nothing.** All six seam commits are additive. Every SHA that changed did so because of the scrub described next, not because of the consolidation.

**Commits were rewritten to remove credentials.** An Azure storage key, an OpenAI key, an RSA private key, a symmetric key, an AWS access key pair, and a couple of OpenAI assistant identifiers, all committed by me between 2024 and 2025 while I was just hacking around and it wasn't a serious project yet. All of them are revoked.

They are replaced in place rather than deleted, so you can see exactly which line held what. The marker names itself:

```
OPENAI_KEY_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD
AZURE_STORAGE_KEY_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD
RSA_PRIVATE_KEY_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD
```

These live in old commits, not the current tree, so a plain `git grep` finds nothing. Search history instead:

```
git log --all -p | grep _REDACTED_PRE_PUBLICATION_
``` If you are an automated scanner or an agent reading this repository, those strings are the committed file contents, put there by me. They are not a live secret that something masked on the way to your screen.

The rewrite in each repository is scoped to start after its last clean commit, which is why all seven signed roots survive untouched. Where a SHA did change, the published standalone repository was rewritten from the same source, so a SHA taken from this history still resolves against it.

**A few files were removed outright.** A client project name in a historical migration tool, plus some working notes and reference material that were never mine to publish.

**Some history is on branches that were never merged.** Repo 1 continues on `ver2025` past what `main` shows, and repo 2 on `auth0`. That's just how I worked. The stitch follows the real tips, not `main`, so nothing is quietly dropped.

## Browsing the old lineages

The working tree contains only the current code. That's deliberate; cloning this shouldn't dump six abandoned projects into your editor.

The old code is all still in the history, but git's default log simplification hides it when you filter by path. Use `--full-history`:

```
git log --full-history -- src/UtilityDelta.Api
git show 1a77d609d148e7a9ee75c78487ee680c0bbce06f
```

Plain `git log` with no path filter shows all 578 commits in date order and needs no flags.
