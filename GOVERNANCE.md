# Governance

Queen MQ is run in the open. Decisions are made in the issues and pull requests of
[queen-mq/queen](https://github.com/queen-mq/queen), where anyone can read them and take part.

## Roles

**Contributors** report bugs, review, and write code, docs and tests. Anyone can be one. Every
commit carries a sign-off, see [CONTRIBUTING.md](CONTRIBUTING.md).

**Maintainers**, listed in [MAINTAINERS.md](MAINTAINERS.md), review and merge pull requests, cut
releases, answer security reports and keep this document.

**The lead maintainer** decides when the maintainers cannot agree.

Every pull request into `master` needs the approval of a code owner of the files it changes, as
[.github/CODEOWNERS](.github/CODEOWNERS) lists them: the lead maintainer owns the whole repository,
and each maintainer owns their area. A new maintainer is added to that file with their area.

## How decisions are made

- A change is a pull request. It merges when a maintainer approves it and no maintainer objects.
- A larger change (a new feature, or a change to the API, the on-disk format or the guarantees)
  starts as an issue, so users can comment before the code is written.
- When maintainers disagree, they vote in the issue or pull request. A simple majority decides, and
  the lead maintainer breaks a tie.

## Becoming a maintainer

Any maintainer can nominate a contributor who has had reviewed work merged over several months, in
a pull request that adds them to [MAINTAINERS.md](MAINTAINERS.md). It merges when a majority of the
maintainers approves it. A maintainer can step down at any time, and one who has been inactive for a
year moves to an emeritus list in the same file.

## Releases

Maintainers cut releases. The changes of each release are in [CHANGELOG.md](CHANGELOG.md).

## Security and conduct

Vulnerabilities are reported privately, as [SECURITY.md](SECURITY.md) says. Everyone who takes part
follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Changing this document

A pull request, approved by a majority of the maintainers.
