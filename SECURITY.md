# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | ✅        |

## Reporting a vulnerability

**Do not open a public issue for security vulnerabilities.**

Email the details to the maintainer via a [private security advisory](https://github.com/Sanjays2402/ferry/security/advisories/new). Include:

- a description of the vulnerability and its impact
- steps to reproduce (a minimal script is ideal)
- the Ferry version and broker in use

You can expect an initial response within 7 days. If the report is confirmed, a fix will be prioritized and you'll be credited in the release notes (unless you'd rather stay anonymous).

## Scope notes

Ferry is a library and a set of CLI processes that you run yourself. The threat model that matters:

- **Task payloads are code-adjacent.** Only enqueue tasks from trusted producers — a malicious payload executes on your workers with their privileges, by design.
- **The dashboard has no auth.** Don't expose `ferry dashboard` to the public internet; bind it to localhost or put it behind your own auth proxy.
- **Broker credentials** (`redis://` URLs with passwords) are read from your config, never logged by Ferry.
