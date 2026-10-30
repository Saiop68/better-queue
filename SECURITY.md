# Security Policy

## Supported versions

Only the latest released version receives security fixes.

| Version | Supported |
| ------- | --------- |
| 1.0.x   | yes       |

## Reporting a vulnerability

Please **don't** open a public issue for a security problem.

Email **betterqueue@users.noreply.github.com** with:

- what the issue is and what an attacker could do
- the version affected
- steps to reproduce, or a proof of concept
- any suggested fix

You'll get an acknowledgement within a few days. We'll tell you when we've
looked at it and when a fix is out.

## Scope

BetterQueue talks to Redis and executes closures you supply. It does not
execute untrusted code — the job functions are yours.

Things that are in scope:

- a remote user being able to influence queue state in a way the application
  didn't intend
- a job payload escaping to another queue
- unbounded memory growth from a malicious workload
- the rate limit being bypassed by a crafted sequence of calls

Things that are **not** vulnerabilities:

- an application that passes user input straight into a job closure
- using Redis Cluster (documented as unsupported — you'll get `CROSSSLOT`)
- losing in-flight closures when the process is killed (documented; the work
  has to be re-submitted by the application)
- an application that lets anyone enqueue unlimited jobs with no quota
