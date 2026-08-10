# Executive Summary (plain English)

**The problem.** A security team can only protect what it can see. In a large
cloud estate, nobody has a clean, current map of *what resources exist, which
ones talk to each other, who is acting on them,* and *which belong to the same
application.* That map is the foundation for security reviews, threat modeling,
and incident response.

**What this project does.** It takes three raw security data sources — network
connection logs, API-activity logs, and a resource inventory — and turns them,
automatically, into that map. It runs in six steps: land the raw data, clean and
de-duplicate it, hide sensitive fields, check the numbers, build the map, and load
it into a database analysts can query.

**How well it works (measured, not claimed).** On a full run it processed
**12.35 million records in about 90 seconds**, removed **300,000 duplicate
records** created by at-least-once log delivery, passed **all five automated
quality checks**, and grouped **50,000 resources into 1,232 applications** based
on who talks to whom. A second, independent tool re-checked every headline number
and confirmed **zero** sensitive fields leaked past the masking step.

**Why it matters for the role.** It demonstrates the exact day-job of a security
data engineer: building reliable, governed, tested pipelines that turn messy
high-volume telemetry into datasets other teams can trust — with the quality gate
that means *nothing ships unverified.*

**Honesty note.** The data is synthetic, generated to match the public formats of
VPC Flow Logs and CloudTrail. It is not real production data. The engineering —
the pipeline, the model, the governance, and the tests — is real and runs.
