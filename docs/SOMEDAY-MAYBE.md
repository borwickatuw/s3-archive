# Someday / Maybe

Ideas worth considering but deliberately out of scope for now. Not
commitments — a parking lot so they aren't lost.

## Resumable-extract: stale destination cleanup

Context: the planned resumable-extract feature writes a control object
to the destination prefix recording the source archive's identity
(`ETag` + size). On re-run it resumes only if the source is unchanged;
if the source *changed*, it does not resume.

Open question this defers: **what to do with destination objects left
over from a previous, different archive** extracted to the same prefix.

- **Current / baseline behavior:** treat it as the user's problem —
  overwrite the members the new archive contains, and leave any
  now-orphaned objects from the old archive in place. (This matches how
  extract behaves today: it overwrites colliding keys and never deletes.)
- **Someday:** offer real cleanup, but only behind an explicit
  `--force` (or `--overwrite` / `--delete-stale`) flag — never delete by
  default.
- **Fanciest version:** make the collision *content-aware* — detect
  whether a colliding destination object actually differs from what the
  new archive would write (size/hash), and only overwrite/replace what
  genuinely changed, leaving identical objects untouched. Avoids
  needless re-transfer and needless churn.

Deferred because safe deletion of destination data is a materially
riskier feature than the resume itself, and the baseline (overwrite,
leave orphans) is acceptable.
