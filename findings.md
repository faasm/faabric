I've come up with the following:

Assumptions/ background:

- There are two types of `userfaultfd` tracking: `MISSING` (event triggered when
  page is first accessed and created by the kernel) `WP` (event triggered on a
  write to a write-protected page).
- `userfaultfd` distinguishes between two types of memory: private and shared.
  Function memory in Faabric is shared as it's mapped from a fd created with
  `memfd_create`.
- There are two ways to receive `userfaultfd` events, via `SIGBUS` or via a
  background thread which calls `poll`.

For a newly mapped/ created page, we will see the following events:

- Initial `MISSING` event when the page is _either_ read from or written to the
  first time.
- Subsequent `WP` event if the page has been write-protected and it's written
  to _again_.
- Thus, a page that's only written to once, will only get one `MISSING` event,
  so these must be included in the dirty tracking.

Observations:

- All function memory in Faabric will have been freshly mapped just before
  execution, so will always trigger a `MISSING` event on the first read _or_
  write (i.e. we can't ignore this in case there's only one write to that page).
- `userfaultfd` write-protected pages don't work with shared memory (hence can't
  be used for Faabric dirty tracking).
- When using `FEATURE_MISSING`, an event is created for reads and writes (i.e.
  any time the page needs to be created by the kernel).
- When using `SIGBUS`, it's not possible to distinguish between reads and
  writes.
- When using a background thread, we can distinguish between reads and writes
  via the `flags` on the `userfaultfd` event.
