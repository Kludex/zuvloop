# Formal verification

A TLA+ model of the `ThreadSafeHandle` cancel protocol in
[`zig/tshandle.zig`](../zig/tshandle.zig), checked with TLC.

```bash
export TLA_TOOLS_JAR=/path/to/tla2tools.jar   # v1.7.4
./scripts/check-model
```

## Why this protocol

`call_soon_threadsafe` hands a handle to the loop and any number of foreign
threads may then call `cancel()` on it. Both sides touch the same waiter list,
and two things keep that safe — neither of them visible at the call site:

- `cancel` is registered with `py.methodNoArgs`, so the `wrapNoArgs` wrapper
  holds `PyCriticalSection(handle)` across the call. That is what makes "read
  the state word, then link a waiter" atomic against the loop's drain.
- `awaitCompletion` then calls `PyEval_SaveThread()`, which releases that
  section, *before* it blocks. That is what stops the two sides deadlocking.

Swap `methodNoArgs` for `methodNoArgsUnlocked` and the first breaks; keep the
section held while parking and the second does. Neither change looks wrong
reading `cancel()`, and neither is stated anywhere in the code. The model is
where they are written down.

## The models

`scripts/check-model` runs four configs against the one spec and asserts what
each must report. Only the first is expected to come back clean; the other three
must each report a named violation:

| config | TLC must report |
| --- | --- |
| `ThreadSafeHandle` | no error — the protocol as the code has it |
| `ThreadSafeHandle_witness` | `WaiterPathUnreached` violated |
| `ThreadSafeHandle_unlocked` | `MutualExclusion` violated |
| `ThreadSafeHandle_unlocked_strand` | `NoStrandedWaiter` violated |

The two `unlocked` configs set `CANCEL_HOLDS_LOCK = FALSE`, standing in for
`cancel` registered without the critical section. The first shows the immediate
symptom; the second omits `MutualExclusion` so TLC carries on to the consequence
— a canceller that links its waiter after the drain has already run and then
parks on a lock nobody is left to release.

Asserting the failures, rather than just checking that the good model passes, is
what keeps the spec from going quietly toothless.

## The witness config

An earlier draft of this spec held the loop's critical section across the entire
run. Everything passed — and none of it meant anything, because a canceller
needs that same section to read the state word, so it could never observe
`RUNNING`, never link a waiter, and never exercise the protocol at all. Both
halves of the mechanism could be deleted from the model with TLC still reporting
no error.

The fix is `LoopSuspend`. A critical section is not held across a block:
CPython's `Include/cpython/critical_section.h` has it that on
`_PyThreadState_Detach()` — "before a blocking I/O operation or when waiting to
acquire a lock" — a thread "suspends all of its active critical sections,
temporarily releasing the associated locks". The callback is arbitrary Python,
so that gap is real, and it is the only reason the waiter list needs to exist.

`ThreadSafeHandle_witness` is what stops that regressing. It checks an invariant
that holds only if no canceller ever parks and is released, and requires TLC to
break it. A model that stops reaching the waiter list fails there instead of
passing vacuously everywhere else.

## Scope

The model covers the handle protocol, not the Zig. Two cancellers, one loop
thread, and at most one suspension gap: one gap already admits both cancellers,
since parking gives the lock back, so further gaps add interleavings rather than
new shapes. Nothing here models the ready queue, the timer heap, or libuv.
