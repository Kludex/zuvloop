---------------------------- MODULE ThreadSafeHandle ----------------------------
(***************************************************************************)
(* The `ThreadSafeHandle` cancel protocol from zuvloop's zig/tshandle.zig.  *)
(*                                                                         *)
(* One loop thread runs the callback; any number of foreign threads call    *)
(* `cancel()`. The handle holds an atomic state word - PENDING, RUNNING,    *)
(* then DONE - and a list of parked waiters that the loop thread releases   *)
(* once the callback has finished.                                          *)
(*                                                                         *)
(* What is worth a model is how that waiter list stays safe. Both sides     *)
(* reach for it, and the protocol holding them apart is:                    *)
(*                                                                         *)
(*   loop thread   `run()` holds PyCriticalSection(handle) for the whole    *)
(*                 run, including publishing DONE and draining the list.    *)
(*   canceller     `cancel()` is registered with `py.methodNoArgs`, whose   *)
(*                 `wrapNoArgs` wrapper holds PyCriticalSection(handle)     *)
(*                 across the call. It links its waiter under that lock,    *)
(*                 and then `awaitCompletion` calls `PyEval_SaveThread()`,  *)
(*                 which releases the section, before it blocks.           *)
(*                                                                         *)
(* So the lock is what makes "read the state word, then link" atomic        *)
(* against the drain, and dropping it before parking is what stops the two  *)
(* sides deadlocking on each other. Both halves are load-bearing and        *)
(* neither is visible in `cancel()` itself: the lock arrives from the       *)
(* method wrapper, and the release happens inside a helper.                 *)
(*                                                                         *)
(* A critical section is not held across a block. CPython's                 *)
(* Include/cpython/critical_section.h has it that on `_PyThreadState_       *)
(* Detach()` - "before a blocking I/O operation or when waiting to acquire  *)
(* a lock" - a thread "suspends all of its active critical sections,        *)
(* temporarily releasing the associated locks", and resumes the top-most    *)
(* one on attach. The callback is arbitrary Python, so `LoopSuspend` models  *)
(* that gap. Without it a canceller could never observe RUNNING and the     *)
(* waiter list would be dead code, so it is the whole reason the protocol   *)
(* exists - and `ThreadSafeHandle_witness.cfg` is there to keep the model   *)
(* honest about reaching it.                                                *)
(*                                                                         *)
(* CANCEL_HOLDS_LOCK = TRUE models the code as written. FALSE models        *)
(* `cancel` registered with `methodNoArgsUnlocked` instead, and TLC then    *)
(* finds the lost wakeup that change would introduce - which makes this     *)
(* spec a regression guard for a requirement nothing in the code states.    *)
(***************************************************************************)

EXTENDS Naturals

CONSTANTS Cancellers,        \* the foreign threads calling cancel()
          CANCEL_HOLDS_LOCK  \* TRUE = methodNoArgs (as written), FALSE = Unlocked

VARIABLES
    state,      \* "pending" | "running" | "done"
    waiters,    \* cancellers currently linked into the waiter list
    drained,    \* cancellers whose lock the loop thread has released
    runs,       \* how many times the callback body has run
    lock,       \* "free", "loop", or a canceller: who holds PyCriticalSection(handle)
    loopPc,
    pc

vars == <<state, waiters, drained, runs, lock, loopPc, pc>>

CancellerLabels == {"start", "deciding", "linking", "parking", "parked", "leaving", "returned"}
LoopLabels == {"start", "body", "suspended", "resumed", "draining", "releasing", "gone"}

Init ==
    /\ state = "pending"
    /\ waiters = {}
    /\ drained = {}
    /\ runs = 0
    /\ lock = "free"
    /\ loopPc = "start"
    /\ pc = [t \in Cancellers |-> "start"]

(*************************  the loop thread  *******************************)

(* `run()` takes the critical section, then compare-and-swaps into RUNNING. *)
LoopEnter ==
    /\ loopPc = "start"
    /\ lock = "free"
    /\ lock' = "loop"
    /\ IF state = "pending"
         THEN /\ state' = "running"
              /\ runs' = runs + 1
              /\ loopPc' = "body"
         ELSE /\ UNCHANGED <<state, runs>>
              /\ loopPc' = "releasing"     \* cancelled before it was picked up
    /\ UNCHANGED <<waiters, drained, pc>>

(* The callback body blocks on something, so the interpreter suspends the    *)
(* critical section and hands the lock back. Modelled as happening at most   *)
(* once: one gap is enough to let every canceller in, because parking gives  *)
(* the lock up again, and a second gap adds interleavings rather than shapes. *)
LoopSuspend ==
    /\ loopPc = "body"
    /\ lock' = "free"
    /\ loopPc' = "suspended"
    /\ UNCHANGED <<state, waiters, drained, runs, pc>>

LoopResume ==
    /\ loopPc = "suspended"
    /\ lock = "free"
    /\ lock' = "loop"
    /\ loopPc' = "resumed"
    /\ UNCHANGED <<state, waiters, drained, runs, pc>>

(* The `defer` block publishes DONE and then drains, as two steps the way *)
(* the code has them - both under the lock the loop is still holding.     *)
LoopPublishDone ==
    /\ loopPc \in {"body", "resumed"}
    /\ state' = "done"
    /\ loopPc' = "draining"
    /\ UNCHANGED <<waiters, drained, runs, lock, pc>>

LoopDrain ==
    /\ loopPc = "draining"
    /\ drained' = drained \cup waiters
    /\ waiters' = {}
    /\ loopPc' = "releasing"
    /\ UNCHANGED <<state, runs, lock, pc>>

LoopRelease ==
    /\ loopPc = "releasing"
    /\ lock' = "free"
    /\ loopPc' = "gone"
    /\ UNCHANGED <<state, waiters, drained, runs, pc>>

LoopStep ==
    \/ LoopEnter \/ LoopSuspend \/ LoopResume
    \/ LoopPublishDone \/ LoopDrain \/ LoopRelease

(*************************  a cancelling thread  ***************************)

(* `wrapNoArgs` takes the critical section before `cancel()` runs. *)
CancelEnter(t) ==
    /\ pc[t] = "start"
    /\ IF CANCEL_HOLDS_LOCK THEN lock = "free" ELSE TRUE
    /\ lock' = IF CANCEL_HOLDS_LOCK THEN t ELSE lock
    /\ pc' = [pc EXCEPT ![t] = "deciding"]
    /\ UNCHANGED <<state, waiters, drained, runs, loopPc>>

(* Read the state word and decide: win the race, find it already done, or park. *)
CancelDecide(t) ==
    /\ pc[t] = "deciding"
    /\ IF state = "pending"
         THEN /\ state' = "done"                       \* CAS pending -> done wins
              /\ pc' = [pc EXCEPT ![t] = "leaving"]
         ELSE IF state = "done"
           THEN /\ pc' = [pc EXCEPT ![t] = "leaving"]
                /\ UNCHANGED state
           ELSE /\ pc' = [pc EXCEPT ![t] = "linking"]  \* saw RUNNING: must park
                /\ UNCHANGED state
    /\ UNCHANGED <<waiters, drained, runs, lock, loopPc>>

(* `awaitCompletion`: link the node, then `PyEval_SaveThread()` drops the  *)
(* critical section, then block on the lock.                              *)
CancelLink(t) ==
    /\ pc[t] = "linking"
    /\ waiters' = waiters \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "parking"]
    /\ UNCHANGED <<state, drained, runs, lock, loopPc>>

CancelPark(t) ==
    /\ pc[t] = "parking"
    /\ lock' = IF lock = t THEN "free" ELSE lock   \* PyEval_SaveThread releases it
    /\ pc' = [pc EXCEPT ![t] = "parked"]
    /\ UNCHANGED <<state, waiters, drained, runs, loopPc>>

CancelWake(t) ==
    /\ pc[t] = "parked"
    /\ t \in drained
    /\ pc' = [pc EXCEPT ![t] = "leaving"]
    /\ UNCHANGED <<state, waiters, drained, runs, lock, loopPc>>

CancelLeave(t) ==
    /\ pc[t] = "leaving"
    /\ lock' = IF lock = t THEN "free" ELSE lock
    /\ pc' = [pc EXCEPT ![t] = "returned"]
    /\ UNCHANGED <<state, waiters, drained, runs, loopPc>>

CancelStep(t) ==
    \/ CancelEnter(t) \/ CancelDecide(t) \/ CancelLink(t)
    \/ CancelPark(t) \/ CancelWake(t) \/ CancelLeave(t)

Next == LoopStep \/ \E t \in Cancellers : CancelStep(t)

(* Each thread is scheduled on its own, so fairness is per thread rather *)
(* than over `Next` as a whole: one thread making progress must not be   *)
(* allowed to stand in for another that never runs again.                *)
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(LoopStep)
    /\ \A t \in Cancellers : WF_vars(CancelStep(t))

(*****************************  properties  ********************************)

TypeOK ==
    /\ state \in {"pending", "running", "done"}
    /\ waiters \subseteq Cancellers
    /\ drained \subseteq Cancellers
    /\ runs \in Nat
    /\ lock \in {"free", "loop"} \cup Cancellers
    /\ loopPc \in LoopLabels
    /\ pc \in [Cancellers -> CancellerLabels]

(* The callback body must never run twice. *)
AtMostOneRun == runs <= 1

(* At most one holder of the critical section, ever - neither the loop      *)
(* against a canceller, nor two cancellers against each other.             *)
(*                                                                         *)
(* `InSection` is where a canceller is holding it: `CancelEnter` takes it   *)
(* and `CancelPark` gives it back. "leaving" is deliberately not in there,  *)
(* because it is reached two ways - straight from `CancelDecide`, still     *)
(* holding the section, or from `CancelWake` after having given it up - and *)
(* a woken waiter is in "leaving" while the loop still holds the lock it is *)
(* about to release. `CancelLeave` releases only if it is the holder.       *)
InSection == {"deciding", "linking", "parking"}

MutualExclusion ==
    /\ (lock = "loop") => \A t \in Cancellers : pc[t] \notin InSection
    /\ \A t1, t2 \in Cancellers :
         (t1 # t2 /\ pc[t1] \in InSection) => pc[t2] \notin InSection

(* The lost wakeup: the loop has finished and released, and yet a thread is *)
(* parked that was never drained, so nothing is left to release it.         *)
NoStrandedWaiter ==
    (loopPc = "gone")
        => \A t \in Cancellers : (pc[t] = "parked") => (t \in drained)

(* Liveness: nobody blocks forever - neither a canceller nor the loop. *)
EveryCancelReturns == \A t \in Cancellers : <>(pc[t] = "returned")
LoopAlwaysFinishes == <>(loopPc = "gone")

(* A coverage check rather than a property, and the one config that wants to *)
(* fail: everything above holds vacuously of a model that cannot reach a     *)
(* parked waiter at all, which is what an earlier draft of this spec did.    *)
(* TLC has to report this violated, and the counterexample it prints is the  *)
(* witness that a canceller does link, park, and get released.               *)
WaiterPathUnreached ==
    \A t \in Cancellers : ~(pc[t] = "parked" /\ t \in drained)

=============================================================================
