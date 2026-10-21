//! Deterministic pause points inside the real cached-read helper, for tests
//! that need a fill to straddle a concurrent write. Compiled only with the
//! `test-support` feature; a production build has neither this module nor
//! the hook calls.
//!
//! ```ignore
//! let mut pause = test_support::pause_at(PausePoint::BeforeFill, move |s| s.key == key);
//! let read = tokio::spawn(async move { q.fetch_one(&pool, &mut redis).await });
//! pause.reached().await; // the read's SQL has returned; its fill has not run
//! write.execute(&pool, &mut redis).await?;
//! pause.release();
//! ```

use std::sync::{Mutex, PoisonError};

use tokio::sync::oneshot;

use crate::shared::value::SqlParam;

/// Where inside a cached read the helper parks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PausePoint
{
    /// The Redis lookup has returned, before SQL runs or a hit is returned.
    AfterLookup,
    /// SQL has returned, before the result is written to Redis.
    BeforeFill,
}

/// The read a matcher is asked about.
pub struct PauseSite<'a>
{
    /// The `sq:e:` entry key.
    pub key: &'a str,
    /// Renumbered SQL.
    pub sql: &'a str,
    pub binds: &'a [SqlParam],
    /// Every table the entry depends on.
    pub tables: &'a [&'static str],
    /// At `AfterLookup`, whether the lookup was a hit; always `false` at `BeforeFill`.
    pub hit: bool,
}

type Matcher = Box<dyn Fn(&PauseSite<'_>) -> bool + Send + Sync>;

struct Slot
{
    point: PausePoint,
    matcher: Matcher,
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

static SLOTS: Mutex<Vec<Slot>> = Mutex::new(Vec::new());

/// A registered pause. Dropping it without `release` also lets the read go.
pub struct Pause
{
    reached: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

impl Pause
{
    /// Waits until a matching read is parked at the pause point.
    ///
    /// # Panics
    /// Panics if the slot was dropped without being reached.
    pub async fn reached(&mut self)
    {
        (&mut self.reached).await.expect("pause slot dropped before it was reached");
    }

    /// Lets the parked read continue.
    pub fn release(self)
    {
        let _ = self.release.send(());
    }
}

/// Parks the first read at `point` for which `matcher` returns true. One-shot:
/// the slot is consumed by the read it parks.
pub fn pause_at(
    point: PausePoint,
    matcher: impl Fn(&PauseSite<'_>) -> bool + Send + Sync + 'static,
) -> Pause
{
    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    SLOTS.lock().unwrap_or_else(PoisonError::into_inner).push(Slot {
        point,
        matcher: Box::new(matcher),
        reached: reached_tx,
        release: release_rx,
    });
    Pause { reached: reached_rx, release: release_tx }
}

/// Called by the cache helper at each pause point.
pub(crate) async fn hook(point: PausePoint, site: &PauseSite<'_>)
{
    let slot = {
        let mut slots = SLOTS.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(i) = slots.iter().position(|s| s.point == point && (s.matcher)(site))
        else
        {
            return;
        };
        slots.swap_remove(i)
    };
    let _ = slot.reached.send(());
    let _ = slot.release.await;
}
