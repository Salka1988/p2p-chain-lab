use std::{future::Future, time::Duration};

use anyhow::{Context, Result};
use tokio::{task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(100);

pub struct TaskGroup {
    cancellation_token: CancellationToken,
    shutdown_timeout: Duration,
    tasks: JoinSet<TaskResult>,
}

struct TaskResult {
    name: &'static str,
    result: Result<()>,
}

impl Default for TaskGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskGroup {
    pub fn new() -> Self {
        Self {
            cancellation_token: CancellationToken::new(),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            tasks: JoinSet::new(),
        }
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn child_token(&self) -> CancellationToken {
        self.cancellation_token.child_token()
    }

    pub fn spawn(
        &mut self,
        name: &'static str,
        task: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        self.tasks.spawn(async move {
            TaskResult {
                name,
                result: task.await,
            }
        });
    }

    pub async fn wait(&mut self) -> Result<()> {
        if self.tasks.is_empty() {
            anyhow::bail!("cannot wait on a task group without tasks");
        }

        let mut error = tokio::select! {
            biased;

            _ = self.cancellation_token.cancelled() => {
                tracing::info!("programmatic shutdown requested");
                None
            }
            signal = shutdown_signal() => match signal {
                Ok(()) => {
                    tracing::info!("shutdown signal received");
                    None
                }
                Err(signal_error) => Some(anyhow::Error::new(signal_error)
                    .context("failed to receive shutdown signal")),
            },
            result = self.tasks.join_next() => match Self::task_result(result) {
                Ok(name) => Some(anyhow::anyhow!("{name} stopped unexpectedly")),
                Err(task_error) => Some(task_error),
            },
        };

        self.cancellation_token.cancel();
        tracing::info!(
            remaining_tasks = self.tasks.len(),
            "waiting for tasks to shut down"
        );

        if timeout(self.shutdown_timeout, self.join_remaining(&mut error))
            .await
            .is_err()
        {
            let remaining = self.tasks.len();
            tracing::warn!(remaining_tasks = remaining, ?self.shutdown_timeout, "graceful shutdown timed out");

            self.tasks.abort_all();

            if timeout(ABORT_DRAIN_TIMEOUT, self.tasks.shutdown())
                .await
                .is_err()
            {
                tracing::warn!(
                    remaining_tasks = self.tasks.len(),
                    "some aborted tasks did not terminate promptly"
                );
                self.tasks = JoinSet::new();
            }

            let timeout_error = anyhow::anyhow!(
                "shutdown timed out after {:?}; aborted {} remaining task(s)",
                self.shutdown_timeout,
                remaining,
            );
            error = Some(match error {
                Some(previous) => previous.context(timeout_error.to_string()),
                None => timeout_error,
            });
        }

        if let Some(error) = error {
            tracing::error!(?error, "task group stopped with error");
            Err(error)
        } else {
            tracing::info!("task group shut down gracefully");
            Ok(())
        }
    }

    async fn join_remaining(&mut self, error: &mut Option<anyhow::Error>) {
        while let Some(result) = self.tasks.join_next().await {
            match Self::task_result(Some(result)) {
                Ok(name) => tracing::debug!(task = name, "task stopped"),
                Err(task_error) => {
                    tracing::warn!(?task_error, "task stopped with error");
                    if error.is_none() {
                        *error = Some(task_error);
                    }
                }
            }
        }
    }

    fn task_result(
        result: Option<Result<TaskResult, tokio::task::JoinError>>,
    ) -> Result<&'static str> {
        let Some(result) = result else {
            anyhow::bail!("all supervised tasks stopped unexpectedly");
        };
        let task_result = result.context("supervised task panicked or was cancelled")?;
        task_result
            .result
            .with_context(|| format!("{} failed", task_result.name))?;
        Ok(task_result.name)
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        result = terminate.recv() => match result {
            Some(()) => Ok(()),
            None => Err(std::io::Error::other("SIGTERM signal stream closed")),
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn reports_timeout_when_remaining_task_ignores_shutdown() -> Result<(), &'static str> {
        let mut task_group = TaskGroup::new().with_shutdown_timeout(Duration::from_millis(20));
        task_group.spawn("fast task", async { Ok(()) });
        task_group.spawn("stuck task", async {
            std::future::pending::<anyhow::Result<()>>().await
        });

        let started_at = Instant::now();
        let error = task_group
            .wait()
            .await
            .err()
            .ok_or("expected task group to fail")?;

        assert!(started_at.elapsed() >= Duration::from_millis(20));
        assert!(error.to_string().contains("shutdown timed out after"));
        Ok(())
    }

    #[tokio::test]
    async fn reports_task_failure() -> Result<(), &'static str> {
        let mut task_group = TaskGroup::new().with_shutdown_timeout(Duration::from_millis(20));
        task_group.spawn("failing task", async { anyhow::bail!("boom") });

        let error = task_group
            .wait()
            .await
            .err()
            .ok_or("expected task group to fail")?;

        assert!(error.to_string().contains("failing task failed"));
        Ok(())
    }

    async fn panicking_task() -> anyhow::Result<()> {
        panic!("unexpected task panic");
    }

    #[tokio::test]
    async fn handles_task_panic_and_cleans_up_other_tasks() {
        let mut task_group = TaskGroup::new();
        let token = task_group.child_token();
        let (done_tx, done_rx) = oneshot::channel();

        task_group.spawn("cooperative task", async move {
            token.cancelled().await;
            let _ = done_tx.send(());
            Ok(())
        });
        task_group.spawn("panicking task", panicking_task());

        let error = task_group.wait().await.expect_err("expected panic error");

        assert!(error.to_string().contains("panicked"));
        assert!(done_rx.await.is_ok());
    }

    async fn panic_during_cleanup(token: CancellationToken) -> anyhow::Result<()> {
        token.cancelled().await;
        panic!("cleanup panic");
    }

    #[tokio::test]
    async fn continues_shutdown_when_another_task_panics() {
        let mut task_group = TaskGroup::new();
        let token = task_group.child_token();

        task_group.spawn("failing task", async { anyhow::bail!("initial failure") });
        task_group.spawn("cleanup panic task", panic_during_cleanup(token));

        let error = task_group.wait().await.expect_err("expected error");

        assert!(error.to_string().contains("failing task failed"));
    }

    #[tokio::test]
    async fn rejects_empty_task_group() {
        let mut task_group = TaskGroup::new();
        let error = task_group.wait().await.expect_err("expected error");

        assert!(error.to_string().contains("without tasks"));
    }
}
