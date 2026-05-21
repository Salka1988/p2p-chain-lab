use std::{future::Future, time::Duration};

use tokio::{task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TaskGroup {
    cancellation_token: CancellationToken,
    shutdown_timeout: Duration,
    tasks: JoinSet<TaskResult>,
}

struct TaskResult {
    name: &'static str,
    result: anyhow::Result<()>,
}

impl TaskGroup {
    pub fn new() -> Self {
        Self {
            cancellation_token: CancellationToken::new(),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            tasks: JoinSet::new(),
        }
    }

    pub fn with_shutdown_timeout(mut self, shutdown_timeout: Duration) -> Self {
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    pub fn child_token(&self) -> CancellationToken {
        self.cancellation_token.child_token()
    }

    pub fn spawn(
        &mut self,
        name: &'static str,
        task: impl Future<Output = anyhow::Result<()>> + Send + 'static,
    ) {
        self.tasks.spawn(async move {
            TaskResult {
                name,
                result: task.await,
            }
        });
    }

    pub async fn wait(mut self) -> anyhow::Result<()> {
        let first_exit = tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                tracing::info!("shutdown signal received");
                self.cancellation_token.cancel();
                None
            }
            result = self.tasks.join_next() => {
                self.cancellation_token.cancel();
                Some(Self::task_result(result)?)
            }
        };

        let mut error = match first_exit {
            Some(Ok(name)) => Some(anyhow::anyhow!("{name} stopped unexpectedly")),
            Some(Err(error)) => Some(error),
            None => None,
        };

        match timeout(self.shutdown_timeout, self.join_remaining(&mut error)).await {
            Ok(result) => result?,
            Err(_) => {
                self.tasks.abort_all();
                let timeout_error = anyhow::anyhow!(
                    "shutdown timed out after {:?}; aborted remaining tasks",
                    self.shutdown_timeout
                );
                if error.is_none() {
                    error = Some(timeout_error);
                } else {
                    tracing::warn!(?timeout_error);
                }
            }
        }

        if let Some(error) = error {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn join_remaining(&mut self, error: &mut Option<anyhow::Error>) -> anyhow::Result<()> {
        while let Some(result) = self.tasks.join_next().await {
            match Self::task_result(Some(result))? {
                Ok(name) => tracing::debug!(task = name, "task stopped"),
                Err(task_error) => {
                    tracing::warn!(?task_error, "task stopped with error");
                    if error.is_none() {
                        *error = Some(task_error);
                    }
                }
            }
        }

        Ok(())
    }

    fn task_result(
        result: Option<Result<TaskResult, tokio::task::JoinError>>,
    ) -> anyhow::Result<Result<&'static str, anyhow::Error>> {
        let Some(result) = result else {
            return Ok(Err(anyhow::anyhow!("all tasks stopped unexpectedly")));
        };
        let task_result = result?;

        match task_result.result {
            Ok(()) => Ok(Ok(task_result.name)),
            Err(error) => Ok(Err(error.context(format!("{} failed", task_result.name)))),
        }
    }
}
