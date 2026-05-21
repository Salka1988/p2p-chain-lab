use std::future::Future;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub struct TaskGroup {
    cancellation_token: CancellationToken,
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
            tasks: JoinSet::new(),
        }
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
        let result = tokio::select! {
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

        let mut error = match result {
            Some(Ok(name)) => Some(anyhow::anyhow!("{name} stopped unexpectedly")),
            Some(Err(error)) => Some(error),
            None => None,
        };

        while let Some(result) = self.tasks.join_next().await {
            match Self::task_result(Some(result))? {
                Ok(_) => {}
                Err(task_error) => {
                    if error.is_none() {
                        error = Some(task_error);
                    }
                }
            }
        }

        if let Some(error) = error {
            Err(error)
        } else {
            Ok(())
        }
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
