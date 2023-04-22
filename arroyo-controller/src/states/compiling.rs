use tracing::info;

use crate::compiler::ProgramCompiler;
use crate::states::StateError;

use super::{scheduling::Scheduling, Context, State, Transition};

#[derive(Debug)]
pub struct Compiling;

#[async_trait::async_trait]
impl State for Compiling {
    fn name(&self) -> &'static str {
        "Compiling"
    }

    async fn next(self: Box<Self>, ctx: &mut Context) -> Result<Transition, StateError> {
        if ctx.status.pipeline_path.is_some() {
            // 如果pipeline_path已存在，说明job已经被编译，直接进入下一阶段，开始调度job
            info!(
                message = "Pipeline already compiled",
                job_id = ctx.config.id,
            );
            return Ok(Transition::next(*self, Scheduling {}));
        }

        // 开始编译job
        info!(
            message = "Compiling pipeline",
            job_id = ctx.config.id,
            hash = ctx.program.get_hash()
        );

        let pc = ProgramCompiler::new(
            ctx.config.pipeline_name.clone(),
            ctx.config.id.clone(),
            ctx.program.clone(),
        );

        // 执行编译
        match pc.compile().await {
            Ok(res) => {
                ctx.status.pipeline_path = Some(res.pipeline_path);
                ctx.status.wasm_path = Some(res.wasm_path);
                // 编译完成，进入下一个阶段：调度job
                Ok(Transition::next(*self, Scheduling {}))
            }
            Err(e) => Err(e
                .downcast::<StateError>()
                .unwrap_or_else(|e| ctx.retryable(self, "Query compilation failed", e, 10))),
        }
    }
}
