"""Example handler.

Replace this module with your own handlers. The topic ``dispatch-demo`` is
only used to keep the example self-contained.
"""

from __future__ import annotations

from ntfy_dispatch import HandlerContext, Message, handler


@handler(topics=["dispatch-demo"], name="example", queue_size=100)
def example(msg: Message, ctx: HandlerContext) -> None:
    ctx.logger.info(
        "received id=%s topic=%s title=%r priority=%s tags=%s",
        msg.id,
        msg.topic,
        msg.title,
        msg.priority,
        ",".join(msg.tags),
    )

    if ctx.config.get("print_body", False):
        ctx.logger.info("message body: %s", msg.message)

    # Example of calling an HTTP API with the per-handler client:
    #
    # target = ctx.config.get("target")
    # if target:
    #     response = ctx.http.post(
    #         str(target),
    #         json={"id": msg.id, "topic": msg.topic, "message": msg.message},
    #     )
    #     response.raise_for_status()
