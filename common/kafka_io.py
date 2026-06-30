import asyncio
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from common.events import Event

log = logging.getLogger("kafka_io")


class EventConsumer:
    def __init__(self, topics, group_id, bootstrap, handler, idempotency, max_retries=3):
        self.topics = topics
        self.group_id = group_id
        self.bootstrap = bootstrap
        self.handler = handler
        self.idempotency = idempotency
        self.max_retries = max_retries
        self._consumer = None
        self._producer = None
        self._task = None

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap,
            group_id=self.group_id,
            enable_auto_commit=True,          # background, non-blocking
            auto_commit_interval_ms=1000,     # commit offsets every 1s
            auto_offset_reset="earliest",
        )
        self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap)
        await self._consumer.start()
        await self._producer.start()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task:
            self._task.cancel()
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

    async def _run(self):
        # At-least-once delivery: offsets auto-commit on an interval.
        # Reprocessing on crash is safe because consumers are idempotent
        # (Redis dedup + Stripe idempotency keys).
        async for msg in self._consumer:
            event = Event.from_bytes(msg.value)
            if await self.idempotency.already_processed(self.group_id, event.event_id):
                continue
            ok = await self._process_with_retry(event)
            if ok:
                await self.idempotency.mark_processed(self.group_id, event.event_id)
            else:
                await self._send_to_dlq(msg, event)

    async def _process_with_retry(self, event) -> bool:
        for attempt in range(1, self.max_retries + 1):
            try:
                await self.handler(event)
                return True
            except Exception as e:
                log.warning(
                    "handler failed %s/%s for %s: %s",
                    attempt, self.max_retries, event.event_id, e,
                )
                await asyncio.sleep(0.5 * attempt)
        return False

    async def _send_to_dlq(self, msg, event):
        dlq_topic = f"{msg.topic}.dlq"
        await self._producer.send_and_wait(dlq_topic, msg.value, key=msg.key)
        log.error("sent event %s to %s", event.event_id, dlq_topic)