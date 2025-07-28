import asyncio
import pytest
from config.logging import setup_logging
from actors.actor_system import ActorSystem
from actors.messages import ActorMessage, MESSAGE_TYPES
from tests.fixtures import EchoActor

@pytest.mark.asyncio
async def test_retry_and_dlq():
    setup_logging()
    system = ActorSystem("test")
    
    # Создаем актор с очень маленькой очередью
    class TinyQueueActor(EchoActor):
        def __init__(self, actor_id: str, name: str):
            super().__init__(actor_id, name)
            self._message_queue = asyncio.Queue(maxsize=1)
    
    actor = TinyQueueActor("tiny", "TinyQueue")
    await system.register_actor(actor)
    
    # Не запускаем актор, чтобы очередь не обрабатывалась
    
    # Заполняем очередь
    msg1 = ActorMessage.create(sender_id="test", message_type=MESSAGE_TYPES['PING'])
    await system.send_message("tiny", msg1)
    
    # Следующее сообщение должно попасть в retry и затем в DLQ
    msg2 = ActorMessage.create(sender_id="test", message_type=MESSAGE_TYPES['PING'])
    try:
        await system.send_message("tiny", msg2)
    except asyncio.QueueFull:
        pass  # Ожидаемая ошибка - очередь переполнена
    
    # Проверяем Dead Letter Queue
    dlq = system.get_dead_letter_queue()
    print(f"\nDead Letter Queue содержит {len(dlq)} сообщений")
    
    # Очищаем DLQ
    cleared = system.clear_dead_letter_queue()
    print(f"Очищено {cleared} сообщений из DLQ")


if __name__ == "__main__":
    asyncio.run(test_retry_and_dlq())