from typing import Optional
import asyncio

from actors.base_actor import BaseActor
from actors.messages import ActorMessage, MESSAGE_TYPES
from config.settings import (
    STM_QUERY_TIMEOUT,
    STM_METRICS_ENABLED,
    STM_METRICS_LOG_INTERVAL
)
from database.connection import db_connection
from utils.monitoring import measure_latency
from utils.event_utils import EventVersionManager


class MemoryActor(BaseActor):
    """
    Актор для управления кратковременной памятью (STM).
    Сохраняет историю диалогов в PostgreSQL для контекста генерации.
    """
    
    def __init__(self):
        super().__init__("memory", "Memory")
        self._pool = None
        self._degraded_mode = False
        self._event_version_manager = EventVersionManager()
        
        # Метрики
        self._metrics = {
            'initialized': False,
            'degraded_mode_entries': 0,
            'store_memory_count': 0,
            'get_context_count': 0,
            'clear_memory_count': 0,
            'unknown_message_count': 0,
            'db_errors': 0
        }
        
        # Задача для периодического логирования метрик
        self._metrics_task: Optional[asyncio.Task] = None
        
    async def initialize(self) -> None:
        """Инициализация актора и проверка схемы БД"""
        try:
            # Проверяем, нужно ли подключаться
            if not db_connection._is_connected:
                await db_connection.connect()
            
            # Получаем пул подключений
            self._pool = db_connection.get_pool()
            
            # Проверяем существование таблицы и индексов
            await self._verify_schema()
            
            self._metrics['initialized'] = True
            
            # Запускаем периодическое логирование метрик
            if STM_METRICS_ENABLED:
                self._metrics_task = asyncio.create_task(self._metrics_loop())
            
            self.logger.info("MemoryActor initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize MemoryActor: {str(e)}")
            self._degraded_mode = True
            self._metrics['degraded_mode_entries'] += 1
            self._increment_metric('db_errors')
            self.logger.warning("MemoryActor entering degraded mode - will work without persistence")
    
    async def shutdown(self) -> None:
        """Освобождение ресурсов"""
        # Останавливаем метрики
        if self._metrics_task and not self._metrics_task.done():
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass
        
        # Логируем финальные метрики
        self._log_metrics(final=True)
        
        self.logger.info("MemoryActor shutdown completed")
    
    @measure_latency
    async def handle_message(self, message: ActorMessage) -> Optional[ActorMessage]:
        """Обработка входящих сообщений"""
        
        # Обработка STORE_MEMORY
        if message.message_type == MESSAGE_TYPES['STORE_MEMORY']:
            self._metrics['store_memory_count'] += 1
            await self._handle_store_memory(message)
            
        # Обработка GET_CONTEXT
        elif message.message_type == MESSAGE_TYPES['GET_CONTEXT']:
            self._metrics['get_context_count'] += 1
            return await self._handle_get_context(message)
            
        # Обработка CLEAR_USER_MEMORY
        elif message.message_type == MESSAGE_TYPES['CLEAR_USER_MEMORY']:
            self._metrics['clear_memory_count'] += 1
            await self._handle_clear_memory(message)
            
        else:
            self._metrics['unknown_message_count'] += 1
            self.logger.warning(
                f"Unknown message type received: {message.message_type}"
            )
        
        return None
    
    async def _verify_schema(self) -> None:
        """Проверка существования таблицы и индексов"""
        try:
            if self._pool is None:
                raise RuntimeError("Database pool not initialized")
                
            # Проверяем существование таблицы
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'stm_buffer'
                )
            """
            table_exists = await self._pool.fetchval(query, timeout=STM_QUERY_TIMEOUT)
            
            if not table_exists:
                raise RuntimeError("Table stm_buffer does not exist. Run migrations first.")
            
            # Проверяем индексы
            index_query = """
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = 'stm_buffer'
            """
            indexes = await self._pool.fetch(index_query, timeout=STM_QUERY_TIMEOUT)
            
            required_indexes = {
                'idx_stm_user_timestamp',
                'idx_stm_user_sequence', 
                'idx_stm_cleanup'
            }
            
            existing_indexes = {row['indexname'] for row in indexes}
            missing_indexes = required_indexes - existing_indexes
            
            if missing_indexes:
                self.logger.warning(f"Missing indexes: {missing_indexes}")
            
            self.logger.debug("Schema verification completed successfully")
            
        except Exception as e:
            self.logger.error(f"Schema verification failed: {str(e)}")
            raise
    
    async def _handle_store_memory(self, message: ActorMessage) -> None:
        """Обработчик сохранения в память (заглушка для этапа 3.2.1)"""
        if self._degraded_mode:
            self.logger.debug(
                f"STORE_MEMORY in degraded mode for user {message.payload.get('user_id')}"
            )
            return
        
        # TODO: Реализация в этапе 3.2.2
        self.logger.debug("STORE_MEMORY handler called (stub)")
    
    async def _handle_get_context(self, message: ActorMessage) -> Optional[ActorMessage]:
        """Обработчик получения контекста (заглушка для этапа 3.2.1)"""
        if self._degraded_mode:
            # В degraded mode возвращаем пустой контекст
            return ActorMessage.create(
                sender_id=self.actor_id,
                message_type=MESSAGE_TYPES['CONTEXT_RESPONSE'],
                payload={
                    'user_id': message.payload.get('user_id'),
                    'context': [],
                    'degraded_mode': True
                }
            )
        
        # TODO: Реализация в этапе 3.2.2
        self.logger.debug("GET_CONTEXT handler called (stub)")
        
        # Возвращаем заглушку
        return ActorMessage.create(
            sender_id=self.actor_id,
            message_type=MESSAGE_TYPES['CONTEXT_RESPONSE'],
            payload={
                'user_id': message.payload.get('user_id'),
                'context': [],
                'stub': True
            }
        )
    
    async def _handle_clear_memory(self, message: ActorMessage) -> None:
        """Обработчик очистки памяти (заглушка для этапа 3.2.1)"""
        if self._degraded_mode:
            self.logger.debug(
                f"CLEAR_USER_MEMORY in degraded mode for user {message.payload.get('user_id')}"
            )
            return
        
        # TODO: Реализация в этапе 3.2.2
        self.logger.debug("CLEAR_USER_MEMORY handler called (stub)")
    
    def _increment_metric(self, metric_name: str, value: int = 1) -> None:
        """Инкремент метрики"""
        if metric_name in self._metrics:
            self._metrics[metric_name] += value
    
    async def _metrics_loop(self) -> None:
        """Периодическое логирование метрик"""
        while self.is_running:
            try:
                await asyncio.sleep(STM_METRICS_LOG_INTERVAL)
                self._log_metrics()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in metrics loop: {str(e)}")
    
    def _log_metrics(self, final: bool = False) -> None:
        """Логирование метрик"""
        log_msg = "MemoryActor metrics"
        if final:
            log_msg = "MemoryActor final metrics"
        
        self.logger.info(
            f"{log_msg} - "
            f"Store: {self._metrics['store_memory_count']}, "
            f"Get: {self._metrics['get_context_count']}, "
            f"Clear: {self._metrics['clear_memory_count']}, "
            f"Unknown: {self._metrics['unknown_message_count']}, "
            f"DB errors: {self._metrics['db_errors']}, "
            f"Degraded mode: {self._degraded_mode}"
        )