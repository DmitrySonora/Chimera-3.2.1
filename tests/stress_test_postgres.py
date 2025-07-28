#!/usr/bin/env python3
"""
Стресс-тесты для PostgreSQL Event Store
Проверяют производительность под высокой нагрузкой и конкурентным доступом
"""
import asyncio
import time
import statistics
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import os

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from actors.events import BaseEvent, PostgresEventStore, EventStoreConcurrencyError
from database.connection import db_connection
from config.logging import setup_logging


class PerformanceMetrics:
    """Сбор и анализ метрик производительности"""
    
    def __init__(self, name: str):
        self.name = name
        self.measurements: List[float] = []
        
    def add(self, duration_ms: float):
        self.measurements.append(duration_ms)
        
    def get_stats(self) -> Dict[str, float]:
        if not self.measurements:
            return {}
            
        sorted_measurements = sorted(self.measurements)
        return {
            'count': len(self.measurements),
            'min': min(self.measurements),
            'max': max(self.measurements),
            'mean': statistics.mean(self.measurements),
            'median': statistics.median(self.measurements),
            'p95': sorted_measurements[int(len(sorted_measurements) * 0.95)],
            'p99': sorted_measurements[int(len(sorted_measurements) * 0.99)],
        }
    
    def print_report(self, target_ms: float = None):
        stats = self.get_stats()
        if not stats:
            print(f"{self.name}: No measurements")
            return
            
        print(f"\n{self.name}:")
        print(f"  Samples: {stats['count']}")
        print(f"  Min: {stats['min']:.2f}ms")
        print(f"  Mean: {stats['mean']:.2f}ms")
        print(f"  Median: {stats['median']:.2f}ms")
        print(f"  P95: {stats['p95']:.2f}ms")
        print(f"  P99: {stats['p99']:.2f}ms")
        print(f"  Max: {stats['max']:.2f}ms")
        
        if target_ms:
            success_rate = sum(1 for m in self.measurements if m <= target_ms) / len(self.measurements) * 100
            status = "✅ PASS" if stats['p95'] <= target_ms else "❌ FAIL"
            print(f"  Target: < {target_ms}ms (P95) - {status}")
            print(f"  Success rate: {success_rate:.1f}%")


class EventStoreStressTest:
    """Стресс-тесты для PostgreSQL Event Store"""
    
    def __init__(self):
        self.store: PostgresEventStore = None
        self.metrics = {
            'write_single': PerformanceMetrics('Single Write Latency'),
            'write_batch': PerformanceMetrics('Batch Write Latency'),
            'read_100': PerformanceMetrics('Read 100 Events Latency'),
            'flush_100': PerformanceMetrics('Flush 100 Events Latency'),
            'concurrent_write': PerformanceMetrics('Concurrent Write Latency'),
            'version_conflict': PerformanceMetrics('Version Conflict Resolution'),
        }
        
    async def setup(self):
        """Инициализация тестового окружения"""
        setup_logging()
        print("🚀 Initializing stress test environment...")
        
        # Создаем Event Store
        self.store = PostgresEventStore()
        await self.store.initialize()
        
        # Очищаем тестовые данные
        await db_connection.execute("DELETE FROM events WHERE stream_id LIKE 'stress_test_%'")
        print("✅ Environment ready")
        
    async def cleanup(self):
        """Очистка после тестов"""
        print("\n🧹 Cleaning up...")
        await db_connection.execute("DELETE FROM events WHERE stream_id LIKE 'stress_test_%'")
        await self.store.close()
        print("✅ Cleanup complete")
        
    async def test_single_write_latency(self, iterations: int = 1000):
        """Тест латентности единичной записи"""
        print(f"\n📝 Testing single write latency ({iterations} iterations)...")
        
        for i in range(iterations):
            event = BaseEvent.create(
                stream_id=f"stress_test_single_{i % 10}",  # 10 разных потоков
                event_type="StressTestEvent",
                data={"index": i, "payload": "x" * 100},  # ~100 байт payload
                version=i // 10  # Версии для каждого потока
            )
            
            start = time.perf_counter()
            await self.store.append_event(event)
            duration_ms = (time.perf_counter() - start) * 1000
            
            self.metrics['write_single'].add(duration_ms)
            
            if i % 100 == 0:
                print(f"  Progress: {i}/{iterations}", end='\r')
                
        # Форсируем flush для честного замера
        await self.store._flush_buffer()
        print(f"  ✅ Completed {iterations} single writes")
        
    async def test_batch_write_latency(self, batches: int = 100, batch_size: int = 100):
        """Тест латентности батчевой записи"""
        print(f"\n📦 Testing batch write latency ({batches} batches x {batch_size} events)...")
        
        for batch_num in range(batches):
            events = []
            stream_id = f"stress_test_batch_{batch_num}"
            
            # Создаем батч событий
            for i in range(batch_size):
                event = BaseEvent.create(
                    stream_id=stream_id,
                    event_type="BatchStressEvent",
                    data={"batch": batch_num, "index": i, "payload": "y" * 200},
                    version=i
                )
                events.append(event)
            
            # Измеряем время батчевой записи
            start = time.perf_counter()
            for event in events:
                await self.store.append_event(event)
            await self.store._flush_buffer()  # Форсируем запись
            duration_ms = (time.perf_counter() - start) * 1000
            
            self.metrics['write_batch'].add(duration_ms)
            self.metrics['flush_100'].add(duration_ms)  # Это же время flush для 100 событий
            
            if batch_num % 10 == 0:
                print(f"  Progress: {batch_num}/{batches}", end='\r')
                
        print(f"  ✅ Completed {batches} batch writes")
        
    async def test_read_latency(self, iterations: int = 100):
        """Тест латентности чтения 100 событий"""
        print(f"\n📖 Testing read latency for 100 events ({iterations} iterations)...")
        
        # Подготавливаем тестовые потоки
        print("  Preparing test streams...")
        for i in range(10):
            stream_id = f"stress_test_read_{i}"
            for j in range(100):
                event = BaseEvent.create(
                    stream_id=stream_id,
                    event_type="ReadTestEvent",
                    data={"index": j, "payload": "z" * 300},
                    version=j
                )
                await self.store.append_event(event)
        await self.store._flush_buffer()
        
        # Измеряем чтение
        for i in range(iterations):
            stream_id = f"stress_test_read_{i % 10}"
            
            start = time.perf_counter()
            events = await self.store.get_stream(stream_id)
            duration_ms = (time.perf_counter() - start) * 1000
            
            assert len(events) == 100, f"Expected 100 events, got {len(events)}"
            self.metrics['read_100'].add(duration_ms)
            
            if i % 20 == 0:
                print(f"  Progress: {i}/{iterations}", end='\r')
                
        print(f"  ✅ Completed {iterations} reads")
        
    async def test_concurrent_writes(self, workers: int = 50, events_per_worker: int = 100):
        """Тест конкурентной записи в разные потоки"""
        print(f"\n🔥 Testing concurrent writes ({workers} workers x {events_per_worker} events)...")
        
        async def worker(worker_id: int):
            stream_id = f"stress_test_concurrent_{worker_id}"
            latencies = []
            
            for i in range(events_per_worker):
                event = BaseEvent.create(
                    stream_id=stream_id,
                    event_type="ConcurrentEvent",
                    data={"worker": worker_id, "index": i},
                    version=i
                )
                
                start = time.perf_counter()
                await self.store.append_event(event)
                duration_ms = (time.perf_counter() - start) * 1000
                latencies.append(duration_ms)
                
                # Случайная задержка для имитации реальной нагрузки
                await asyncio.sleep(random.uniform(0.001, 0.01))
            
            return latencies
        
        # Запускаем всех воркеров параллельно
        start_time = time.perf_counter()
        tasks = [worker(i) for i in range(workers)]
        all_latencies = await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_time
        
        # Собираем все метрики
        for latencies in all_latencies:
            for latency in latencies:
                self.metrics['concurrent_write'].add(latency)
        
        total_events = workers * events_per_worker
        throughput = total_events / total_time
        print(f"  ✅ Completed {total_events} concurrent writes")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Throughput: {throughput:.0f} events/sec")
        
    async def test_version_conflicts(self, iterations: int = 100):
        """Тест обработки конфликтов версий"""
        print(f"\n⚔️ Testing version conflict handling ({iterations} iterations)...")
        
        conflicts_resolved = 0
        
        for i in range(iterations):
            stream_id = f"stress_test_conflict_{i % 5}"  # 5 потоков для конфликтов
            
            # Создаем два конкурирующих события с одной версией
            async def write_with_retry(version: int, data: dict) -> float:
                start = time.perf_counter()
                retry_count = 0
                
                while retry_count < 10:
                    try:
                        event = BaseEvent.create(
                            stream_id=stream_id,
                            event_type="ConflictEvent",
                            data=data,
                            version=version + retry_count
                        )
                        await self.store.append_event(event)
                        return (time.perf_counter() - start) * 1000
                    except EventStoreConcurrencyError:
                        retry_count += 1
                        await asyncio.sleep(0.001)
                
                raise Exception("Failed to resolve conflict after 10 retries")
            
            # Запускаем две конкурирующие записи
            base_version = (i // 5) * 2
            task1 = write_with_retry(base_version, {"writer": "A", "iteration": i})
            task2 = write_with_retry(base_version, {"writer": "B", "iteration": i})
            
            latencies = await asyncio.gather(task1, task2, return_exceptions=True)
            
            # Записываем метрики для успешных записей
            for latency in latencies:
                if isinstance(latency, float):
                    self.metrics['version_conflict'].add(latency)
                    conflicts_resolved += 1
            
            if i % 20 == 0:
                print(f"  Progress: {i}/{iterations}", end='\r')
        
        print(f"  ✅ Resolved {conflicts_resolved} version conflicts")
        
    async def test_extreme_load(self, duration_seconds: int = 30):
        """Экстремальная нагрузка - максимум конкурентных операций"""
        print(f"\n🌋 EXTREME LOAD TEST ({duration_seconds} seconds)...")
        print("  Simulating maximum concurrent load with mixed operations")
        
        stats = {
            'writes': 0,
            'reads': 0,
            'conflicts': 0,
            'errors': 0,
        }
        
        async def extreme_worker(worker_id: int, stop_event: asyncio.Event):
            stream_base = f"stress_test_extreme_{worker_id}"
            version_counters = {}
            
            while not stop_event.is_set():
                operation = random.choice(['write', 'write', 'write', 'read'])  # 75% writes
                
                try:
                    if operation == 'write':
                        stream_id = f"{stream_base}_{random.randint(0, 9)}"
                        version = version_counters.get(stream_id, 0)
                        
                        event = BaseEvent.create(
                            stream_id=stream_id,
                            event_type="ExtremeLoadEvent",
                            data={
                                "worker": worker_id,
                                "timestamp": datetime.now().isoformat(),
                                "random": random.random(),
                                "payload": "x" * random.randint(100, 1000)
                            },
                            version=version
                        )
                        
                        await self.store.append_event(event)
                        version_counters[stream_id] = version + 1
                        stats['writes'] += 1
                        
                    else:  # read
                        stream_id = f"{stream_base}_{random.randint(0, 9)}"
                        events = await self.store.get_stream(stream_id, from_version=0)
                        stats['reads'] += 1
                        
                except EventStoreConcurrencyError:
                    stats['conflicts'] += 1
                except Exception as e:
                    stats['errors'] += 1
                    if stats['errors'] < 10:  # Log first 10 errors
                        print(f"\n  Error in worker {worker_id}: {str(e)}")
                
                # Минимальная задержка для избежания 100% CPU
                await asyncio.sleep(0.0001)
        
        # Запускаем воркеров
        stop_event = asyncio.Event()
        worker_count = 100  # 100 параллельных воркеров
        
        print(f"  Starting {worker_count} workers...")
        workers = [extreme_worker(i, stop_event) for i in range(worker_count)]
        worker_tasks = [asyncio.create_task(w) for w in workers]
        
        # Мониторинг прогресса
        start_time = time.perf_counter()
        last_stats = dict(stats)
        
        while time.perf_counter() - start_time < duration_seconds:
            await asyncio.sleep(5)
            elapsed = time.perf_counter() - start_time
            
            # Вычисляем throughput за последние 5 секунд
            writes_delta = stats['writes'] - last_stats['writes']
            reads_delta = stats['reads'] - last_stats['reads']
            ops_per_sec = (writes_delta + reads_delta) / 5
            
            print(f"\n  [{elapsed:.0f}s] Writes: {stats['writes']}, "
                  f"Reads: {stats['reads']}, "
                  f"Conflicts: {stats['conflicts']}, "
                  f"Errors: {stats['errors']}, "
                  f"Throughput: {ops_per_sec:.0f} ops/sec")
            
            last_stats = dict(stats)
        
        # Останавливаем воркеров
        print("\n  Stopping workers...")
        stop_event.set()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        
        # Финальная статистика
        total_ops = stats['writes'] + stats['reads']
        total_time = time.perf_counter() - start_time
        avg_throughput = total_ops / total_time
        
        print(f"\n  ✅ EXTREME LOAD TEST COMPLETE")
        print(f"  Total operations: {total_ops:,}")
        print(f"  Average throughput: {avg_throughput:.0f} ops/sec")
        print(f"  Conflict rate: {stats['conflicts'] / stats['writes'] * 100:.2f}%")
        print(f"  Error rate: {stats['errors'] / total_ops * 100:.4f}%")
        
    async def run_all_tests(self):
        """Запуск всех тестов"""
        print("\n" + "="*60)
        print("PostgreSQL Event Store - STRESS TEST SUITE")
        print("="*60)
        
        await self.setup()
        
        try:
            # Основные тесты производительности
            await self.test_single_write_latency(iterations=1000)
            await self.test_batch_write_latency(batches=100)
            await self.test_read_latency(iterations=100)
            await self.test_concurrent_writes(workers=50)
            await self.test_version_conflicts(iterations=100)
            
            # Экстремальная нагрузка
            print("\n" + "-"*60)
            await self.test_extreme_load(duration_seconds=30)
            
            # Отчет о производительности
            print("\n" + "="*60)
            print("PERFORMANCE REPORT")
            print("="*60)
            
            self.metrics['write_single'].print_report(target_ms=5)
            self.metrics['write_batch'].print_report(target_ms=5)
            self.metrics['read_100'].print_report(target_ms=10)
            self.metrics['flush_100'].print_report(target_ms=50)
            self.metrics['concurrent_write'].print_report(target_ms=5)
            self.metrics['version_conflict'].print_report()
            
            # Метрики Event Store
            print("\n" + "-"*60)
            print("Event Store Metrics:")
            store_metrics = self.store.get_metrics()
            for key, value in store_metrics.items():
                if isinstance(value, dict):
                    print(f"  {key}:")
                    for k, v in value.items():
                        print(f"    {k}: {v}")
                else:
                    print(f"  {key}: {value}")
            
        finally:
            await self.cleanup()
            
        print("\n✅ All stress tests completed!")


async def main():
    """Точка входа"""
    # Проверяем наличие PostgreSQL
    if not os.getenv("POSTGRES_DSN"):
        print("❌ Error: POSTGRES_DSN environment variable not set")
        print("Please set: export POSTGRES_DSN='postgresql://user:pass@localhost/db'")
        return
        
    test_suite = EventStoreStressTest()
    await test_suite.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())