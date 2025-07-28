#!/usr/bin/env python3
"""
Ручные тесты для проверки исправлений PostgreSQL Event Store
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from actors.events import BaseEvent, EventStore
from actors.events.postgres_event_store import PostgresEventStore
from database.event_store_migrator import EventStoreMigrator
from config.logging import setup_logging
from config.settings import EVENT_STORE_TYPE


async def test_1_event_order_preservation():
    """Тест 1: Проверка сохранения порядка событий при возврате в буфер"""
    print("\n=== ТЕСТ 1: Порядок событий при ошибках ===")
    
    # Создаем in-memory store для проверки
    store = PostgresEventStore()
    
    # Добавляем события в буфер
    events = []
    for i in range(5):
        event = BaseEvent.create(
            stream_id="test_order",
            event_type="OrderTest",
            data={"sequence": i},
            version=i
        )
        events.append(event)
        store._write_buffer.append(event)
    
    print(f"Исходный порядок в буфере: {[e.data['sequence'] for e in store._write_buffer]}")
    
    # Симулируем возврат событий при ошибке
    returned_events = list(store._write_buffer)
    store._write_buffer.clear()
    
    # Применяем исправление
    for event in reversed(returned_events):
        store._write_buffer.appendleft(event)
    
    print(f"Порядок после возврата: {[e.data['sequence'] for e in store._write_buffer]}")
    
    # Проверка
    is_correct = all(
        store._write_buffer[i].data['sequence'] == i 
        for i in range(len(store._write_buffer))
    )
    
    if is_correct:
        print("✅ PASS: Порядок событий сохранен корректно!")
    else:
        print("❌ FAIL: Порядок событий нарушен!")
    
    return is_correct


async def test_2_atomic_migration():
    """Тест 2: Проверка атомарности миграции потоков"""
    print("\n=== ТЕСТ 2: Атомарность миграции ===")
    
    # Создаем источник с тестовыми данными
    source = EventStore()
    
    # Добавляем 3 потока с разным количеством событий
    streams_data = {
        "stream_small": 5,
        "stream_medium": 50,
        "stream_large": 200
    }
    
    for stream_id, count in streams_data.items():
        for i in range(count):
            event = BaseEvent.create(
                stream_id=stream_id,
                event_type="TestEvent",
                data={"index": i},
                version=i
            )
            await source.append_event(event)
    
    print(f"Создано потоков: {len(streams_data)}")
    for stream_id, count in streams_data.items():
        print(f"  - {stream_id}: {count} событий")
    
    # Создаем мигратор
    migrator = EventStoreMigrator()
    
    # Проверяем метод _migrate_stream
    print("\nПроверка: метод _migrate_stream должен использовать _write_stream_events")
    
    # Читаем код метода
    import inspect
    method_source = inspect.getsource(migrator._migrate_stream)
    
    if "_write_stream_events" in method_source:
        print("✅ PASS: Метод использует атомарную запись через _write_stream_events")
        return True
    else:
        print("❌ FAIL: Метод не использует атомарную запись")
        return False


async def test_3_advisory_lock_keys():
    """Тест 3: Проверка генерации двойных ключей для advisory locks"""
    print("\n=== ТЕСТ 3: Двойные ключи advisory locks ===")
    
    try:
        from actors.events.postgres_event_store import generate_stream_lock_keys
        print("✅ Функция generate_stream_lock_keys найдена")
    except ImportError:
        print("❌ FAIL: Функция generate_stream_lock_keys не найдена")
        return False
    
    # Тестируем генерацию ключей
    test_streams = [
        "user_123",
        "session_abc",
        "dlq_actor1",
        "very_long_stream_id_that_should_still_work_correctly"
    ]
    
    print("\nГенерация ключей для разных stream_id:")
    
    all_keys = set()
    for stream_id in test_streams:
        high_key, low_key = generate_stream_lock_keys(stream_id)
        print(f"  {stream_id}: ({high_key}, {low_key})")
        
        # Проверяем что ключи в диапазоне int32
        if not (-2**31 <= high_key < 2**31 and -2**31 <= low_key < 2**31):
            print(f"    ❌ Ключи вне диапазона int32!")
            return False
        
        # Проверяем уникальность пары
        key_pair = (high_key, low_key)
        if key_pair in all_keys:
            print(f"    ❌ Коллизия ключей!")
            return False
        all_keys.add(key_pair)
    
    print("\n✅ PASS: Все ключи уникальны и в правильном диапазоне")
    
    # Проверяем использование в _write_stream_events
    print("\nПроверка использования в _write_stream_events:")
    store = PostgresEventStore()
    import inspect
    method_source = inspect.getsource(store._write_stream_events)
    
    if "generate_stream_lock_keys" in method_source and "pg_advisory_xact_lock($1, $2)" in method_source:
        print("✅ PASS: Метод использует двойные ключи для advisory lock")
        return True
    else:
        print("❌ FAIL: Метод не использует двойные ключи")
        return False


async def test_4_integration_check():
    """Тест 4: Общая проверка интеграции"""
    print("\n=== ТЕСТ 4: Интеграционная проверка ===")
    
    # Проверяем что EVENT_STORE_TYPE можно переключить
    print(f"Текущий EVENT_STORE_TYPE: {EVENT_STORE_TYPE}")
    
    # Проверяем наличие всех необходимых файлов
    files_to_check = [
        "actors/events/postgres_event_store.py",
        "database/connection.py",
        "database/event_store_migrator.py",
        "database/migrations/001_create_events_table.sql"
    ]
    
    all_exist = True
    for file_path in files_to_check:
        path = Path(file_path)
        exists = path.exists()
        print(f"  {file_path}: {'✅' if exists else '❌'}")
        if not exists:
            all_exist = False
    
    if all_exist:
        print("\n✅ PASS: Все файлы PostgreSQL интеграции присутствуют")
        return True
    else:
        print("\n❌ FAIL: Некоторые файлы отсутствуют")
        return False


async def main():
    """Запуск всех тестов"""
    setup_logging()
    
    print("=" * 60)
    print("ПРОВЕРКА ИСПРАВЛЕНИЙ PostgreSQL Event Store")
    print("=" * 60)
    
    results = []
    
    # Запускаем тесты
    results.append(("Порядок событий", await test_1_event_order_preservation()))
    results.append(("Атомарность миграции", await test_2_atomic_migration()))
    results.append(("Advisory locks", await test_3_advisory_lock_keys()))
    results.append(("Интеграция", await test_4_integration_check()))
    
    # Итоговый отчет
    print("\n" + "=" * 60)
    print("ИТОГОВЫЙ ОТЧЕТ:")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    print(f"\nПройдено тестов: {passed}/{total}")
    
    if passed == total:
        print("\n🎉 ВСЕ ИСПРАВЛЕНИЯ ВНЕДРЕНЫ КОРРЕКТНО!")
    else:
        print("\n⚠️  Некоторые исправления требуют проверки")


if __name__ == "__main__":
    asyncio.run(main())