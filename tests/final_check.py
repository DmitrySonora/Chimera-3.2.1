#!/usr/bin/env python3
"""
Скрипт финальной проверки готовности системы Химера 2.0
Проверяет работоспособность всех компонентов после Pydantic интеграции
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime


def print_section(title: str):
    """Красивый вывод заголовка секции"""
    print(f"\n{'='*60}")
    print(f"🔍 {title}")
    print(f"{'='*60}")


def print_ok(message: str):
    """Вывод успешного результата"""
    print(f"✅ {message}")


def print_error(message: str):
    """Вывод ошибки"""
    print(f"❌ {message}")


def check_pydantic_models():
    """Проверка работоспособности всех Pydantic моделей"""
    print_section("Проверка Pydantic моделей")
    
    errors = []
    
    # 1. Проверка ActorMessage
    try:
        from actors.messages import ActorMessage, MESSAGE_TYPES
        msg = ActorMessage.create(
            sender_id="test",
            message_type=MESSAGE_TYPES['PING'],
            payload={"test": "data"}
        )
        assert msg.sender_id == "test"
        assert msg.message_type == MESSAGE_TYPES['PING']
        assert msg['sender_id'] == "test"  # Обратная совместимость
        print_ok("ActorMessage - создание и валидация работают")
    except Exception as e:
        errors.append(f"ActorMessage: {str(e)}")
        print_error(f"ActorMessage: {str(e)}")
    
    # 2. Проверка BaseEvent
    try:
        from actors.events import BaseEvent
        event = BaseEvent.create(
            stream_id="test_stream",
            event_type="TestEvent",
            data={"key": "value"},
            version=0
        )
        assert event.stream_id == "test_stream"
        assert event.version == 0
        # Проверка иммутабельности
        try:
            event.version = 1
            errors.append("BaseEvent: не иммутабельный!")
        except Exception:
            pass  # Ожидаемое поведение
        print_ok("BaseEvent - создание и иммутабельность работают")
    except Exception as e:
        errors.append(f"BaseEvent: {str(e)}")
        print_error(f"BaseEvent: {str(e)}")
    
    # 3. Проверка UserSession
    try:
        from actors.user_session_actor import UserSession
        session = UserSession(user_id="test_user")
        session.current_mode = "expert"
        session.mode_confidence = 0.8
        session.mode_history = ["talk", "expert", "expert"]
        assert session.current_mode == "expert"
        # Проверка валидации
        try:
            session.mode_confidence = 1.5  # Должна быть ошибка
            errors.append("UserSession: нет валидации confidence")
        except Exception:
            pass  # Ожидаемое поведение
        print_ok("UserSession - создание и валидация работают")
    except Exception as e:
        errors.append(f"UserSession: {str(e)}")
        print_error(f"UserSession: {str(e)}")
    
    # 4. Проверка моделей структурированных ответов
    try:
        from models.structured_responses import (
            BaseResponse, TalkResponse, ExpertResponse, CreativeResponse,
            parse_response
        )
        
        # Базовая модель
        base = BaseResponse(response="Тестовый ответ")
        assert base.response == "Тестовый ответ"
        
        # Talk модель
        talk = TalkResponse(
            response="Привет!",
            emotional_tone="дружелюбный",
            engagement_level=0.7
        )
        assert talk.engagement_level == 0.7
        
        # Expert модель
        expert = ExpertResponse(
            response="Ответ эксперта",
            confidence=0.9,
            sources=["источник 1", "источник 2"]
        )
        assert len(expert.sources) == 2
        
        # Creative модель
        creative = CreativeResponse(
            response="Творческий текст",
            style_markers=["метафора"],
            metaphors=["жизнь как река"]
        )
        assert "метафора" in creative.style_markers
        
        # Парсинг из JSON
        parsed = parse_response('{"response": "Тест"}', mode='base')
        assert parsed.response == "Тест"
        
        print_ok("Модели структурированных ответов - все 4 режима работают")
    except Exception as e:
        errors.append(f"Structured responses: {str(e)}")
        print_error(f"Structured responses: {str(e)}")
    
    return len(errors) == 0, errors


def check_mode_parameters():
    """Проверка наличия режимных параметров"""
    print_section("Проверка режимных параметров генерации")
    
    try:
        from config.prompts import MODE_GENERATION_PARAMS, GENERATION_PARAMS_LOG_CONFIG
        
        # Проверка наличия всех режимов
        required_modes = ['base', 'talk', 'expert', 'creative']
        for mode in required_modes:
            if mode not in MODE_GENERATION_PARAMS:
                print_error(f"Отсутствуют параметры для режима '{mode}'")
                return False, [f"Missing mode: {mode}"]
            
            params = MODE_GENERATION_PARAMS[mode]
            print(f"\nРежим '{mode}':")
            print(f"  • temperature: {params.get('temperature', 'не задана')}")
            print(f"  • max_tokens: {params.get('max_tokens', 'не задан')}")
        
        # Проверка конфигурации логирования
        print("\nКонфигурация логирования:")
        print(f"  • log_parameters_usage: {GENERATION_PARAMS_LOG_CONFIG.get('log_parameters_usage', False)}")
        print(f"  • log_response_length: {GENERATION_PARAMS_LOG_CONFIG.get('log_response_length', False)}")
        print(f"  • debug_mode_selection: {GENERATION_PARAMS_LOG_CONFIG.get('debug_mode_selection', False)}")
        
        print_ok("Все режимные параметры на месте")
        return True, []
        
    except Exception as e:
        print_error(f"Ошибка при проверке параметров: {str(e)}")
        return False, [str(e)]


async def check_basic_integration():
    """Проверка базовой интеграции компонентов"""
    print_section("Проверка базовой интеграции")
    
    errors = []
    
    try:
        from actors.actor_system import ActorSystem
        from actors.events import EventStore
        from actors.user_session_actor import UserSessionActor
        
        # Создаем систему
        system = ActorSystem("test")
        print_ok("ActorSystem создан")
        
        # Создаем Event Store
        event_store = EventStore()
        system.set_event_store(event_store)
        print_ok("EventStore подключен")
        
        # Создаем и регистрируем актор
        actor = UserSessionActor()
        await system.register_actor(actor)
        print_ok("UserSessionActor зарегистрирован")
        
        # Проверяем, что актор получил ссылку на систему
        assert actor.get_actor_system() is not None
        print_ok("Актор имеет ссылку на ActorSystem")
        
        # Базовая проверка определения режима
        session = actor._sessions.get("test") or await actor._get_or_create_session("test", None)
        mode, confidence = actor._determine_generation_mode("Объясни квантовую физику", session)
        assert mode == "expert"
        print_ok(f"Определение режима работает: '{mode}' с уверенностью {confidence:.2f}")
        
        return True, []
        
    except Exception as e:
        errors.append(str(e))
        print_error(f"Ошибка интеграции: {str(e)}")
        return False, errors


async def main():
    """Основная функция проверки"""
    print("\n" + "="*60)
    print("🐲 ФИНАЛЬНАЯ ПРОВЕРКА СИСТЕМЫ ХИМЕРА 2.0")
    print("="*60)
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    all_ok = True
    all_errors = []
    
    # 1. Проверка Pydantic моделей
    ok, errors = check_pydantic_models()
    all_ok &= ok
    all_errors.extend(errors)
    
    # 2. Проверка режимных параметров
    ok, errors = check_mode_parameters()
    all_ok &= ok
    all_errors.extend(errors)
    
    # 3. Проверка базовой интеграции
    ok, errors = await check_basic_integration()
    all_ok &= ok
    all_errors.extend(errors)
    
    # Итоговый отчет
    print_section("ИТОГОВЫЙ ОТЧЕТ")
    
    if all_ok:
        print("\n🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО! 🎉")
        print("\nСистема готова к работе:")
        print("  ✅ Pydantic модели работают корректно")
        print("  ✅ Режимные параметры настроены")
        print("  ✅ Базовая интеграция функционирует")
        print("\n🐲 Химера 2.0 готова к следующему этапу разработки!")
    else:
        print(f"\n⚠️ ОБНАРУЖЕНЫ ПРОБЛЕМЫ ({len(all_errors)} ошибок):")
        for i, error in enumerate(all_errors, 1):
            print(f"  {i}. {error}")
        print("\n❗ Необходимо исправить ошибки перед продолжением")
    
    print("\n" + "="*60)
    return all_ok


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)