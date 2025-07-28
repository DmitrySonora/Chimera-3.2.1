import pytest

from models.response_schemas import (
    GENERATION_MODES, 
    RESPONSE_SCHEMAS, 
    get_schema, 
    validate_field
)
from actors.generation_actor import GenerationActor
from config.logging import get_logger

# Тестируем совместимость со старыми схемами
from models.structured_responses import RESPONSE_MODELS


class TestResponseSchemas:
    """Тесты для схем валидации структурированных ответов"""
    
    def test_schemas_import(self):
        """Проверка, что схемы импортируются корректно"""
        assert GENERATION_MODES is not None
        assert RESPONSE_SCHEMAS is not None
        assert len(GENERATION_MODES) == 4
        assert len(RESPONSE_SCHEMAS) == 4
    
    def test_all_modes_have_schemas(self):
        """Проверка, что для каждого режима есть схема"""
        for mode_key, mode_value in GENERATION_MODES.items():
            assert mode_value in RESPONSE_SCHEMAS
            schema = RESPONSE_SCHEMAS[mode_value]
            assert 'required' in schema
            assert 'optional' in schema
            assert 'validators' in schema
    
    def test_base_schema_validation(self):
        """Проверка валидации базовой схемы"""
        schema = get_schema('base')
        assert schema is not None
        
        # Валидный ответ
        valid_response = {"response": "Привет, я Химера!"}
        validator = schema['validators']['response']
        assert validate_field('response', valid_response['response'], validator) is True
        
        # Невалидные ответы
        assert validate_field('response', "", validator) is False  # Пустая строка
        assert validate_field('response', None, validator) is False  # None
        assert validate_field('response', 123, validator) is False  # Не строка
    
    def test_talk_schema_validation(self):
        """Проверка валидации схемы talk"""
        schema = get_schema('talk')
        
        # Проверка engagement_level
        validator = schema['validators']['engagement_level']
        assert validate_field('engagement_level', 0.5, validator) is True
        assert validate_field('engagement_level', 0, validator) is True
        assert validate_field('engagement_level', 1, validator) is True
        assert validate_field('engagement_level', 1.5, validator) is False  # > 1
        assert validate_field('engagement_level', -0.1, validator) is False  # < 0
        assert validate_field('engagement_level', "high", validator) is False  # Не число
    
    def test_expert_schema_validation(self):
        """Проверка валидации схемы expert"""
        schema = get_schema('expert')
        
        # Проверка sources
        validator = schema['validators']['sources']
        assert validate_field('sources', ["источник 1", "источник 2"], validator) is True
        assert validate_field('sources', [], validator) is True  # Пустой список OK
        assert validate_field('sources', [1, 2, 3], validator) is False  # Не строки
        assert validate_field('sources', "источник", validator) is False  # Не список
    
    def test_creative_schema_validation(self):
        """Проверка валидации схемы creative"""
        schema = get_schema('creative')
        
        # Проверка style_markers
        validator = schema['validators']['style_markers']
        assert validate_field('style_markers', ["ирония", "метафора"], validator) is True
        assert validate_field('style_markers', [], validator) is True
        assert validate_field('style_markers', None, validator) is False
    
    def test_pydantic_models_compatibility(self):
        """Проверка, что Pydantic модели совместимы со старыми схемами"""
        # Проверяем наличие моделей для всех режимов
        for mode in GENERATION_MODES.values():
            assert mode in RESPONSE_MODELS
            model = RESPONSE_MODELS[mode]
            
            # Проверяем, что у модели есть обязательное поле response
            assert 'response' in model.model_fields


@pytest.mark.asyncio
class TestGenerationActorValidation:
    """Тесты валидации в GenerationActor"""
    
    async def test_extract_json_with_full_dict(self):
        """Проверка, что _extract_from_json может возвращать полный словарь"""
        actor = GenerationActor()
        
        # Инициализируем минимально необходимые атрибуты
        from config.logging import get_logger
        actor.logger = get_logger("test")
        actor._event_version_manager = None
        
        # Тест с return_full_dict=True
        json_response = '{"response": "Тестовый ответ", "confidence": 0.95}'
        result = await actor._extract_from_json(json_response, "test_user", return_full_dict=True)
        
        assert isinstance(result, dict)
        assert result['response'] == "Тестовый ответ"
        assert result['confidence'] == 0.95
        
        # Тест с return_full_dict=False (по умолчанию)
        result = await actor._extract_from_json(json_response, "test_user", return_full_dict=False)
        assert isinstance(result, str)
        assert result == "Тестовый ответ"
    
    async def test_validate_structured_response(self):
        """Проверка метода валидации структурированных ответов"""
        actor = GenerationActor()
        actor.logger = get_logger("test")
        
        # Валидный base ответ
        valid_base = {"response": "Привет!"}
        is_valid, errors = await actor._validate_structured_response(valid_base, 'base')
        assert is_valid is True
        assert len(errors) == 0
        
        # Невалидный base ответ (нет обязательного поля)
        invalid_base = {"text": "Привет!"}
        is_valid, errors = await actor._validate_structured_response(invalid_base, 'base')
        assert is_valid is False
        # Проверяем новый формат ошибки Pydantic
        assert any("response" in error and "Field required" in error for error in errors)
        
        # Валидный talk ответ
        valid_talk = {
            "response": "Как интересно!",
            "emotional_tone": "восторженный",
            "engagement_level": 0.8
        }
        is_valid, errors = await actor._validate_structured_response(valid_talk, 'talk')
        assert is_valid is True
        
        # Невалидный talk ответ (неправильный тип engagement_level)
        invalid_talk = {
            "response": "Ответ",
            "engagement_level": "высокий"  # Должно быть число
        }
        is_valid, errors = await actor._validate_structured_response(invalid_talk, 'talk')
        assert is_valid is False
        assert any("engagement_level" in error for error in errors)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])