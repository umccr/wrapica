"""Tests for wrapica.utils pure functions (miscell.py and __init__.py)."""
import pytest
from pathlib import Path
from uuid import UUID

from wrapica.utils.miscell import (
    is_uuid_format,
    is_uri_format,
    camel_to_snake_case,
    snake_to_camel_case,
    to_lower_camel_case,
    sanitise_dict_keys,
    is_str_type_representation,
    nextflow_parameter_to_str,
    coerce_to_uuid4_obj,
)
from wrapica.utils import fill_placeholder_path, parse_s3_uri


class TestIsUuidFormat:
    """Tests for is_uuid_format."""

    def test_valid_uuid4_string(self):
        assert is_uuid_format("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d") is True

    def test_uuid_object(self):
        uuid_obj = UUID("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
        assert is_uuid_format(uuid_obj) is True

    def test_malformed_uuid(self):
        assert is_uuid_format("a1b2c3d4-e5f6-4a7b-8c9d") is False

    def test_non_uuid_string(self):
        assert is_uuid_format("not-a-uuid-at-all") is False


class TestIsUriFormat:
    """Tests for is_uri_format."""

    def test_valid_uri(self):
        assert is_uri_format("https://example.com/path") is True

    def test_missing_scheme(self):
        assert is_uri_format("example.com/path") is False

    def test_missing_netloc(self):
        assert is_uri_format("https://") is False


class TestCamelToSnakeCase:
    """Tests for camel_to_snake_case."""

    def test_single_word(self):
        assert camel_to_snake_case("word") == "word"

    def test_camel_case(self):
        assert camel_to_snake_case("camelCase") == "camel_case"

    def test_acronym_adjacent(self):
        assert camel_to_snake_case("HTMLParser") == "h_t_m_l_parser"


class TestSnakeToCamelCase:
    """Tests for snake_to_camel_case."""

    def test_single_word(self):
        assert snake_to_camel_case("word") == "Word"

    def test_multi_word(self):
        assert snake_to_camel_case("snake_case") == "SnakeCase"


class TestToLowerCamelCase:
    """Tests for to_lower_camel_case."""

    def test_single_word(self):
        result = to_lower_camel_case("word")
        assert result[0].islower()
        assert result == "word"

    def test_multi_word(self):
        result = to_lower_camel_case("snake_case")
        assert result[0].islower()
        assert result == "snakeCase"


class TestSanitiseDictKeys:
    """Tests for sanitise_dict_keys."""

    def test_converts_keys_and_preserves_values(self):
        input_dict = {
            "firstName": "Alice",
            "lastName": "Smith",
            "emailAddress": 42,
        }
        result = sanitise_dict_keys(input_dict)
        assert result == {
            "first_name": "Alice",
            "last_name": "Smith",
            "email_address": 42,
        }
        assert len(result) == len(input_dict)


class TestIsStrTypeRepresentation:
    """Tests for is_str_type_representation."""

    def test_convertible_pair(self):
        assert is_str_type_representation("123", int) is True

    def test_non_convertible_pair(self):
        assert is_str_type_representation("abc", int) is False


class TestNextflowParameterToStr:
    """Tests for nextflow_parameter_to_str."""

    def test_true(self):
        assert nextflow_parameter_to_str(True) == "true"

    def test_false(self):
        assert nextflow_parameter_to_str(False) == "false"

    def test_int(self):
        assert nextflow_parameter_to_str(42) == "42"

    def test_float(self):
        assert nextflow_parameter_to_str(3.14) == "3.14"


class TestCoerceToUuid4Obj:
    """Tests for coerce_to_uuid4_obj."""

    def test_valid_uuid_string(self):
        uuid_str = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
        result = coerce_to_uuid4_obj(uuid_str)
        assert isinstance(result, UUID)
        assert str(result) == uuid_str

    def test_existing_uuid_object(self):
        uuid_obj = UUID("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
        result = coerce_to_uuid4_obj(uuid_obj)
        assert result is uuid_obj

    def test_invalid_type_raises_typeerror(self):
        with pytest.raises(TypeError):
            coerce_to_uuid4_obj(12345)


class TestFillPlaceholderPath:
    """Tests for fill_placeholder_path."""

    def test_matching_regex(self):
        output_path = Path("/data/output/sample_001/results")
        placeholder_dict = {
            r"/data/output/sample_(\d+)/(.*)": r"/replaced/\1/\2"
        }
        result = fill_placeholder_path(output_path, placeholder_dict)
        assert result == Path("/replaced/001/results")

    def test_non_matching_regex(self):
        output_path = Path("/data/output/sample_001/results")
        placeholder_dict = {
            r"/no/match/here": "/replaced"
        }
        result = fill_placeholder_path(output_path, placeholder_dict)
        assert result == output_path


class TestParseS3Uri:
    """Tests for parse_s3_uri."""

    def test_valid_s3_uri(self):
        bucket, key = parse_s3_uri("s3://my-bucket/path/to/key.txt")
        assert bucket == "my-bucket"
        assert key == "path/to/key.txt"


# --- Property-Based Tests ---

from hypothesis import given, settings
from hypothesis import strategies as st

# Custom strategy: valid snake_case identifiers where each segment has letters
# followed optionally by digits (avoids digit-before-letter which .title() treats
# as a word boundary, breaking the round-trip).
snake_case_strings = st.from_regex(r"[a-z]+[0-9]*(_[a-z]+[0-9]*)*", fullmatch=True)


# Feature: test-suite, Property 1: Case conversion round-trip
class TestCaseConversionProperty:
    """Property-based tests for case conversion functions."""

    @given(s=snake_case_strings)
    @settings(max_examples=100)
    def test_snake_to_camel_round_trip(self, s: str):
        """
        camel_to_snake_case(snake_to_camel_case(s)) == s for all valid snake_case strings.

        **Validates: Requirements 2.3, 2.4**
        """
        assert camel_to_snake_case(snake_to_camel_case(s)) == s
