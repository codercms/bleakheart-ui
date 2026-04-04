from bleakheart_ui.features.main.profile_id_utils import profile_id_from_name, unique_profile_id


def test_profile_id_from_name_english_and_special_chars():
    assert profile_id_from_name(" John--D'oe!!! ") == "john_d_oe"


def test_profile_id_from_name_russian_transliteration():
    assert profile_id_from_name("Иван Петров") == "ivan_petrov"


def test_profile_id_from_name_japanese_fallback_tokens():
    assert profile_id_from_name("山田 太郎") == "u5c71_u7530_u592a_u90ce"


def test_profile_id_from_name_korean_fallback_tokens():
    assert profile_id_from_name("홍 길동") == "ud64d_uae38_ub3d9"


def test_profile_id_from_name_chinese_fallback_tokens():
    assert profile_id_from_name("张 伟") == "u5f20_u4f1f"


def test_unique_profile_id_suffixes_case_insensitive():
    existing = ["john_doe", "JOHN_DOE_2"]
    assert unique_profile_id("john_doe", existing) == "john_doe_3"
