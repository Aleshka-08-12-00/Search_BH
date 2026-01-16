import os
import re
import json
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd
from rapidfuzz import fuzz, process

from app.helpers import (
    english_to_russian_transliteration_dict,
    russian_to_english_transliteration_dict,
    top_product_list,
)

# ---------------------------------------------------------
# Константы для бустов
# ---------------------------------------------------------

WORD_MATCH_BOOST = 5       # +5 за каждое совпадающее слово
NUMBER_MATCH_BOOST = 20    # +20 за каждое совпадающее число
PHRASE_MATCH_BOOST = 15    # +15 если весь запрос как подстрока в названии
WORD_MISSING_PENALTY = 3   # -3 за каждое слово из запроса, которого нет в названии
NUMBER_MISSING_PENALTY = 10  # -10 за каждое число из запроса, которого нет в названии

STOP_WORDS = {
    "для",
    "и",
    "или",
    "с",
    "со",
    "без",
    "на",
    "в",
    "по",
    "от",
    "до",
    "a",
    "an",
    "the",
    "for",
    "with",
    "of",
    "and",
    "or",
}

# Путь к файлу синонимов (можно переопределить через переменную окружения)
SYNONYMS_PATH = Path(os.getenv("SEARCH_SYNONYMS_PATH", "synonyms.json"))

# Глобальный кеш синонимов + время последней модификации файла
_synonyms_cache: Dict[str, List[str]] = {}
_synonyms_mtime: Optional[float] = None


# ---------------------------------------------------------
# Вспомогательные функции: язык / раскладка / транслит
# ---------------------------------------------------------

def detect_language(text: str) -> str:
    """Грубое определение языка: считаем кириллицу/латиницу."""
    count_cyrillic = sum(1 for char in text if "\u0400" <= char <= "\u04FF")
    count_latin = sum(1 for char in text if "a" <= char <= "z" or "A" <= char <= "Z")
    return "ru" if count_cyrillic >= count_latin else "en"


def convert_layout(text: str) -> str:
    """
    Конвертация раскладки (русская клавиатура <-> английская),
    не меняя сами символы (это не транслит).
    """
    russian_layout = "йцукенгшщзхъфывапролджэячсмитьбюЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ"
    english_layout = "qwertyuiop[]asdfghjkl;'zxcvbnm,.QWERTYUIOP{}ASDFGHJKL:\"ZXCVBNM<>"

    if not text:
        return text

    language = detect_language(text)

    if language == "ru":
        translation_table = str.maketrans(russian_layout, english_layout)
        result_chars = []
        for ch in text:
            if "\u0400" <= ch <= "\u04FF":
                result_chars.append(ch.translate(translation_table))
            else:
                result_chars.append(ch)
        return "".join(result_chars)

    translation_table = str.maketrans(english_layout, russian_layout)
    return text.translate(translation_table)


def custom_transliterate(text: str, transliteration_dict: dict) -> str:
    """
    Транслит с поддержкой многосимвольных ключей словаря:
    берём максимально возможную подстроку от текущей позиции.
    """
    if not text:
        return text

    result = []
    i = 0
    max_key_len = max(map(len, transliteration_dict.keys())) if transliteration_dict else 0

    while i < len(text):
        match_found = False
        max_len_here = min(len(text) - i, max_key_len)
        for length in range(max_len_here, 0, -1):
            chunk = text[i : i + length]
            if chunk in transliteration_dict:
                result.append(transliteration_dict[chunk])
                i += length
                match_found = True
                break
        if not match_found:
            result.append(transliteration_dict.get(text[i], text[i]))
            i += 1

    return "".join(result)


def transliterate(text: Optional[str]) -> Optional[str]:
    """Авто-определение направления транслита (ru->en или en->ru)."""
    if not isinstance(text, str) or not text:
        return None

    lowered = text.lower()
    language = detect_language(lowered)

    if language == "ru":
        return custom_transliterate(lowered, russian_to_english_transliteration_dict)
    elif language == "en":
        return custom_transliterate(lowered, english_to_russian_transliteration_dict)
    return None


# ---------------------------------------------------------
# Синонимы: авто-перезагрузка при изменении файла
# ---------------------------------------------------------

def _load_synonyms() -> Dict[str, List[str]]:
    """
    Загружаем словарь синонимов из JSON-файла с кешированием по времени
    модификации. Формат файла:

    {
      "matrix": ["socolor", "super sync"],
      "бабки": "деньги"
    }

    Можно править файл на лету: при изменении mtime кеш будет обновлён.
    """
    global _synonyms_cache, _synonyms_mtime

    path = SYNONYMS_PATH

    # файла нет — очищаем кеш и возвращаем пустой словарь
    if not path.is_file():
        _synonyms_cache = {}
        _synonyms_mtime = None
        return _synonyms_cache

    try:
        mtime = path.stat().st_mtime
    except Exception:
        # если не получилось прочитать mtime, оставляем старый кеш
        return _synonyms_cache

    # файл не менялся — возвращаем кеш
    if _synonyms_mtime is not None and mtime == _synonyms_mtime:
        return _synonyms_cache

    # файл изменился или загружаем первый раз
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        # если JSON битый — не роняем поиск, остаёмся на старом кеше
        return _synonyms_cache

    synonyms: Dict[str, List[str]] = {}
    for key, value in raw.items():
        key_l = str(key).lower().strip()
        if not key_l:
            continue

        if isinstance(value, str):
            synonyms[key_l] = [value.lower().strip()]
        elif isinstance(value, (list, tuple, set)):
            synonyms[key_l] = [
                str(v).lower().strip() for v in value if str(v).strip()
            ]

    _synonyms_cache = synonyms
    _synonyms_mtime = mtime
    return _synonyms_cache



def _match_token_case(token: str, replacement: str) -> str:
    if token.isupper():
        return replacement.upper()
    if token[:1].isupper() and token[1:].islower():
        return replacement.capitalize()
    return replacement


def replace_synonyms_in_query(query: str) -> str:
    """
    Заменяем слова на синонимы сразу при запросе.

    Пример synonyms.json:
    {
      "matrix": ["socolor", "super sync"],
      "shampoo": ["шампунь"]
    }

    "A Curl Can Dream Shampoo 300 мл" -> "A Curl Can Dream Шампунь 300 мл"
    """
    if not isinstance(query, str):
        return ""
    query = query.strip()
    if not query:
        return ""

    synonyms = _load_synonyms()
    if not synonyms:
        return query

    tokens = re.split(r"\s+", query)
    result_tokens: List[str] = []

    for token in tokens:
        t_low = token.lower()
        if t_low in synonyms and synonyms[t_low]:
            replacement = synonyms[t_low][0]
            result_tokens.append(_match_token_case(token, replacement))
        else:
            result_tokens.append(token)

    return " ".join(result_tokens)


def normalize_query(raw_query: str) -> str:
    """
    Нормализация текста запроса:
    - trim
    - убираем лишнюю пунктуацию (оставляем только буквы/цифры/пробелы/точку)
      SoColor: 6RC -> SoColor 6RC
    - схлопываем пробелы

    СИНОНИМЫ здесь НЕ применяем — они обрабатываются отдельно в search_dataframe.
    """
    if not isinstance(raw_query, str):
        return ""
    q = raw_query.strip()
    if not q:
        return ""

    # оставляем только \w, пробелы и '.' (для оттенков 10.23 и т.п.)
    q = re.sub(r"[^\w\s\.]+", " ", q)
    q = re.sub(r"\s+", " ", q)

    return q


# ---------------------------------------------------------
# Базовые поисковые функции (RapidFuzz + простое совпадение)
# ---------------------------------------------------------

def search_with_fuzzy(
    search_query: str,
    dataframe: pd.DataFrame,
    column_name: str = "name",
    threshold: int = 40,   # было 65, делаем мягче
) -> pd.DataFrame:
    """
    Fuzzy-поиск по RapidFuzz с учётом многословных запросов.

    Используем fuzz.token_set_ratio:
    - игнорирует порядок слов
    - устойчив к "лишним" словам

    И запрос, и значения колонки приводим к lower(),
    чтобы "6rc" совпадал с "6RC", "SoColor" с "socolor" и т.п.
    """
    if not isinstance(search_query, str):
        return pd.DataFrame(columns=list(dataframe.columns) + ["Score"])

    q = search_query.strip().lower()
    if not q:
        return pd.DataFrame(columns=list(dataframe.columns) + ["Score"])

    if column_name not in dataframe.columns:
        return pd.DataFrame(columns=list(dataframe.columns) + ["Score"])

    # главное отличие — .str.lower()
    col_values = dataframe[column_name].astype(str).str.lower().tolist()

    tokens = q.split()
    scorer = fuzz.token_set_ratio if len(tokens) <= 2 else fuzz.token_sort_ratio
    if len(tokens) >= 3:
        threshold = max(threshold, 55)

    matches = process.extract(
        q,
        col_values,
        scorer=scorer,
        score_cutoff=threshold,
        limit=None,
    )

    if not matches:
        return pd.DataFrame(columns=list(dataframe.columns) + ["Score"])

    indices = [m[2] for m in matches]
    scores = [m[1] for m in matches]

    result_df = dataframe.iloc[indices].copy()
    result_df["Score"] = [int(s) for s in scores]

    return result_df


def simple_search(search_query: str, dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Простой поиск по полному слову в name (без fuzzy):
    - для чисел матчим как отдельный токен, а не часть другой цифры.
    """
    if not isinstance(search_query, str):
        return pd.DataFrame(columns=list(dataframe.columns) + ["Score"])

    q = search_query.strip()
    if not q or "name" not in dataframe.columns:
        return pd.DataFrame(columns=list(dataframe.columns) + ["Score"])

    name_series = dataframe["name"].astype(str)

    if q.isdigit():
        pattern = fr"(?<![\d.])\b{q}\b(?![\d.])"
        mask = name_series.str.contains(pattern, case=False, na=False, regex=True)
    else:
        pattern = fr"\b{re.escape(q)}\b"
        mask = name_series.str.contains(pattern, case=False, na=False, regex=True)

    result = dataframe[mask].copy()
    if result.empty:
        return pd.DataFrame(columns=list(dataframe.columns) + ["Score"])

    result["Score"] = 100
    result["Score"] = result["Score"].astype(int)
    return result


def top_number_search(search_query: str, dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Поиск по "топовым" оттенкам 1..10 (если используешь).
    """
    result_df = pd.DataFrame(columns=list(dataframe.columns) + ["Score"])
    try:
        num = int(search_query)
    except Exception:
        return result_df

    if num not in top_product_list:
        return result_df

    frames = []
    for value in top_product_list[num]:
        mask = dataframe["name"].astype(str).str.contains(
            fr"\b{re.escape(value)}\b", case=False, na=False
        )
        tmp = dataframe[mask].copy()
        if tmp.empty:
            continue
        tmp["Score"] = 101
        tmp["Score"] = tmp["Score"].astype(int)
        frames.append(tmp)

    if not frames:
        return result_df

    return pd.concat(frames, ignore_index=True)


def merge_and_sort_dataframes(*dataframes: pd.DataFrame) -> pd.DataFrame:
    """Безопасный concat нескольких результатов."""
    frames = [df for df in dataframes if isinstance(df, pd.DataFrame) and not df.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def sort_dataframes(df: pd.DataFrame) -> pd.DataFrame:
    """Сортировка по Score + удаление дублей по id (если есть)."""
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))

    result = df.copy()
    if "Score" in result.columns:
        result["Score"] = result["Score"].astype(int)
        result.sort_values(by="Score", ascending=False, inplace=True)

    if "id" in result.columns:
        result.drop_duplicates(subset="id", inplace=True)

    return result


# ---------------------------------------------------------
# Бусты по совпадениям слов и чисел из исходного запроса
# ---------------------------------------------------------

def apply_token_boosts(
    df: pd.DataFrame,
    raw_query: str,
    name_column: str = "name",
) -> pd.DataFrame:
    """
    Добавляем бонусы к Score:
    - за каждое совпадающее слово: WORD_MATCH_BOOST
    - за каждое совпадающее число: NUMBER_MATCH_BOOST
    """
    if df is None or df.empty:
        return df

    if not isinstance(raw_query, str) or not raw_query.strip():
        return df

    if name_column not in df.columns:
        return df

    text = raw_query.lower()
    tokens = re.findall(r"\w+", text, flags=re.UNICODE)
    if not tokens:
        return df

    numbers = {t for t in tokens if t.isdigit()}
    words = [t for t in tokens if not t.isdigit() and t not in STOP_WORDS]
    normalized_query = " ".join(tokens)

    def calc_bonus(name: str) -> int:
        if not isinstance(name, str):
            name_str = str(name)
        else:
            name_str = name
        name_low = name_str.lower()

        word_hits = 0
        for w in words:
            if re.search(r"\b" + re.escape(w) + r"\b", name_low):
                word_hits += 1

        num_hits = 0
        for n in numbers:
            if re.search(r"\b" + re.escape(n) + r"\b", name_low):
                num_hits += 1

        missing_words = max(0, len(words) - word_hits)
        penalty = 0
        if len(words) >= 2 and missing_words:
            penalty = missing_words * WORD_MISSING_PENALTY

        missing_numbers = max(0, len(numbers) - num_hits)
        if missing_numbers:
            penalty += missing_numbers * NUMBER_MISSING_PENALTY

        phrase_bonus = 0
        if normalized_query and normalized_query in name_low:
            phrase_bonus = PHRASE_MATCH_BOOST

        return (
            word_hits * WORD_MATCH_BOOST
            + num_hits * NUMBER_MATCH_BOOST
            + phrase_bonus
            - penalty
        )

    result = df.copy()
    if "Score" not in result.columns:
        result["Score"] = 0

    result["Score"] = result["Score"].astype(int) + result[name_column].map(calc_bonus)

    return result


# ---------------------------------------------------------
# Единая точка поиска по DataFrame
# ---------------------------------------------------------

def search_dataframe(df: pd.DataFrame, raw_query: str) -> pd.DataFrame:
    """
    Общая логика:
    - нормализуем запрос (обрезка, чистка пунктуации)
    - заменяем слова по synonyms.json
    - если первый токен не число:
        * строим несколько вариантов запроса (с учётом синонимов)
          напр. "Matrix 6RC" -> ["Matrix 6RC", "socolor 6RC", "super sync 6RC"]
        * для каждого варианта запускаем fuzzy
        * добавляем бусты за попадание слов/чисел
    - если первый токен число:
        * ищем по code/name/barcode или по числу в названии
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=list(getattr(df, "columns", [])) + ["Score"])

    q_norm = normalize_query(raw_query)
    q_norm = replace_synonyms_in_query(q_norm)
    if not q_norm:
        return pd.DataFrame(columns=list(df.columns) + ["Score"])

    tokens = q_norm.split()
    if not tokens:
        return pd.DataFrame(columns=list(df.columns) + ["Score"])

    first_token = tokens[0]

    # ----- ТЕКСТОВАЯ ВЕТКА -----
    if not first_token.isdigit():
        # базовый жёсткий поиск по первому слову
        zero_df = simple_search(first_token, df)

        # базовые варианты для fuzzy
        variants = set()
        variants.add(q_norm)       # весь запрос
        variants.add(first_token)  # только бренд/первое слово

        # --- варианты с синонимами ---
        synonyms = _load_synonyms()
        tokens_lower = [t.lower() for t in tokens]

        for i, t_low in enumerate(tokens_lower):
            if t_low in synonyms and synonyms[t_low]:
                for alt in synonyms[t_low]:
                    # заменяем токен на синоним: Matrix 6RC -> socolor 6RC
                    new_tokens = list(tokens)
                    new_tokens[i] = alt
                    alt_query = " ".join(new_tokens)
                    variants.add(alt_query)

                    # ещё можно искать просто по самому синониму отдельно
                    variants.add(alt)

        # раскладка / транслит для КАЖДОГО варианта
        extra_variants = set()
        for v in list(variants):
            converted = convert_layout(v)
            if converted and converted != v:
                extra_variants.add(converted)

            translit = transliterate(v)
            if translit and translit != v:
                extra_variants.add(translit)

        variants |= extra_variants

        # запускаем fuzzy для всех вариантов
        fuzzy_frames = []
        for v in variants:
            fuzzy_df = search_with_fuzzy(v, df)
            if not fuzzy_df.empty:
                fuzzy_frames.append(fuzzy_df)

        combined = merge_and_sort_dataframes(zero_df, *fuzzy_frames)

        # бустим по всем словам/числам исходного НОРМАЛИЗОВАННОГО запроса
        boosted = apply_token_boosts(combined, q_norm)
        return sort_dataframes(boosted)

    # ----- ЧИСЛОВАЯ ВЕТКА -----
    try:
        numeric_token = str(int(first_token))  # нормализуем ведущие нули
    except ValueError:
        return pd.DataFrame(columns=list(df.columns) + ["Score"])

    if len(numeric_token) > 2:
        # Похоже на код/штрихкод
        mask = (
            df.get("code", pd.Series([], dtype=str))
            .astype(str)
            .str.contains(numeric_token, case=False, na=False)
            | df.get("name", pd.Series([], dtype=str))
            .astype(str)
            .str.contains(numeric_token, case=False, na=False)
            | df.get("barcode", pd.Series([], dtype=str))
            .astype(str)
            .str.contains(numeric_token, case=False, na=False)
        )
        result_df = df[mask].copy()
    else:
        # Короткое число — скорее номер оттенка в названии
        if "name" not in df.columns:
            return pd.DataFrame(columns=list(df.columns) + ["Score"])
        mask = df["name"].astype(str).str.contains(numeric_token, case=False, na=False)
        result_df = df[mask].copy()

    if result_df.empty:
        return pd.DataFrame(columns=list(df.columns) + ["Score"])

    result_df["Score"] = 120
    return sort_dataframes(result_df)
