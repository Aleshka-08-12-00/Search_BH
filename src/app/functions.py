import pandas as pd
from fuzzywuzzy import fuzz, process
from app.helpers import english_to_russian_transliteration_dict, russian_to_english_transliteration_dict


"""Get language (ru\en) by input symbols"""
def detect_language(text):
    # Check for the presence of Cyrillic characters
    has_cyrillic = any('\u0400' <= char <= '\u04FF' for char in text)

    # Check for the presence of Latin characters
    has_latin = any('a' <= char <= 'z' or 'A' <= char <= 'Z' for char in text)

    if has_cyrillic and not has_latin:
        return 'ru'
    elif has_latin and not has_cyrillic:
        return 'en'
    else:
        return None


"""Change a keyboard layout"""
def convert_layout(text):
    russian_layout = 'йцукенгшщзхъфывапролджэячсмитьбюЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ'
    english_layout = 'qwertyuiop[]asdfghjkl;\'zxcvbnm,.QWERTYUIOP{}ASDFGHJKL:"ZXCVBNM<>'

    language = detect_language(text)
    if language == 'ru':
        translation_table = str.maketrans(russian_layout, english_layout)
        return text.translate(translation_table)
    elif language == 'en':
        translation_table = str.maketrans(english_layout, russian_layout)
        return text.translate(translation_table)


"""Divide into letters and transliterate"""
def custom_transliterate(text, transliteration_dict):
    result = []
    i = 0
    while i < len(text):
        current_char = text[i]
        next_chars_3 = text[i:i + 3]  # Check for three-character combinations
        next_chars_2 = text[i:i + 2]  # Check for two-character combinations

        if next_chars_3 in transliteration_dict:
            result.append(transliteration_dict[next_chars_3])
            i += 3
        elif next_chars_2 in transliteration_dict:
            result.append(transliteration_dict[next_chars_2])
            i += 2
        else:
            result.append(transliteration_dict.get(current_char, current_char))
            i += 1
    return ''.join(result)


"""Translate to another language"""
def transliterate(text):
    detected_language = detect_language(text.lower())

    if detected_language == 'ru':
        return custom_transliterate(text.lower(), russian_to_english_transliteration_dict)
    elif detected_language == 'en':
        return custom_transliterate(text.lower(), english_to_russian_transliteration_dict)
    else:
        return None


"""Search in dataframe with mistakes"""
def search_with_fuzzy(search_query, dataframe, column_name='name', threshold=75):

    # Use fuzzywuzzy process.extract to find matches with a similarity threshold
    matches = process.extract(search_query, dataframe[column_name], limit=len(dataframe), scorer=fuzz.partial_ratio)

    # Filter matches based on the threshold
    filtered_matches = [match for match in matches if match[1] >= threshold]

    # Extract matched values and their corresponding scores
    matched_values = [match[0] for match in filtered_matches]
    scores = [match[1] for match in filtered_matches]

    result_df = pd.merge(pd.DataFrame({'name': matched_values, 'Score': scores}), dataframe, on='name', how='inner')

    return result_df


"""Merge all dataframes to get one result"""
def merge_and_sort_dataframes(df1, df2, df3, df4):
    # Merge the dataframes on a common column (assuming 'code' is the common column)
    merged_df = pd.concat([df1, df2, df3, df4], ignore_index=True)

    # Remove duplicates based on the 'code' column
    merged_df.drop_duplicates(subset='blockElementId', inplace=True)

    # Sort the dataframe by the 'Score' column (adjust 'Score' to the actual column name)
    merged_df.sort_values(by='Score', inplace=True, ascending=False)

    return merged_df
