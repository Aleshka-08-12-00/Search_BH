import pandas as pd
from fuzzywuzzy import fuzz, process
from app.helpers import english_to_russian_transliteration_dict, russian_to_english_transliteration_dict


"""Get language (ru\en) by input symbols"""
def detect_language(text):
    # Count the occurrences of Cyrillic and Latin characters
    count_cyrillic = sum(1 for char in text if '\u0400' <= char <= '\u04FF')
    count_latin = sum(1 for char in text if 'a' <= char <= 'z' or 'A' <= char <= 'Z')

    # Compare the counts to determine the language
    if count_cyrillic >= count_latin:
        return 'ru'
    else:
        return 'en'


"""Change a keyboard layout"""
def convert_layout(text):
    russian_layout = 'йцукенгшщзхъфывапролджэячсмитьбюЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ'
    english_layout = 'qwertyuiop[]asdfghjkl;\'zxcvbnm,.QWERTYUIOP{}ASDFGHJKL:"ZXCVBNM<>'

    language = detect_language(text)
    # print('before layout changes: ', text)
    if language == 'ru':
        result = []
        i = 0
        while i < len(text):
            if detect_language(text[i]) == 'en':
                result.append(text[i])
            elif detect_language(text[i]) == 'ru':
                translation_table = str.maketrans(russian_layout, english_layout)
                result.append(text[i].translate(translation_table))
            i += 1
        # print('after layout changes: ', result)
        return ''.join(result)
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
def search_with_fuzzy(search_query, dataframe, column_name='name', threshold=65):

    if not isinstance(search_query, str):
        return pd.DataFrame()

    # Use fuzzywuzzy process.extract to find matches with a similarity threshold
    matches = process.extract(search_query, dataframe[column_name], limit=len(dataframe), scorer=fuzz.partial_ratio)

    # Filter matches based on the threshold
    filtered_matches = [match for match in matches if match[1] >= threshold]

    # Extract matched values and set Score to score minus one for everyone
    matched_values = [match[0] for match in filtered_matches]
    scores = [match[1] - 1 for match in filtered_matches]

    result_df = pd.merge(pd.DataFrame({'name': matched_values, 'Score': scores}), dataframe, on='name', how='inner')

    return result_df

""" Simple search with no libs """
def simple_search(search_query, dataframe):
    # print(search_query)
    result = dataframe[dataframe['name'].str.contains(fr'\b{search_query}\b', case=False, na=False)].copy()
    result['Score'] = 100  # Add 'Score' column with value 101
    result['Score'] = result['Score'].astype(int)  # Ensure 'Score' column is of integer type
    return result


"""Merge all dataframes to get one result"""
def merge_and_sort_dataframes(df1, df2, df3, df4, df5):

    datasrames = [df1, df2, df3, df4, df5]
    return pd.concat(datasrames, ignore_index=True)


def sort_dataframes(merged_df):

    merged_df.drop_duplicates(subset='blockElementId', inplace=True)

    # Sort the dataframe by the 'Score' column (adjust 'Score' to the actual column name)
    merged_df.sort_values(by='Score', inplace=True, ascending=False)

    return merged_df
