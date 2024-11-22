# %%
import spacy
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from collections import defaultdict
import math
import pandas as pd
from plotnine import ggplot, aes, geom_bar, theme, element_text, labs,coord_flip,scale_y_log10

# Ensure reproducibility for language detection
DetectorFactory.seed = 0

nlp = spacy.load("en_core_web_sm")


# %%

def detect_language(text):
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"

def process_texts(texts):
    language_counts = defaultdict(int)
    unknown_texts = 0
    for text in texts:
        if isinstance(text, str):
            doc = nlp(text)
            detected_language = detect_language(text)
            if detected_language == "unknown":
                unknown_texts += 1
            language_counts[detected_language] += 1
        # else:
        #     print(f"Skipping non-string value: {text} (type: {type(text)})")

    return language_counts, unknown_texts



# %%
transcriptsFile="" #insert the file path to the transcripts per video
df = pd.read_csv(transcriptsFile) #read as a pandas dataframe

# %%

texts = df['voice_to_text'].tolist()

# Filter out NaN values
filtered_data = [x for x in texts if not (isinstance(x, float) and math.isnan(x)) and pd.isna(x)]

language_counts,unknown_texts =  process_texts(texts)

total_counts = sum(language_counts.values())

print(f"Language Counts: {dict(language_counts)}")
print(f"Total Count: {total_counts}")
print(f"Number of texts: {len(texts)}")
print(f"Unknown texts: {unknown_texts}")
# assert total_counts == len(texts), "Total counts do not match the number of texts"



# %%

#country labels and count
lang_dict = {'en': 213608,'es': 7279, 'pl': 66, 'tl': 309, 'hr': 19, 'fr': 439, 'zh-cn': 159, 'nl': 95, 'sq': 39, 'ar': 195, 'fa': 10, 'af': 174, 'da': 57, 'id': 166, 'it': 219, 'ko': 119, 'ru': 461, 'pt': 338, 'sw': 80, 'ja': 171, 'so': 226, 'tr': 165, 'cy': 181, 'sv': 30, 'no': 88, 'unknown': 49, 'vi': 76, 'de': 312, 'cs': 12, 'et': 67, 'th': 11, 'ca': 23, 'hu': 8, 'uk': 1, 'ro': 33, 'sk': 23, 'fi': 24, 'lt': 4, 'ne': 1, 'sl': 17, 'bg': 3, 'mk': 1, 'ur': 2, 'lv': 2}

#lang code --> full name
language_map = {
    'en': 'English', 'es': 'Spanish', 'pl': 'Polish', 'tl': 'Tagalog', 'hr': 'Croatian', 
    'fr': 'French', 'zh-cn': 'Chinese', 'nl': 'Dutch', 'sq': 'Albanian', 'ar': 'Arabic', 
    'fa': 'Persian', 'af': 'Afrikaans', 'da': 'Danish', 'id': 'Indonesian', 'it': 'Italian', 
    'ko': 'Korean', 'ru': 'Russian', 'pt': 'Portuguese', 'sw': 'Swahili', 'ja': 'Japanese', 
    'so': 'Somali', 'tr': 'Turkish', 'cy': 'Welsh', 'sv': 'Swedish', 'no': 'Norwegian', 
    'unknown': 'Unknown', 'vi': 'Vietnamese', 'de': 'German', 'cs': 'Czech', 'et': 'Estonian', 
    'th': 'Thai', 'ca': 'Catalan', 'hu': 'Hungarian', 'uk': 'Ukrainian', 'ro': 'Romanian', 
    'sk': 'Slovak', 'fi': 'Finnish', 'lt': 'Lithuanian', 'ne': 'Nepali', 'sl': 'Slovenian', 
    'bg': 'Bulgarian', 'mk': 'Macedonian', 'ur': 'Urdu', 'lv': 'Latvian'
}

df = pd.DataFrame(list(lang_dict.items()), columns=['LanguageCode', 'Count'])
df['Language'] = df['LanguageCode'].map(language_map)
# Sort the DataFrame by 'Count' in descending order
df = df.sort_values(by='Count', ascending=False)



#plot the distribution of languages in the transcript
p = (ggplot(df, aes(x='reorder(Language, -Count)', y='Count'))
     + geom_bar(stat='identity')
     + coord_flip()
     + scale_y_log10()
     + theme(
         axis_text_y=element_text(size=45), 
         axis_text_x=element_text(size=45),  
         axis_title_x=element_text(size=60),  
         axis_title_y=element_text(size=60),  
         plot_title=element_text(size=70, face='bold'),  
         panel_background=element_rect(fill='white'),
         plot_background=element_rect(fill='white'),
         figure_size=(32, 28)  # Increase figure size
     )
     + labs(title="", x="Language", y="Count")
     )

p.save("languages.png", dpi=500, limitsize=False)


