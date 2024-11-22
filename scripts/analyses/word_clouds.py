# %%
!pip install wordcloud

# %%
!pip install contractions


# %%
import nltk
from nltk.corpus import stopwords
from nltk import bigrams

import contractions
import re 
import string


from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import Counter
import pandas as pd


nltk.download('stopwords')
english_stopwords = stopwords.words('english')
print(english_stopwords)
# english_stopwords.append()


# %%

datasetfilePath="" #file path to csv file 
df = pd.read_csv(datasetfilePath)

# %% [markdown]
# ### After language classification

# %%
#choose english transcripts
df = df[df['lang'] == 'en']


# %%
## clean text

#removes stopwords
def remove_stopwords_and_punctuation(text):
    #expand contractions
    text = contractions.fix(text)
    #extra spaces
    text = ' '.join(text.split())
    #convert to lowercase
    text = text.lower()
    #remove numbers
    text = re.sub(r'\d+', '', text)
    #remove special characters
    text = re.sub(r'[^\x00-\x7F]+', '', text) 
    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    #remove stopwords
    words = text.split()
    filtered_words = [word for word in words if word.lower() not in english_stopwords]
    return ' '.join(filtered_words)



#remove stopwords from each transcripts
clean_texts = []
texts = df['voice_to_text'].tolist()
for transcript in texts:

    clean_text = remove_stopwords(transcript)
    clean_texts.append(clean_text)


df['clean_voice_to_text'] = clean_texts

# %%

## BIGRAMS


# Generate bigrams for each cleaned text and combine them into one list
bigram_list = []
for text in clean_texts:
    words = text.split()
    bigram_list.extend([' '.join(bigram) for bigram in bigrams(words)])

# Count the frequency of each bigram
bigram_counts = Counter(bigram_list)

# Generate the word cloud for bigrams
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(bigram_counts)

# Display the word cloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.savefig("english_word_clouds.png")
plt.show()

# Count the number of unique bigrams in the word cloud
unique_bigrams_in_cloud = len(wordcloud.words_)
print(f"Number of unique bigrams in the word cloud: {unique_bigrams_in_cloud}")

# %% [markdown]
# ### Bigrams in Spanish Transcripts

# %%
dataFile="" #file path to full data path
df = pd.read_csv(dataFile)
df = df[df['lang']=='es'] #grab all texts that are classified as Spanish
texts = df['voice_to_text'].tolist()


nltk.download('stopwords')
spanish_stopwords = stopwords.words('spanish')

#function that removes stopwords
def remove_stopwords(text):
    words = text.split()
    filtered_words = [word for word in words if word.lower() not in spanish_stopwords]
    return ' '.join(filtered_words)

clean_texts = []
for transcript in texts:
    clean_text = remove_stopwords(transcript)
    clean_texts.append(clean_text)


# Generate bigrams for each cleaned text and combine them into one list
bigram_list = []
for text in clean_texts:
    words = text.split()
    bigram_list.extend([' '.join(bigram) for bigram in bigrams(words)])

# Count the frequency of each bigram
bigram_counts = Counter(bigram_list)

# Generate the word cloud for bigrams
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(bigram_counts)

# Display the word cloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.savefig("spanish_word_cloud.png")
plt.show()

# Count the number of unique bigrams in the word cloud
unique_bigrams_in_cloud = len(wordcloud.words_)
print(f"Number of unique bigrams in the word cloud: {unique_bigrams_in_cloud}")



