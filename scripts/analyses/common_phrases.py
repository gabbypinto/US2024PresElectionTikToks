# %% [markdown]
# ## Most common query phrases used in the video_description - based on the keywords.txt files. video_description is the caption
# 

# %%
import pandas as pd
import os 
import json 
from plotnine import ggplot, aes, element_blank,element_rect,geom_bar, theme, element_text, coord_flip,labs
import pandas as pd


# %%
#read the FINALIZED metadata file
filePathMetadata="" #path to the full metadata file
df = pd.read_csv(filePathMetadata)
print(f"Number of metadata/videos: {len(df)}")

# %%
keywordsTextFile="" #path to the most recent phase no. (text) file
with open (keywordsTextFile,'r') as file:
    lines = [line.strip() for line in file if line.strip()]
        
# print(lines)

# %%
captions = df['video_description'].tolist()


"""
Visualize the top 20 phrases
"""
def plot_top_phrases(phrase_counts, top_n=20,output_file='top_phrases.png'):
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame(list(phrase_counts.items()), columns=['Phrase', 'Count'])
    
    # Sort the DataFrame by 'Count' in descending order
    df = df.sort_values(by='Count', ascending=False)
    
    # Select the top N phrases
    df_top = df.head(top_n)
    
    # Create a bar plot
    p = (
        ggplot(df_top, aes(x='reorder(Phrase, Count)', y='Count')) +
        geom_bar(stat='identity') +
        coord_flip() +
        theme(
            axis_text_x=element_text(rotation=45, hjust=1),
            panel_background=element_rect(fill='white'),
            plot_background=element_rect(fill='white'),
            panel_grid_major=element_blank(),  # Remove major grid lines
            panel_grid_minor=element_blank()   # Remove minor grid lines
        ) +
        labs(title='', x='Phrase', y='Count')
    )
    
    # return p
    p.save(output_file, dpi=300)
    print(f"Plot saved as {output_file}")
    print(p)

"""
Save dictionary to JSON 
"""
def save_dict_to_file(dictionary, filename):
    with open(filename, 'w') as file:
        json.dump(dictionary, file)

"""
Read dictionary from JSON
"""
def read_dict_from_file(filename):
    with open(filename, 'r') as file:
        return json.load(file)


"""
Arg: list of type str, list of type phrases
Return: dict
"""
def count_phrases(sentences, phrases):
    # Initialize a dictionary to store the counts
    phrase_counts = {phrase: 0 for phrase in phrases}
    
    # Iterate over each sentence
    for sentence in sentences:
        if isinstance(sentence, str):
            # Check each phrase in the list
            for phrase in phrases:
                # Increment the count if the phrase is found in the sentence
                if phrase in sentence:
                    phrase_counts[phrase] += 1
    
    return phrase_counts

result = count_phrases(captions, lines)

#Print the common phrases shown within the video description
for phrase, count in result.items():
    if count > 0:
        print(f"{phrase}: {count}")

#save the dictionary to json (optional)
save_dict_to_file(result, 'phrase_counts.json')

#read the dictionary from the recently created json file (optional)
loaded_result = read_dict_from_file('phrase_counts.json')



