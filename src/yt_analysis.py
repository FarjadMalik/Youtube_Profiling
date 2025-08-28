import matplotlib.pyplot as plt

from wordcloud import WordCloud
from keybert import KeyBERT

# Internal imports
from utils.helpers import STOPWORDS


def plot_word_cloud(word_blob: str, title: str):
    """
    
    """
    wordcloud = WordCloud(width=800, height=400, background_color='white', stopwords=STOPWORDS).generate(word_blob)

    plt.figure(figsize=(12, 6))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.title(f"Word Cloud of {title}")
    plt.show()

# def extract_keywords(word_blob: str, n_words: int):
#     """

#     """
#     kw_model = KeyBERT()
#     keywords = kw_model.extract_keywords(
#         word_blob,
#         keyphrase_ngram_range=(1, 2),   # extract unigrams and bigrams
#         use_maxsum=True,
#         top_n=n_words
#     )
#     return keywords

# Aggregate titles, descriptions, and tags from get_video_info and get_video_tags 
# and run keyword extraction or topic modeling (LDA, BERTopic).
def topic_modeling():
    pass

