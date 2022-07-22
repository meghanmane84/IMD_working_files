# import torch
import pickle
# import nltk
# from transformers import BartTokenizer,BartForConditionalGeneration

# nltk.download('stopwords')
# nltk.download('punkt')
# nltk.download("words")

from nltk.corpus import stopwords,words
from nltk.tokenize import word_tokenize

mt5_tokens = pickle.load(open('D:/CODE/SIH/TLDR_SIH/backend/model/mt5_tokenizer.pkl', 'rb'))
mt5_model = pickle.load(open('D:/CODE/SIH/TLDR_SIH/backend/model/mt5_model.pkl', 'rb'))

summarizer_model = pickle.load(open('D:/CODE/SIH/TLDR_SIH/backend/model/summarization_model.pkl', 'rb'))

def get_title(input_text):

    print("STARTING MT5 MODEL")
    tokens = mt5_tokens.encode(input_text, truncation=True, max_length = 1000, return_tensors="pt")
    title = mt5_model.generate(tokens)
    return( mt5_tokens.decode(title[0]) )

def summary_model(input_text):

    print("STARTING SUMMARIZER MODEL")
    summary = summarizer_model(input_text, truncation=True, max_length = 1000)
    return summary


# clean_text = text_cleaning(input_text)
# def text_cleaning(input_text):
    
#     print("CLEANING TEXT")
#     stop_words = set(stopwords.words('english'))
#     eng = set(words.words())
#     input_words = word_tokenize(input_text)
#     clean_text = ""
#     for word in input_words:
#         word=word.lower()
#         if word.isalpha() and word not in stop_words and word in eng:
#             clean_text+=" "+word
#     print(clean_text)              
#     return clean_text 
