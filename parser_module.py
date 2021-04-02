import logging
## for file logging
logging.basicConfig(filename='search_engine.log', filemode='a',
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',)
import re
import string
import unicodedata
import os
import pandas as pd
import numpy as np
import gensim
from stemmer import Stemmer
from document import Document
from TermInDoc import TermInDoc

#########################################################################################################
#             THIS READER IS JUST FOR PARSER TESTING - DELETE AFTER DONE TESTIMG!                       #
#########################################################################################################
class Reader(object):
    def __init__(self,dir_path):
        self.PATH = dir_path

    def read_file(self, date_dir_path):
        df = pd.read_parquet(date_dir_path, engine="pyarrow")
        ### sampling only x for testing, DONT FORGET TO DELETE AFTER!!!!
        #df = df.sample(1)
        df = df[33:40]
        return df

    def read_50_random_samples_from_all_parquets(self):
        #GOAL: to read random 50 files from each Parquet
        dir_list = os.listdir(self.PATH)#list of names(strings) of all files\dirs in PATH
        #df = pd.DataFrame(columns=['tweet_id','tweet_date', 'full_text', 'urls', 'indices', 'retweet_text','retweet_urls', 'retweet_indices', 'quoted_text', 'quoted_urls', 'quoted_indices', 'retweet_quoted_text','retweet_quoted_urls', 'retweet_quoted_indices' ])
        df = None
        flag_first = True
        for elem in dir_list:
            if 'date' in elem:
                date_dir_path = os.path.join(self.PATH, elem)
                if flag_first == True:
                    df = self.read_file(date_dir_path)
                    flag_first = False
                    break
                else:
                    temp_df = self.read_file(date_dir_path)
                    df = df.append(temp_df, ignore_index = True)
            elif os.path.isfile(elem):
                print("It is a normal file")
            #else:
            #print("It is a special file (socket, FIFO, device file)" )
        return df

#########################################################################################################
#              READER IS JUST FOR PARSER TESTING - DELETE AFTER DONE TESTIMG!                           #
#########################################################################################################




class Parse:
    def __init__(self):
        self.stop_words = ['i', 'non', 'me','my', 'myself', 'we', 'We','our', 'ours', 'ourselves',
                           'you', "you're", "you've",'At','For','Of','Him','Her','dont', 'doesnt','Not','So','A','wont',
                           'Has','Have', 'If', 'Who', 'Whom', 'Be', 'Being', 'Which', 'shouldnt', 'wouldnt', 'couldnt',
                           "you'll","you'd",'your', 'yours','yourself', 'yourselves','he','him','his','himself','she',
                           "she's",'her','hers','herself','it',"it's",'its','itself','they','them','their','theirs','themselves',
                           'what','which', 'who','whom','this','that',"that'll",'these','those','am','is','are','was',
                           'were','be','been','being', 'have','has','had','having','do','does','did','doing','a','an',
                           'the','and','but','if', 'or','because', 'as', 'until',  'while', 'of','at', 'by','for', 'with',
                           'about','against','between','into','through', 'during','before','after','above', 'below', 'to',
                           'from','up','down','in','out','on','off','over','under','again','further','then','once','here',
                           'there','when','where','why','how','all','any','both','each','few', 'more','most','other','some',
                           'such','no','nor','not','only','own','same','so','than','too','very','s','t','can','will','just',
                           'don', "don't",'should',"should've",'now', 'd', 'll', 'm', 'o', 're', 've','y','ain','aren',"aren't",
                           'couldn',"couldn't",'didn', "didn't",'doesn',"doesn't",'hadn',"hadn't",'hasn',"hasn't",'haven',
                           "haven't",'isn', "isn't",'ma','mightn',"mightn't",'mustn', "mustn't",'needn', "needn't",'shan',"shan't",
                           'shouldn',"shouldn't",'wasn', "wasn't",'weren', "weren't",'won',"won't",'wouldn',"wouldn't",
                           "twitter.com/status", "twitter.com", "twitter", 'lot', 'lots', 'many', 'much']
        #self.documents_dict = {}
        #self.tokenizer = TweetTokenizer()
        #self.entitiesCandidates_dict = {}
        #self.UpperLower_dict = {}
        #self.model = gensim.models.KeyedVectors.load_word2vec_format('GoogleNews-vectors-negative300.bin', binary=True)

    def removeURLfromString(self, sentence):
        sentence = re.sub(r'(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?','', sentence)
        return sentence

    def parse_url(self, url_str):
        """
        tokenize url into separate words, but keep domain intact (ex: tweeter.com)
        :param url_str:
        :return: list
        """
        if not url_str:
            return []
        try:
            tokens = re.split('[^a-zA-Z0-9.]', url_str) #mysterious function, not 100% sure why this works, it just sort of does....
            clean_url_tokens = [tkn for tkn in tokens if len(tkn) >= 1]
            #split 'www' from domain
            for i, tkn in enumerate(clean_url_tokens):
                if re.search(r'www.[A-Za-z0-9.]*', tkn):
                    clean_url_tokens.pop(i)
                    if len(tkn) > 4:
                        #clean_url_tokens.append(tkn[:4].strip('.'))
                        clean_url_tokens.append(tkn[4:])
                    #else:
                    #clean_url_tokens.append(tkn.strip('.'))
            return clean_url_tokens
        except Exception as err:
            logging.info("parse_url() threw this error:\n {0}".format(err))
        return []


    def getindices(self,s):
        lst = []
        for i, c in enumerate(s):
            if c.isupper() and i+1 < len(s) and s[i+1].islower():
                lst.append(i)
                break
        return lst
        # return [i for i, c in enumerate(s) if c.isupper()]


    def Flatten(self,TheList):
        if isinstance(TheList, list):
            return [a for i in TheList for a in self.Flatten(i)]
        else:
            return [TheList]



    # def findEntitiesCandidates(self,sentence):
    #     try:
    #         pattern = r'([A-Z][a-z]+(?=\s[A-Z])(?:\s[A-Z][a-z]+)+)'
    #         cand = re.findall(r'([A-Z][a-z]+(?=\s[A-Z])(?:\s[A-Z][a-z]+)+)',sentence)
    #         cand += re.findall(r'([A-Z]{3,})', sentence)
    #         for ent in cand:
    #             if len(ent.split()) > 3: #not taking "entity" made up of 4 words or more
    #                 continue
    #             if ent not in self.entitiesCandidates_dict:
    #                 self.entitiesCandidates_dict[ent] = 1
    #             else:
    #                 self.entitiesCandidates_dict[ent] += 1
    #     except Exception as err:
    #         logging.info("findEntitiesCandidates() threw this error:\n {0}".format(err))
    #     return sentence
    #
    # def findCandidatesForUpperLowerCase(self, sentence):
    #     try:
    #         pattern = r'(\b[A-Z][a-z]*[A-Z]*\b)'
    #         cand = re.findall(pattern,sentence)
    #         for ent in cand:
    #             if ent not in self.UpperLower_dict:
    #                 self.UpperLower_dict[ent] = 1
    #             else:
    #                 self.UpperLower_dict[ent] += 1
    #     except Exception as err:
    #         logging.info("findCandidatesForUpperLowerCase() threw this error:\n {0}".format(err))
    #     return sentence

    def catch_hashtags(self,sentence):
        """
        #Stay_At_Home -> #stayathome
        #StayAtHome -> #stayathome
        :param tokens_list:
        :return:
        """
        tokens_list = sentence.split()
        try:
            for idx,tkn in enumerate(tokens_list):
                if tkn.startswith('#'):
                    replace = idx
                    if len(tkn.split('_')) == 1:
                        cap_idx = self.getindices(tkn)
                        if len(cap_idx) == 0:
                            out = [tkn[1:]] + [tkn]
                        else:
                            out = [tkn[1:cap_idx[0]]] + re.findall(r'[A-Z][a-z\W]+',tkn) + [tkn]
                            if re.search(r'COVID', tkn):
                                out.append('COVID')
                            #outs = [i for i in out]
                    else:
                        last = tkn.replace('_','')
                        tkn = tkn.strip('#')
                        words = tkn.split('_')
                        outs = words + [last]
                    tokens_list[replace] = out
            flt_tokens = [i for i in self.Flatten(tokens_list) if len(i) >= 1]
            out = ' '.join(flt_tokens)
            return out
        except Exception as err:
            logging.info("catch_hashtags() threw this error:\n {0}".format(err))
        return sentence


    def handlePercentExpression(self, sentence):
        try:
            sentence =  re.sub(r'\spercentage|\spercent|\s%', '%', sentence)
            return sentence
        except Exception as err:
            logging.info("handlePercentExpression() threw this error:\n {0}".format(err))
        return sentence

    def remove_newlines_from_sentence(self, sentence):
        try:
            str_whitespace = '\t\n\r\v\f'
            return sentence.translate(str.maketrans(str_whitespace, ' '*len(str_whitespace)))
        except Exception as err:
            logging.info("remove_newlines_from_sentence() threw this error:\n {0}".format(err))
        return sentence

    def handleNumberExpressions(self, sentence):
        try:
            #if there's time -> check if \.*\d* on all regex expressions is better replaced with ([\.][\d]+){0,1}
            #todo make it so that regex doesn't catch the numbers from words like: "number$" "$number" -> consider \\b
            #handle Billions
            #find substrings that are over 10 digits
            for m in re.finditer(r'(?<!\.)( [\d]+\.*[\d]* Billion)|(?<!\.)([\d]+\.*[\d]* Billion)|'
                                 r'(?<!\.)( [\d]+\.*[\d]* billion)|(?<!\.)([\d]+\.*[\d]* billion)|'
                                 r'(?<!\.)( [\d],([\d]{3},){2,}[\d]{3}\.*[\d]*)|(?<!\.)([\d],([\d]{3},){2,}[\d]{3}\.*[\d]*)|'
                                 r'(?<!\.)( [\d]{10,}\.*[\d]*)|(?<!\.)([\d]{10,}\.*[\d]*)', sentence, flags=re.IGNORECASE):
                sentence = re.sub(m.group(0), 'ENTERHERE', sentence)
                handled_number_exp = self.handleBillionExpression(m.group(0))
                sentence = re.sub(r'ENTERHERE',' '+handled_number_exp , sentence)
            #handle Millions
            #find substrings that have 7 digits - 9 digits
            for m in re.finditer(r'(?<!\.)( [\d]+\.*[\d]* Million)|(?<!\.)([\d]+\.*[\d]* Million)|'
                                 r'(?<!\.)( [\d]+\.*[\d]* million)|(?<!\.)([\d]+\.*[\d]* million)|'
                                 r'(?<!\.)( ([\d]{1,3},)([\d]{3},)[\d]{3}\.*[\d]*)|(?<!\.)(([\d]{1,3},)([\d]{3},)[\d]{3}\.*[\d]*)|'
                                 r'(?<!\.)( [\d]{7,9}\.*[\d]*)|(?<!\.)([\d]{7,9}\.*[\d]*)', sentence, flags=re.IGNORECASE):
                sentence = re.sub(m.group(0), 'ENTERHERE', sentence)
                handled_number_exp = self.handleMillionExpression(m.group(0))
                sentence = re.sub(r'ENTERHERE',' '+handled_number_exp , sentence)
            #handle Thousands
            #find substrings that have 4 digits - 6 digits
            for m in re.finditer(r'(?<!\.)( [\d]+\.*[\d]* Thousand)|(?<!\.)([\d]+\.*[\d]* Thousand)|'
                                 r'(?<!\.)( [\d]+\.*[\d]* thousand)|(?<!\.)([\d]+\.*[\d]* thousand)|'
                                 r'(?<!\.)( [\d]{1,3},[\d]{3}\.*[\d]*)|(?<!\.)([\d]{1,3},[\d]{3}\.*[\d]*)|'
                                 r'(?<!\.)( [\d]{4,6}\.*[\d]*)|(?<!\.)([\d]{4,6}\.*[\d]*)', sentence, flags=re.IGNORECASE):
                num = m.group(0).strip()
                if len(num)==4 and  num[0] == '2':
                    continue
                sentence = re.sub(m.group(0), 'ENTERHERE', sentence)
                handled_number_exp = self.handleThousandExpression(m.group(0))
                sentence = re.sub(r'ENTERHERE',' '+handled_number_exp , sentence)
            return sentence
        except Exception as err:
            logging.info("handleNumberExpressions() threw this error:\n {0}".format(err))
        return sentence


    def handleThousandExpression(self, thousand_str):
        try:
            # 55.18 Thousand -> 55.18K   (with or without trailing whitespace)
            if re.search(r'(?<!\.)( [\d]+\.*[\d]* Thousand)|(?<!\.)([\d]+\.*[\d]* Thousand)|(?<!\.)( [\d]+\.*[\d]* thousand)|(?<!\.)([\d]+\.*[\d]* thousand)', thousand_str):
                thousand_str = re.sub(r'(?<=\d )Thousand', 'K', thousand_str)
                thousand_str = re.sub(r'(?<=\d )thousand', 'K', thousand_str)
                thousand_str = re.sub(r'\s*', '', thousand_str)
                #truncate X.xxxxxxxxxx...K to X.xxx
                thousand_str = self.truncateDecimalsToThreeDigits(thousand_str)
                return thousand_str
            # 1000 -> 1K  (7 digits - 9 digits seq )
            elif re.search(r'(?<!\.)( [\d]{4,6}\.*[\d]*)|(?<!\.)([\d]{4,6}\.*[\d]*)', thousand_str):
                # remove all non-digits from string
                thousand_str = re.sub(r'[^\d.]+', '', thousand_str)
                thousand_str = thousand_str.rstrip(".")
                try:
                    thousand_str = float(thousand_str) / 1000
                    thousand_str = str(thousand_str).rstrip("0").rstrip(".") + 'K'
                    thousand_str = re.sub(r'\s*', '', thousand_str)
                    # truncate X.xxxxxxxxxx...K to X.xxxK
                    thousand_str = self.truncateDecimalsToThreeDigits(thousand_str)
                except Exception as err:
                    try:
                        logging.info("handleThousandExpression() threw error while trying to convert to float:\n{0}\n ".format(err))
                    except Exception:
                        pass
                return thousand_str
            # 10,123.45678 -> 10.123K (comma delimited number)
            elif re.search(r'(?<!\.)( [\d]{1,3},[\d]{3}\.*[\d]*)|(?<!\.)([\d]{1,3},[\d]{3}\.*[\d]*)', thousand_str):
                # remove all non-digits from string
                thousand_str = re.sub(r'[^\d.]+', '', thousand_str)
                thousand_str = thousand_str.rstrip(".")
                try:
                    # convert to float
                    thousand_str = float(thousand_str) / 1000
                    thousand_str = str(thousand_str).rstrip("0").rstrip(".") + 'K'
                    #truncate X.xxxxxxxxxx...K to X.xxxK
                    thousand_str = self.truncateDecimalsToThreeDigits(thousand_str)
                except Exception as err:
                    try:
                        logging.info("handleThousandExpression() threw error while trying to convert to float:\n{0}\n ".format(err))
                    except Exception:
                        pass
                return thousand_str
            return thousand_str
        except Exception as err:
            logging.info("handleThousandExpression() threw this error:\n {0}".format(err))
        return ''



    def handleMillionExpression(self, million_str):
        try:
            # 55.18 Million -> 55.18M    (with or without trailing whitespace)
            if re.search(r'(?<!\.)( [\d]+\.*[\d]* Million)|(?<!\.)([\d]+\.*[\d]* Million)|(?<!\.)( [\d]+\.*[\d]* million)|(?<!\.)([\d]+\.*[\d]* million)', million_str):
                million_str = re.sub(r'(?<=\d )Million', 'M', million_str)
                million_str = re.sub(r'(?<=\d )million', 'M', million_str)
                million_str = re.sub(r'\s*', '', million_str)
                #truncate X.xxxxxxxxxx...M to X.xxx
                million_str = self.truncateDecimalsToThreeDigits(million_str)
                return million_str
            # 999999999 -> 999.999   (7 digits - 9 digits seq )
            # 1000000 -> 1M   (7 digits - 9 digits seq )
            elif re.search(r'(?<!\.)( [\d]{7,9}\.*[\d]*)|(?<!\.)([\d]{7,9}\.*[\d]*)', million_str):
                # remove all non-digits from string
                million_str = re.sub(r'[^\d.]+', '', million_str)
                million_str = million_str.rstrip(".")
                try:
                    million_str = float(million_str) / 1000000
                    million_str = str(million_str).rstrip("0").rstrip(".") + 'M'
                    million_str = re.sub(r'\s*', '', million_str)
                    # truncate X.xxxxxxxxxx...M to X.xxxM
                    million_str = self.truncateDecimalsToThreeDigits(million_str)
                except Exception as err:
                    try:
                        logging.info("in handleMillionExpression() conversion to float(x) falied:\n{0}\n ".format(err))
                    except Exception:
                        pass
                return million_str
            # 10,123,000.45678378 -> 10.123M (comma delimited number)
            elif re.search(r'(?<!\.)( ([\d]{1,3},)([\d]{3},)[\d]{3}\.*[\d]*)|(?<!\.)(([\d]{1,3},)([\d]{3},)[\d]{3}\.*[\d]*)', million_str):
                # remove all non-digits from string
                million_str = re.sub(r'[^\d.]+', '', million_str)
                million_str = million_str.rstrip(".")
                try:
                    # convert to float
                    million_str = float(million_str) / 1000000
                    million_str = str(million_str).rstrip("0").rstrip(".") + 'M'
                    #truncate X.xxxxxxxxxx...M to X.xxxM
                    million_str = self.truncateDecimalsToThreeDigits(million_str)
                except Exception as err:
                    try:
                        logging.info("in handleMillionExpression() conversion to float(x) falied:\n{0}\n ".format(err))
                    except Exception:
                        pass
                return million_str
            return million_str
        except Exception as err:
            logging.info("handleMillionExpression() threw this error:\n {0}".format(err))
        return ''


    def handleBillionExpression(self, billion_str):
        try:
            # 55.18 Billion (with or without trailing whitespace)
            if re.search(r'(?<!\.)( [\d]+\.*[\d]* Billion)|(?<!\.)([\d]+\.*[\d]* Billion)|(?<!\.)( [\d]+\.*[\d]* billion)|(?<!\.)([\d]+\.*[\d]* billion)', billion_str):
                billion_str = re.sub(r'(?<=\d )Billion', 'B', billion_str)
                billion_str = re.sub(r'(?<=\d )billion', 'B', billion_str)
                billion_str = re.sub(r'\s*', '', billion_str)
                #truncate X.xxxxxxxxxx...B to X.xxx
                billion_str = self.truncateDecimalsToThreeDigits(billion_str)
                return billion_str
            # 1000000000567 (10 digits seq or more)
            elif re.search(r'(?<!\.)( [\d]{10,})|(?<!\.)([\d]{10,})', billion_str):
                # remove all non-digits from string
                billion_str = re.sub(r'[^\d.]+', '', billion_str)
                billion_str = billion_str.rstrip(".")
                try:
                    billion_str = float(billion_str)/1000000000
                    billion_str = str(billion_str).rstrip("0").rstrip(".") + 'B'
                    billion_str = re.sub(r'\s*', '', billion_str)
                    # truncate X.xxxxxxxxxx...B to X.xxx
                    billion_str = self.truncateDecimalsToThreeDigits(billion_str)
                except Exception as err:
                    try:
                        logging.info("in handleBillionExpression() conversion to float(x) falied:\n{0}\n ".format(err))
                    except Exception:
                        pass
                return billion_str
            # 4,321,567,892.3333333 (comma delimited number)
            elif re.search(r'(?<!\.)( [\d],([\d]{3},){2,}[\d]{3}\.*[\d]*)|(?<!\.)([\d],([\d]{3},){2,}[\d]{3}\.*[\d]*)', billion_str):
                # remove all non-digits from string
                billion_str = re.sub(r'[^\d.]+', '', billion_str)
                billion_str = billion_str.rstrip(".")
                try:
                    # convert to float
                    billion_str = float(billion_str)/1000000000
                    billion_str = str(billion_str).rstrip("0").rstrip(".") + 'B'
                    #truncate X.xxxxxxxxxx...B to X.xxx
                    billion_str = self.truncateDecimalsToThreeDigits(billion_str)
                except Exception as err:
                    try:
                        logging.info("in handleBillionExpression() conversion to float(x) falied:\n{0}\n ".format(err))
                    except Exception:
                        pass
                return billion_str
            return billion_str
        except Exception as err:
            logging.info("handleBillionExpression() threw this error:\n {0}".format(err))
        return ''


    def truncateDecimalsToThreeDigits(self, rational_number_str):
        try:
            #if regex saw a . and immediately after 4 digits or more -> truncate to 3 digits
            spliited = re.split(r'\.[0-9]{3,}',rational_number_str)
            if len(spliited) == 1:
                return rational_number_str
            else:
                #get index of first digit after the decimal point
                decimal_idx = rational_number_str.find('.') + 1
                #get index of the letter K/B/M in the string
                K_idx = len(rational_number_str) - 1
                try:
                    #case 1: the number is less than 1000 -> truncate digits after the decimal point (NO K/M/B)
                    letter = int(rational_number_str[K_idx])
                    new_string = rational_number_str[:decimal_idx+3]
                    return new_string
                except:
                    #case 2: the number is greater than 1000 -> truncate digits after the decimal point + add the K/M/B letter
                    new_string = rational_number_str[:decimal_idx+3].rstrip("0").rstrip(".")
                    new_string += rational_number_str[K_idx]
                    return new_string
        except Exception as err:
            logging.info("truncateDecimalsToThreeDigits() threw this error:\n {0}".format(err))
        return ''



    def remove_stop_words(self, tokens_list):
        try:
            # remove stopwords
            #tokens_list = tokens_list.split()
            text_tokens_without_stopwords = [w for w in tokens_list if w.lower() not in self.stop_words]
            #o = ' '.join(text_tokens_without_stopwords)
            return text_tokens_without_stopwords
        except Exception as err:
            logging.info("remove_stop_words() threw this error:\n {0}".format(err))
        return tokens_list


    def concatFractions(self, tokens_list):
        try:
            for i, tkn in enumerate(tokens_list):
                #if "/" in tkn and len(tokens_list) > 1 and len(tkn) >= 3 and tkn[0].isnumeric() and tkn[len(tkn)-1].isnumeric:
                if "/" in tkn:
                    #forward_slash_indx = tkn.find("/")
                    index1 = i
                    #index2 = forward_slash_indx+1
                    list_of_denom_and_num = tkn.split("/")
                    numerator = list_of_denom_and_num[0]
                    denomenator = list_of_denom_and_num[1]
                    if not numerator.isnumeric() or not denomenator.isnumeric() and index1+1 < len(tokens_list):
                        tokens_list.pop(i)
                        tokens_list.insert(index1, numerator)
                        tokens_list.insert(index1 + 1 , denomenator)
                    elif i-1 >= 0 and tokens_list[i-1].isnumeric():
                        frac_part = tokens_list.pop(i)
                        int_part = tokens_list.pop(i-1)
                        int_plus_frac_num = int_part + " " + frac_part
                        tokens_list.insert(i-1, int_plus_frac_num)
                    elif index1+1 < len(tokens_list):
                        tokens_list.pop(i)
                        tokens_list.insert(index1, numerator)
                        tokens_list.insert(index1 + 1 , denomenator)
                    elif not numerator.isnumeric() or not denomenator.isnumeric() and index1+1 == len(tokens_list):
                        tokens_list.pop(i)
                        tokens_list.append(numerator)
                        tokens_list.append(denomenator)
            tokens_list = [tkn for tkn in tokens_list if tkn != '']
            return tokens_list
        except Exception as err:
            logging.info("concatFractions threw this error:\n {0}".format(err))
        return tokens_list

    def remove_cut_last_word(self, sentence):
        sen_list = sentence.split()
        last_word_in_tweet = sen_list[len(sen_list) - 1]
        if re.search(r'[\.]{2,}', sentence):
            sentence = ' '.join(sen_list[:-1])
        return sentence

    def toLower(self, sentence):
        sentence = sentence.casefold()
        return sentence


    ################################       CUSTOM RULES       ######################################################
    def unicodeToAscii(self, sentence):
        '''
        Custom Rule #1 - standardize non-ASCII (i.e unicode that aren't ASCII) characters to ASCII representation
        for example: O'Néàl --> O'Neal
        '''
        all_letters = string.ascii_letters + string.digits + " #@%./"
        return ''.join(
            c for c in unicodedata.normalize('NFD', sentence)
            if unicodedata.category(c) != 'Mn'
            and c in all_letters
        ) # type : str

    #todo test this function + find 2 tweets it actually affected for REPORT
    def removePunctuation(self, sentence):
        ######## !!WARNING!! - this function will DESTROY urls. #########
        # not ideal func as it doesn't remove "junky" '@' and '#' and '%' occurences,
        # plus it does remove '//:' (hopefully there won't be a url in full_text when calling this function)
        string_punctuation = """!"$&'()*+,-:;<=>?[\]^_`{|}~"""
        return sentence.translate(str.maketrans(string_punctuation, ' '*len(string_punctuation)))

    def remove_periods(self, sentence):
        #remove any period that isn't followed by a number
        sentence = re.sub(r'\.(?!\d)', '', sentence)
        sentence = re.sub(r'[\w]+…|…', '', sentence)
        return sentence

    def remove_junky_tags_percents_hashtags_etc(self, sentence):
        sentence = re.sub(r'[^\w ]{2,}', '', sentence)
        return sentence

    # \\b in regex == avoiding partial matches
    def remove_single_char_words(self, sentence):
        sentence = re.sub(r'\b[a-zA-Z@#%]\b', '', sentence)
        return sentence

    def remove_forward_slash_separating_two_character_words(self, sentence):
        sentence = re.sub(r'(?<!\d)\/(?!\d)', ' ', sentence)
        return sentence


    def normalizeCoronaExpressions(self, sentence):
        out = re.sub(r'(?<!#)[Cc]+[Oo]*[Rr]+[oO]*[nN]+[aA]+[vV]+[iI]+[rR]+[uU]+[sS]+[Ee]*[sS]*','COVID ',sentence)
        out = re.sub(r'(?<!#)[Cc]+[Oo]+[Vv]+[Ii]+[Dd]+[-]*[19]*','COVID ',out)
        out = re.sub(r'(?<!#)[Cc]+[Oo]*[Vv]+[Ii]*[Dd]+[-]*[19]*','COVID ',out)
        pattern = r'(?<!#)COVID-19|(?<!#)coronavirus|(?<!#)COVID19|(?<!#)Coronavirus|(?<!#)Covid-19|(?<!#)Covid19|(?<!#)corona virus|(?<!#)virus|(?<!#)Coronaviruses|(?<!#)covid19|(?<!#)covid|(?<!#)covd'
        out = re.sub(pattern,'COVID ',out)

        return out

    def remove_longer_than_22_chars_words(self, sentence):
        pattern = re.escape(r'[!"$#%@&\'()*+,-:;<=>?[\]^_`{|}~a-zA-Z\\]{22,}')
        sentence = re.sub(pattern, '', sentence)
        return sentence

    ################################    END CUSTOM RULES    ######################################################

    ################################        GETTERS          #########################################################
    # def get_potential_entities_dictionary(self):
    #     return self.entitiesCandidates_dict
    #
    # def get_upper_lower_case_letters_dictionary(self):
    #     return self.UpperLower_dict

    # def get_all_documents_dictionary(self):
    #     return self.documents_dict
    ################################       END Of GETTERS      #########################################################

    def parse_sentence(self, sentence, doStemming = False):
        """
        This function applies all the parsing rules (tokenize, remove stop words and apply lower case for every word within the text)
        :param sentence: str
        :return: parsed_sentence_list
        """
        try:

            sentence = self.remove_newlines_from_sentence(sentence)
            # Remove all urls from full_text (Handle them in parse_doc)
            sentence = self.removeURLfromString(sentence)
            ### HASHTAG #: [#stay_at_home OR #stayAtHome] -> [stay, at, home, #stayathome]
            sentence = self.catch_hashtags(sentence)
            sentence = self.toLower(sentence)
            sentence = self.remove_periods(sentence)
            # <6%>  <10.6 percent>  <10.6 percentage>
            # Number percent OR Number percentage -> %Number
            sentence = self.handlePercentExpression(sentence)
            # Numbers without units (number without any non-number char attached)
            sentence = self.handleNumberExpressions(sentence)
            # CUSTOM RULE 2: unify COVID
            sentence = self.normalizeCoronaExpressions(sentence)
            # CUSTOM RULE 1:  maybe all puctuation that isn't necessary
            sentence = self.removePunctuation(sentence)
            sentence = self.unicodeToAscii(sentence)
            sentence = self.remove_forward_slash_separating_two_character_words(sentence)
            sentence = self.remove_junky_tags_percents_hashtags_etc(sentence)
            #remove words that are only 1 character
            sentence = self.remove_single_char_words(sentence)
            # Remove words that are longer than 22 letters (I got a good explanation why, we'll write in the report)
            sentence = self.remove_longer_than_22_chars_words(sentence)

            #find potential entities
            #self.findEntitiesCandidates(sentence)
            # lower/upper case words
            #self.findCandidatesForUpperLowerCase(sentence)
            #########################   Break 'sentence' into tokens:   ###################################
            tokens_list = sentence.split()
            ### @ TAG: keep @personName (don't loose the @)
            ### (apply the rule: '35 1/4' is a single token
            tokens_list = self.concatFractions(tokens_list)
            ### remove stopwords
            tokens_list = self.remove_stop_words(tokens_list)
            ############################### Do/Don't Do stemming ##########################################
            # if toStem == True -> call Stemmer() on every token
            if doStemming:
                stemmer = Stemmer() # I tried not to fuck up the Upper Case words while stemming, hopefully it worked
                tokens_list = [stemmer.stem_term(tkn) for tkn in tokens_list]
                return tokens_list
            return tokens_list
        except Exception as err:
            logging.info("parser_module.parse_sentence() threw the following Exception:\n{0} \n".format(err))
        return []


    def handle_urls(self, urls_dict):
        # if url_dict not empty -> take only the long_url and tokenize separately from full_text
        if isinstance(urls_dict, dict):
            tokenized_urls_to_ret = []
            if urls_dict != {}:
                for url_key in urls_dict.keys():
                    try:
                        long_url = urls_dict[url_key]
                        tokenized_url = self.parse_url(long_url)

                        tokenized_urls_to_ret += tokenized_url
                    except Exception as err:
                        logging.info("parser_module.handle_urls() threw the following Exception:\n{0} \nprobably when trying to send long_url to parse_url ".format(err))
                #todo filter out junk tokens
            return tokenized_urls_to_ret
        return []

    def createClosestTermsDict(self, term, tokens_list):
        try:
            closest_terms_dict = {}
            for i, trm in enumerate(tokens_list):
                if trm == term and len(tokens_list) > i+1:
                    consecutive_term = tokens_list[i+1]
                    if consecutive_term in closest_terms_dict.keys():
                        closest_terms_dict[consecutive_term] += 1
                    else:
                        closest_terms_dict[consecutive_term] = 1
            closest_as_list = sorted(closest_terms_dict.items(), key=lambda item: item[1], reverse=True)
            closest_as_list = closest_as_list[:5]
            closest_terms_dict = dict(closest_as_list)
            return closest_terms_dict
        except Exception as err:
            logging.info("createClosestTermsDict() threw error:\n{0}\n ".format(err))
        return {}


    # !!! DO NOT CHANGE parse_doc() SIGNATURE (except for kwargs, that's ok) !!!
    def parse_doc(self, doc_as_list, doStemming=False):
        """
        This function takes a tweet document as list and break it into different fields
        :param doStemming: boolean, if True - do stemming, else don't.
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        try:
            # !!! DO NOT CHANGE FUNC SIGNATURE  (except for kwargs with default values, that's ok) !!!
            ################################################
            # for each Document object - this is the info we get from parser:
            #   1. tweet_id
            #   2. tweet_date
            #   3. full_text
            #   4. urls_dict
            #   5. all_terms_TF_dictionary
            #   6. doc_length
            #   7. num_of_Unique_Terms
            #   8. mostFreqTerm = (theTerm, TF)
            #   9. Terms_in_Tweet_dict = {term1: TermInDoc1(term1, tweet_id, TF1, positions_in_doc1, closest_terms_dict1),
            #                             term2: TermInDoc2(term2, tweet_id, TF2, positions_in_doc2, closest_terms_dict2),
            #                             term3: TermInDoc3(term3, tweet_id, TF3, positions_in_doc3, closest_terms_dict3),
            #                             .......}
            ################################################
            tweet_id = doc_as_list[0]
            tweet_date = doc_as_list[1]
            full_text = doc_as_list[2]
            urls_dict = dict()
            try:
                urls_dict = eval(doc_as_list[3]) # convert urls to dict
                tokenized_urls = self.handle_urls(urls_dict)
            except Exception:
                pass
            tokenized_text = self.parse_sentence(full_text, doStemming)
            doc_length = len(tokenized_text)  # after text operations.
            #########################################################################################
            #count num of occurrences for each Term_i in Doc
            all_terms_TF_dict = {} #num of occurrences of each Term_i in this Doc
            for term in tokenized_text:
                if term not in all_terms_TF_dict.keys():
                    all_terms_TF_dict[term] = 1
                else:
                    all_terms_TF_dict[term] += 1
            #_mostFreqTerm = (mostFreqTerm(string), TF(int) )
            mostFreqTerm = sorted(all_terms_TF_dict.items(), key=lambda item: item[1], reverse=True)
            try:
                if len(mostFreqTerm) > 1:
                    mostFreqTerm = mostFreqTerm[0][1]
                else:
                    mostFreqTerm = 3
            except Exception:
                pass
            #########################################################################################
            #_uniqueTerms in the Doc
            uniqueTerms = len(all_terms_TF_dict.keys())
            #########################################################################################
            Terms_in_Tweet_dict = {}
            for i,term in enumerate(tokenized_text):

                if ' ' in term:
                    term = term.replace(' ','~')


                # (_term, _tweet_id, _TF, _position_in_doc, closest_terms_dict):
                TF = all_terms_TF_dict.get(term, 0)
                if term not in Terms_in_Tweet_dict.keys():
                    termInDoc_obj = TermInDoc(_tweet_id=tweet_id, _TF=TF, _positions_in_doc=[i])
                    closest_terms_dict = self.createClosestTermsDict(term, tokenized_text)
                    termInDoc_obj.setClosestTermsDict(closest_terms_dict)
                    # insert key=term; value=TermInDoc to Terms_in_Tweet_dict:
                    Terms_in_Tweet_dict[term] = termInDoc_obj
                else:
                    existing_term_obj = Terms_in_Tweet_dict[term]
                    try:
                        existing_term_obj.positions_in_doc += [i]
                    except Exception as err:
                        logging.info("parse_doc() threw error while trying to access TermInDoc from Terms_in_Tweet_dict dictionary:\n{0}\n ".format(err))
            #########################################################################################
            document = Document(_tweet_id=tweet_id, _tweet_date=tweet_date, _full_text=full_text, _urls_dict=urls_dict,
                                _doc_length=doc_length, _uniqueTerms=uniqueTerms, _mostFreqTerm=mostFreqTerm, _Terms_in_Tweet_dict=Terms_in_Tweet_dict)
            #########################################################################################
            # # insert this Tweet to the document_dictionary
            # if tweet_id not in self.documents_dict.keys():
            #     # KEY=Tweet_Id ; VALUE=(uniqueTerms, mostFreqTerm, doc_length)
            #     self.documents_dict[tweet_id] = (uniqueTerms, mostFreqTerm[1],  doc_length)
            return document
            #########################################################################################
        except Exception as err:
            print(err)
            logging.info("parse_doc() threw the following Exception:\n{0} ".format(err))
        return None



if __name__ == "__main__":
    reader = Reader('D:\IdeaProjects\SISE\IR_course\Data')
    df = reader.read_50_random_samples_from_all_parquets()
    parser = Parse()
    # i want sublist[2] for each list
    df_as_list = df.values.tolist()
    url_dict = "{'https://moodle2.bgu.ac.il/moodle/course/view.php?id=34132':'https://moodle2.bgu.ac.il/moodle/course/view.php?id=34132'}"
    #df_as_list = [["123","1.1.2000","COVID 19/word  COVID WOrd/19  19 WoRd/covid  34 1/2 COVID 19/20  W1 19/  W2 /34   W3 W4/   W5 /W6  12/13",url_dict]]
    only_full_texts = list(zip(*df_as_list))[2]

    ####  get Document objects after parsing (this is what we send to Indexer):  ####
    documents_list = []
    for doc_as_list in df_as_list:
        tmp_document_object = parser.parse_doc(doc_as_list=doc_as_list)
        documents_list.append(tmp_document_object)
        print("--> For the following Tweet(Document):\n","\"", tmp_document_object.full_text,"\"")
        print("--> Most Frequent Term TF:\n","\"", tmp_document_object.mostFreqTerm,"\"")
        print("\n-->This is the Terms_in_Tweet_dict:\n ")
        for key, val in  tmp_document_object.Terms_in_Tweet_dict.items():
            print(key, ":", end=' ')
            print(val, "\n")
        print("---------------------------------------------------------------------------------------------------------")
    #
    # print("-->NOW PRINTING ALL Parser()'s Dictionaries [Use In Indexer]:\n ")
    # print("all_documents_dictionary",parser.get_all_documents_dictionary(),"\n")
    # #
    #print(" ################################# SHOWING ONLY TOKENIZED FULL_TEXT BELOW:  ################################### ")
    # tokens_lists = []
    # for text in only_full_texts:
    #     tmp_tokenized_full_text = parser.parse_sentence(text)
    #     tokens_lists.append(tmp_tokenized_full_text)
    # ####  printing only the tokenized full_text  #####
    # l = list(zip(only_full_texts, tokens_lists))
    # for txt, lst in l:
    #     print(txt)
    #     print(lst)
    #     print("---------------------------------------------------------------------------------------------------------")





