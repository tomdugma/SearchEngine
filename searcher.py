import linecache
import re

from parser_module import Parse
from ranker import Ranker
from TermInDoc import TermInDoc
import utils


class Searcher:

    def __init__(self, inverted_index,document_dictionary=None):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index
        self.document_dictionary = document_dictionary

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        relevant_docs = {}

        for term in query:
            try: # an example of checks that you have to do
                term_path, term_position_in_file = self.inverted_index[term][2], self.inverted_index[term][3]
                term_path = term_path[2:len(term_path)-1]
                term_position_in_file = int(term_position_in_file[1:len(term_position_in_file)])
                match_line_file = linecache.getline(term_path,term_position_in_file)
                parsed_term = self.parser.parse_sentence(term)
                if '&&' in match_line_file:
                    first = True
                    line_match_to_term = match_line_file.split('&&')
                    for l in line_match_to_term:
                        if first:
                            new_TID = self.term_to_object(l, True)
                            # print(new_TID.tweet_id)
                            # print(new_TID.TF)
                            # print(new_TID.positions_in_doc)
                            # print(new_TID.closest_terms_dict)
                            relevant_docs[term] =[new_TID]
                            first = False
                        else:
                            # print(l)
                            l = "a " + l[3:]  # added garbage so the function term_to_object know where to start from
                            new_TID = self.term_to_object(l)
                            # print(new_TID.tweet_id)
                            # print(new_TID.TF)
                            # print(new_TID.positions_in_doc)
                            # print(new_TID.closest_terms_dict)
                            relevant_docs[term].append(new_TID)
                else:

                    new_TID = self.term_to_object(match_line_file, True)
                    relevant_docs[term] = [new_TID]
            except Exception as e:
                print(e)
                print('term {} not found in posting'.format(term))
        return relevant_docs

    def term_to_object(self,match_line_file, first=False):
        term_dict = {}
        has_dict = True
        if '{}' in match_line_file:
            line_match_to_term = match_line_file.split('{')
            dic_part = line_match_to_term[1].split('}')[0]
            first_part = line_match_to_term[0].split()
            has_dict = False
        else:
            line_match_to_term = match_line_file.split('{')
            dic_part = line_match_to_term[1].split('}')[0]
            first_part = line_match_to_term[0].split()

        # print(line_match_to_term[1])
        if first:
            tweet_id = first_part[1][5:len(first_part[1])-1]
        else:
            tweet_id = first_part[1][3:len(first_part[1])-1]
        term_TF = first_part[2][1:2]
        term_positions_in_tweet = first_part[3]
        term_positions_in_tweet = [c for c in term_positions_in_tweet[2:len(term_positions_in_tweet) - 2]]
        if has_dict:  # if we have any data in the dictionary

            term_closes_dic = dic_part.split(':')
            key, value = term_closes_dic[0].replace("'", ""), term_closes_dic[1][len(term_closes_dic[1]) - 1]
            term_dict[key] = value

        new_TID = TermInDoc(tweet_id, term_TF, term_positions_in_tweet)
        new_TID.setClosestTermsDict(term_dict)
        return new_TID
