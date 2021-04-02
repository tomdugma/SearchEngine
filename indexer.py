import os
import numpy as np
import sortedcontainers.sorteddict
import pandas as pd


class Indexer:

    def __init__(self, config):
        self.inverted_idx = sortedcontainers.SortedDict()  ## REPLACED WITH REGULAR DICTIOANRY CHECK IF OK !?
        self.postingDict = sortedcontainers.SortedDict()
        self.config = config
        self.posting_counter = 0
        self.my_path_to_posting_files = "posting_folder"
        self.my_path_to_merged_posting_files = "posting_folder_merged"
        self.BATCH_SIZE = 1000
        self.MERGED_BATCH_SIZE = 1000
        self.number_of_documents_in_corpus = 0

    def get_inverted_idx(self):
        return self.inverted_idx

    def add_new_doc(self, document,output_path, is_last=False, last_para=False):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        if document != None:

            document_dictionary = document.Terms_in_Tweet_dict
            # Go over each term in the doc
            if not os.path.exists(output_path) and output_path is not None:
                os.makedirs(output_path)

            new_doc = True
            for term in document_dictionary.keys():
                try:

                    if len(self.postingDict) == self.BATCH_SIZE:
                        f = open(output_path + "\\" + str(self.posting_counter) + ".txt", 'w')
                        for key, val in self.postingDict.items():
                            f.write(key + " " + str(val) + "\n")
                        f.close()

                        self.postingDict.clear()
                        self.posting_counter += 1
                    if term not in self.inverted_idx.keys():  # if its not in inverted, than its 100% sure not in posting dict
                        # [in how much tweets appears, corpus_freq, pointer_to_inverted_index_file, pointer_to_specific_location_in_tweet]
                        # pointers will be updated at the end after merging.
                        self.inverted_idx[term] = [1, 1, None, None, None]
                        self.postingDict[term] = [document_dictionary[term].__str__()]

                    else:
                        if new_doc:  # only if its the first time we met this term IN THIS CURRENT DOCUMENT !, we do++
                            self.inverted_idx[term][0] += 1
                            new_doc = False
                        self.inverted_idx[term][1] += 1
                        if term in self.postingDict:
                            self.postingDict[term] += ['&&'] + [document_dictionary[term].__str__()]

                        else:

                            self.postingDict[term] = [document_dictionary[term].__str__()]
                except Exception as e:
                    print(e)

            if is_last and last_para:
                f = open(output_path + "\\" + str(self.posting_counter) + ".txt", 'w')
                for key, val in self.postingDict.items():
                    f.write(key + " " + str(val) + "\n")
                f.close()

    def merge(self,tmp_posting_path):

        updated_path = "merged_posting_files"
        if not os.path.exists(updated_path):
            os.makedirs("merged_posting_files")

        self.posting_counter = 0
        position_in_merged_file = 1

        new_tmp_sorted_dic_terms = sortedcontainers.SortedDict()
        merged_posting_file = sortedcontainers.SortedDict()
        curr_merged_file_path = updated_path + "\\" + str(self.posting_counter) + ".txt"
        open_files = os.listdir(tmp_posting_path)
        open_files_list = [(open(tmp_posting_path + "\\" + file, 'r', encoding='utf8')) for file in
                           open_files]  # open all posting files we created.
        files_object_hash_map = {}
        ############################
        # GETS FIRST TERM IN EACH FORMER POSTING FILE
        ############################
        self.get_first_word_from_all_files(open_files_list, files_object_hash_map, new_tmp_sorted_dic_terms)
        while len(new_tmp_sorted_dic_terms) > 0:  # c = amount of terms reading each iteration.
            items = new_tmp_sorted_dic_terms.items()  # get all keys from sorted dict
            curr_key = items[0][0]  # get "minimum" key.

            if curr_key not in merged_posting_file:
                try:
                    self.inverted_idx[curr_key][2] = curr_merged_file_path
                    self.inverted_idx[curr_key][3] = position_in_merged_file
                    value = str(items[0][1][0:]).replace(',', '')
                    position_in_merged_file += 1
                    merged_posting_file[curr_key] = value  # add to new posting file
                except Exception as e:
                    # print("issue occurde in merging")
                    a = 1
                    # print(e)
            else:
                merged_posting_file[curr_key] += "[" + value + "]"

            new_tmp_sorted_dic_terms.pop(curr_key)  # remove key from dictionary.
            next_term = []
            for i in range(len(files_object_hash_map[curr_key])):
                next_term.append(files_object_hash_map[curr_key][i].readline().split(' '))

                if next_term[i][0] != '':
                    if next_term[i][0] not in new_tmp_sorted_dic_terms:
                        files_object_hash_map[next_term[i][0]] = [files_object_hash_map[curr_key][i]]
                        new_tmp_sorted_dic_terms[next_term[i][0]] = next_term[i][1:]
                    else:
                        files_object_hash_map[next_term[i][0]].append(files_object_hash_map[curr_key][i])
                        new_tmp_sorted_dic_terms[next_term[i][0]] += ['&&'] + next_term[i][1:]
                else:  # END OF FILE !
                    open_files_list.remove(files_object_hash_map[curr_key][i])
                    files_object_hash_map[curr_key][i].close()
                    if len(open_files_list) == 0:  # IF WE WENT OVER ALL THE POSTING FILES, FINISH WITH TMP AND DONE.
                        self.finish_last_merged_file(new_tmp_sorted_dic_terms, merged_posting_file)
                        break
            files_object_hash_map.pop(curr_key)  # we finish with the curr file, remove it.
            if len(merged_posting_file) == self.MERGED_BATCH_SIZE:
                with open(curr_merged_file_path, 'w') as f:
                    for key, val in merged_posting_file.items():
                        f.write(key + " " + str(val) + "\n")
                f.close()
                self.posting_counter += 1
                curr_merged_file_path = updated_path + "\\" + str(self.posting_counter) + ".txt"
                merged_posting_file.clear()  # cleans up the curr merge dict
                position_in_merged_file = 1
        ####################################################################################################
        # We went out of the "big" while loop, means we finish with the tmp dict and there is still
        # "left overs" at the last merging file, so flush him too.
        ####################################################################################################
        print("*********LAST MERGING FILE*********")
        f = open(curr_merged_file_path, 'w')
        for key, val in merged_posting_file.items():
            f.write(key + " " + str(val) + "\n")
        f.close()


    ##################################################################
    # in case we ran out of files, just finish tmp dict (its the last merging file we will create)
    ##################################################################
    def finish_last_merged_file(self, tmp_dict, merge_dict):
        while len(tmp_dict) != 0:  # means we are at the last posting file, might be over 1000 terms.
            items = tmp_dict.items()  # get all keys from sorted dict
            curr_key = items[0][0]  # get "minimum" key.
            tmp_dict.pop(curr_key)  # remove key from dictionary.
            if curr_key not in merge_dict:
                value = str(items[0][1]).replace(',', '')
                merge_dict[curr_key] = value  # add to new posting file
            else:
                merge_dict[curr_key] += "[" + value + "]"

    ##################################################################
    # FIRST ITERATION, FILL THE TMP DICT WITH FIRST TERM FROM ALL POSTING FILES.
    ##################################################################
    def get_first_word_from_all_files(self, open_files_list, hash_map_pointers, tmp_dic):
        first_line_list = [file.readline().split(' ') for file in open_files_list]  # gets first line from all files.

        for i in range(len(open_files_list)):
            if first_line_list[i][0] not in tmp_dic:
                hash_map_pointers[first_line_list[i][0]] = [
                    open_files_list[i]]  # key = term, value = file object belong to the specific term.
                tmp_dic[first_line_list[i][0]] = first_line_list[i][
                                                 1:]  # if its not in, add new key and its data from posting file.
            else:
                hash_map_pointers[first_line_list[i][0]].append(open_files_list[i])
                tmp_dic[first_line_list[i][0]] += ['&&'] + [
                    first_line_list[i][1:]]  # if its aleardy there, just append the data from the posting file.
