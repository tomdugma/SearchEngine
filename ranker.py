import linecache
import math
import os


class Ranker:
    def __init__(self):
        pass



    @staticmethod
    def rank_relevant_doc(relevant_doc, query_length, idx):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        result = {}
        for term in relevant_doc.keys():
            for TID in relevant_doc[term]:
                curr_TF = TID.TF
                match_id = int(TID.tweet_id)

                for docs_id in os.listdir("doc_info"):
                    min_id = int(docs_id[0:19])
                    max_id = int(docs_id[20:39])
                    if min_id <= match_id <= max_id:
                        with open("doc_info\\"+docs_id, 'r') as f:
                            try:
                                most_freq_term,coef = Ranker.binary_search_files(f.name,1,1000,match_id)
                                score = ((int(TID.TF) / int(most_freq_term)) * (math.log(10000000 / len(relevant_doc[term]) ,2))) / math.sqrt(query_length * float(coef))
                                result[match_id] = score
                            except Exception as e:
                                print(e)
        return sorted(result.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]

    @staticmethod
    def binary_search_files(file, l, r, target):
        # Check base case
        if r >= l:
            mid = l + (r - l) // 2
            curr_line = int(linecache.getline(file, mid).rstrip().split()[0])
            # If element is present at the middle itself
            if curr_line == target:
                data = linecache.getline(file, mid).rstrip().split()
                mft = data[1][1:2]
                # unique_terms = data[2][0:len(data[2])-1]
                coef = data[3][0:len(data[3]) - 1]
                return mft, coef
                # If element is smaller than mid, then it
            # can only be present in left subarray
            elif curr_line > target:
                return Ranker.binary_search_files(file, l, mid - 1, target)

                # Else the element can only be present
            # in right subarray
            else:
                return Ranker.binary_search_files(file, mid + 1, r, target)

        else:
            # Element is not present in the array
            return -1