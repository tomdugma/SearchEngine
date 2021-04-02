import csv
import os
import math

# import gensim
import sortedcontainers
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils

# --------------
# MAIN CLASS
# --------------

num_of_tweets = 10000000
doc_dict = sortedcontainers.SortedDict()
DOC_BATCH_SIZE = 1000
# def calculate_num_of_docs(path):
#     res = 0
#     for subdir, dirs, files in os.walk(config.corpusPath):
#         for file in files:
#             if not subdir.endswith('0'):  # if its not parquet folder.
#                 continue
#             if not file.endswith('uet'):  # if its not the parquet file.
#                 continue
#     file_path = os.path.join(subdir, file)
#     documents_list = ReadFile.read_file(file_name=file_path)
#     res += len(documents_list)
#     return res
#

def run_engine(corpus_path, output_path,indexer, stemming):
    """
    :return:
    """

    # config = ConfigClass()
    # config.corpusPath = corpus_path
    # indexer = Indexer(config)
    r = ReadFile(corpus_path=corpus_path)
    p = Parse()
    last_para = False
    number_of_parquets_done = 0
    # print("######## START GOING OVER THE CORPUS ########")
    for subdir, dirs, files in os.walk(corpus_path):
        number_of_parquets_done += 1
        for file in files:
            if not subdir.endswith('0'):  # if its not parquet folder.
                continue
            if not file.endswith('uet'):  # if its not the parquet file.
                continue
            if number_of_parquets_done == 24:
                last_para = True
            file_path = os.path.join(subdir,file)
            documents_list = r.read_file(file_name=file_path)
            is_last_dic = False
            # print("######## file number {} ########".format(number_of_parquets_done))
            for idx, doc in enumerate(documents_list):
                if idx == len(documents_list) - 1:
                    is_last_dic = True
                # parse the document
                parsed_document = p.parse_doc(doc, stemming)
                # index the document data
                indexer.add_new_doc(parsed_document,output_path,is_last_dic,last_para)
        #     if number_of_parquets_done >=5:
        #         break
        # if number_of_parquets_done >= 5:
        #     break

def cossimCoefCalc(corpus_path, indexer,stemming=False,output_path=None): # second run !

    #if no output path was given.
    if output_path is None:
        os.makedirs("doc_info")
        output_path = "doc_info"
    # indexer = Indexer(path)
    number_of_parquets_done = 0
    r = ReadFile(corpus_path=corpus_path)
    p = Parse()
    for subdir, dirs, files in os.walk(corpus_path):
        number_of_parquets_done += 1
        for file in files:
            if not subdir.endswith('0'):  # if its not parquet folder.
                continue
            if not file.endswith('uet'):  # if its not the parquet file.
                continue
            file_path = os.path.join(subdir, file)
            documents_list = r.read_file(file_name=file_path)
            cossimCoef_id = 0
            for idx, doc in enumerate(documents_list):
                parsed_doc = p.parse_doc(doc, stemming)
                document_dictionary = parsed_doc.Terms_in_Tweet_dict
                for term in document_dictionary.keys():
                    if term not in indexer:
                        print("term {} not in idx".format(term))
                        continue
                    term_TF_IDF = (document_dictionary[term].TF / parsed_doc.mostFreqTerm) * (
                        math.log(10000000 / int(indexer[term][0]), 2))
                    cossimCoef_id += math.pow(term_TF_IDF, 2)
                parsed_doc.cosineCoeffMechane = cossimCoef_id
                doc_dict[parsed_doc.tweet_id] = (
                    parsed_doc.mostFreqTerm, parsed_doc.uniqueTerms, parsed_doc.cosineCoeffMechane)
                cossimCoef_id = 0
        #     if number_of_parquets_done >= 5:
        #         break
        # if number_of_parquets_done >= 5:
        #     break
    try:
        counter = 0
        lst = list(doc_dict.items())
        while counter < len(lst):
            try:
                min_id = lst[counter][0]
                max_id = lst[counter + DOC_BATCH_SIZE - 1][0]
                f = open(output_path + "\\" + min_id + "_" + max_id + ".txt", 'w')
                curr_slice = lst[counter:counter + DOC_BATCH_SIZE]
                for key, value in curr_slice:
                    f.write(key + " " + str(value) + "\n")
                    # for key, val in doc_dict.items():
                    #     f.write(key + " " + str(val) + "\n")
                f.close()
                counter += DOC_BATCH_SIZE
                # doc_dict.clear()
            except:
                min_id = lst[counter][0]
                max_id = lst[len(lst) - 1][0]
                curr_slice = lst[counter:len(lst) - 1]
                f = open(output_path + "\\" + min_id + "_" + max_id + ".txt", 'w')
                for key, value in curr_slice:
                    f.write(key + " " + str(value) + "\n")
                f.close()

    except Exception as e:
        print(e)

def merging_and_indexing(indexer,corpus_path):
    print("BEGAN MERGING !!!!!!!!!!!!!!!")
    # config.corpusPath = output_path
    indexer.merge(corpus_path)  # was corpusPath
    print("BEGAN BUILDING INDXERRRRRR !!!!!!!!!!!!!!!")
    # if stemming:
    with open("inverted_index.txt", 'w', encoding="utf8") as f:
        for key, val in indexer.inverted_idx.items():
            f.write(key + " " + str(val) + "\n")

    # else:
    #     with open("inverted_index.txt", 'w', encoding="utf8") as f:
    #         for key, val in indexer.inverted_idx.items():
    #             f.write(key + " " + str(val) + "\n")
    f.close()

    print('Finished parsing and indexing. Starting to export files')
    # if stemming:
    utils.save_obj(indexer.inverted_idx, "inverted_index")
    # else:
    #     utils.save_obj(indexer.inverted_idx, "inverted_index")

    # utils.save_obj(indexer.postingDict, "posting")

    ############################################################
    # THIS WORD 2 VEC BELOW, WAS TRANSFER TO SEARCH PART, BECAUSE ITS TAKING TOO LONG, ALTOUGHT ITS NOT GOOD BECAUSE IT WILL MAKE
    # SEARCHING FOR QUERY RESULTS WAY SLOWER !
    ############################################################
    # with open("inverted_index.txt", 'w', encoding="utf8") as f:
    #     print("loading model")
    #     model = gensim.models.KeyedVectors.load_word2vec_format('GoogleNews-vectors-negative300.bin.gz', binary=True, encoding='utf-8')
    #     print("finishing model")
    #     for key, val in indexer.inverted_idx.items():
    #         tmp = copy(val)
    #         try:
    #             indexer.inverted_idx[key][4] = model.most_similar(key, topn=3)
    #             f.write(key + " " + str(val) + "\n")
    #         except Exception as e:
    #             print('word {} not in voc'.format(key))
                # f.write(key + " " + str(tmp) + "\n")
                # print(e)

    # cossimCoefCalc(indexer.inverted_idx)  # second run on corpus, for cosim similarty calculation.



def load_index(stemming = False):
    print('Load inverted index')
    inverted_index = utils.load_inverted_index()
    return inverted_index


def search_and_rank_query(query, inverted_index,stemming, k):
    p = Parse()

    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list[1:])
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, len(query), inverted_index)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


#todo this is the MAIN function for the automatic tests! make sure the entire engine starts from here!
def main(corpus_path=r'C:\Users\tomdu\Desktop\semester 5\DR\Data\Data',
               output_path="tmp_posting", stemming=False,
         queries=None, num_docs_to_retrieve=200):
    """
    this is the MAIN function for the automatic tests! make sure the entire engine starts from here
    :param corpus_path: the path to corpus
    :param output_path: the path to our engine's output
    :param stemming: boolean, true - do stemming, false - don't do stemming
    :param queries: either a python list of queries (strings) OR a file where each line is a querie, support both options
    :param num_docs_to_retrieve: the number of docs to return for each querie
    :return:
    """
    #####################################################################
    # ENTIRE ENGINE BELOW, TESTED ON 2400 tweets work fine

    # config = ConfigClass()
    # config.corpusPath = corpus_path
    # indexer = Indexer(config)
    # run_engine(corpus_path,output_path,indexer,stemming)
    #
    # after first run on corpus, we have tmp posting, which is our new corpus.
    # config.corpusPath = output_path
    #
    # merging_and_indexing(indexer,corpus_path=config.get__corpusPath())
    inverted_index = load_index(stemming)
    #
    # after finish merging, change back to the original corpus path for the 2nd run
    # config.corpusPath = corpus_path
    # cossimCoefCalc(config.get__corpusPath(),inverted_index)

    # ENTIRE ENGINE ABOVE, TESTED ON 2400 tweets work fine
    #####################################################################

    # check if "queries" is a list or a file
    if isinstance(queries, list):
        for query in queries:
            result = search_and_rank_query(query,inverted_index,num_docs_to_retrieve)
            for elem in result:
                print(elem)
    else: #query is a file
        with open('results.csv', 'w+',newline='') as csv_write:
            query_num = 1
            res = 0
            l = 0
            with open(queries, 'r', encoding="utf8") as f:
                for line in f:
                    print(line)
                    if line == '\n':
                        continue
                    else:
                        result = search_and_rank_query(line, inverted_index, stemming, num_docs_to_retrieve)
                    # else:
                    #     tmp = search_and_rank_query(line, inverted_index, stemming, num_docs_to_retrieve)
                        print("write csv")
                        for row in result:
                            res = [query_num, row[0], row[1]]
                            wr = csv.writer(csv_write, dialect='excel')
                            l += 1
                            wr.writerow(res)
                        # for i in tmp:
                        #     result.append(i)
                        # result.append( search_and_rank_query(line, inverted_index, stemming, num_docs_to_retrieve))
                    query_num += 1
            f.close()
        csv_write.close()
        print(l)
        # with open('results.csv', 'w+') as f:
        #     print("write csv")
        #     for row in result:
        #         res = [row[0], row[1]]
        #         wr = csv.writer(f, dialect='excel')
        #         wr.writerow(res)

    # calculate_num_of_docs(corpus_path)
    # run_engine(corpus_path, output_path, stemming)
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    # inverted_index = load_index()
    # for doc_tuple in search_and_rank_query(query, inverted_index, k):
    #     print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))


if __name__ == '__main__':
    # main()
    # corpus_path = ed"
    # output_path = r"C:\Users\tomdu\Desktop\semester 5\DR\Search_Engine-master\Search_Engine-master\output_folder"
    # stemming = False
    # queries = ['shit job']
    queries = 'queries.txt'
    # num_of_docs = 5
    main(queries=queries)
