
class Document:

    def __init__(self, _tweet_id, _tweet_date=None, _full_text=None, _urls_dict=None, _doc_length=0, _mostFreqTerm=None, _uniqueTerms=None,
                 _Terms_in_Tweet_dict=None,
                 retweet_text=None, retweet_url=None, quote_text=None, quote_url=None):
        """
        :param _tweet_id: tweet id
        :param _tweet_date: tweet date
        :param _full_text: full text as string from tweet
        :param _urls_dict: url
        :param retweet_text: retweet text
        :param retweet_url: retweet url
        :param quote_text: quote text
        :param quote_url: quote url
        :param _all_terms_TF_dictionary: dictionary of term and documents.
        :param _doc_length: doc length
        """
        self.tweet_id = _tweet_id
        self.tweet_date = _tweet_date
        self.full_text = _full_text
        self.url = _urls_dict
        #self.term_doc_dictionary = _all_terms_TF_dictionary
        self.doc_length = _doc_length

        # mostFreqTerm - most freq term in the specific tweet.כ
        self.mostFreqTerm = _mostFreqTerm #-->  tuple: (theTerm(string), TF(int))

        # uniqueTerms - the *AMOUNT* of unique terms in the tweet.
        self.uniqueTerms = _uniqueTerms #--> int

        self.Terms_in_Tweet_dict = _Terms_in_Tweet_dict #-->dictionary: {term1: TermInDoc1, term2: TermInDoc2, .......}

        ##### these will have None value, unless those kwargs be filled with the appropriate text from Data #######
        self.retweet_text = retweet_text
        self.retweet_url = retweet_url
        self.quote_text = quote_text
        self.quote_url = quote_url
        self.cosineCoeffMechane = 0

