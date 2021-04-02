class TermInDoc:

    def __init__(self,_tweet_id, _TF, _positions_in_doc):
        # term - the actual word or term.

        #self.term = _term # --> redundant: the dictionary key 'term' is mapped to THIS TermInDoc instance
        self.tweet_id = _tweet_id # --> redundant: (Document instance that contains this TermInDoc instance *already contains* doc_id at the Doc-level)
        self.TF = _TF #--> int
        self.positions_in_doc = _positions_in_doc #-->list
        # 10 following terms that appeared most times after curr term (after cleaning) (in this term)
        self.closest_terms_dict = dict() #key = word after term ; val = count -> sort by val -> keep top 10

    def setClosestTermsDict(self, _closest_terms_dict):
        self.closest_terms_dict = _closest_terms_dict

    def __str__(self):
        return '['+self.tweet_id+' '+str(self.TF)+' '+str(self.positions_in_doc)+' ' +str(self.closest_terms_dict)+']'