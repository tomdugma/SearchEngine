from nltk.stem.porter import PorterStemmer
import logging

class Stemmer:
    def __init__(self):
        self.stemmer = PorterStemmer()

    #['performing', 'Maker', 'USA', 'computer', 'Conference', 'NIPS']
    #['perform', 'Make', 'USA', 'comput', 'Confer', 'NIPS']
    def stem_term(self, token):
        """
        This function stem a token
        :param token: string of a single token
        :return: stemmed token
        """
        try:
            # Entity (ex: token == "Donald Trump")
            if token == token.title():#returns a string with first letter of each word capitalized
                token = self.stemmer.stem(token).capitalize()

            # Lower/Upper letters (ex: Max -> MAX;  NBA -> NBA)
            elif token[0].isupper(): #True if first letter is Upper-case
                token = self.stemmer.stem(token).upper()#converts ALL lowercase characters in a string into uppercase
            # all lower-case token
            else:
                token = self.stemmer.stem(token)
            return token   ## use at tokenizing stage : [stemmer(token) for token in tokenize(sentence)]
        except Exception as err:
            logging.basicConfig(filename='search_engine.log', filemode='a') #todo check how to this once and not every time I log something
            logging.info("stemmer.stem_term(token) threw the following Exception:\n{0} ".format(err))

