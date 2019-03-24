import nltk
from nltk import *
# nltk.download()

# from nltk.corpus import wordnet

# from nltk import wordnet


class StringStemmingRuleProvider:

    _porter_stemmer = nltk.PorterStemmer()
    _snowball_stemmer = nltk.SnowballStemmer('english')
    _n_gram = 4
    # _lemmatizer = wordnet.WordNetLemmatizer()

    @staticmethod
    def porter(str_to_stem):
        return [StringStemmingRuleProvider._porter_stemmer.stem(str_to_stem)]

    @staticmethod
    def snowball(str_to_stem):
        return [StringStemmingRuleProvider._snowball_stemmer.stem(str_to_stem)]

    @staticmethod
    def n_gram_yielder(str_to_stem):
        nr_of_grams = len(str_to_stem)-StringStemmingRuleProvider._n_gram + 1

        for index in range(0, nr_of_grams):
            yield str_to_stem[index: index+StringStemmingRuleProvider._n_gram]

    @staticmethod
    def n_gram(str_to_stem):

        if(len(str_to_stem) <= StringStemmingRuleProvider._n_gram):
            return [str_to_stem]

        return list(StringStemmingRuleProvider.n_gram_yielder(str_to_stem))

    # @staticmethod
    # def lemmatize(str_to_stem):
    #     return StringStemmingRuleProvider._lemmatizer.lemmatize(str_to_stem)
