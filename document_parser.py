from rulesProviders.string_split_rule_provider import StringSplitRuleProvider
from bson.objectid import ObjectId
from rulesProviders.string_stemming_rule_provider import StringStemmingRuleProvider
from utils.dictionary_helper import aggregate_dicts
import hashlib


class DocumentParser:

    def parse_document(self, document):
        return DocumentParser.compute_inverse_index(document)

    @staticmethod
    def split_string(str):
        return StringSplitRuleProvider.alpha_numeric_splitting(str)

    @staticmethod
    def compute_inverse_index(document):
        words_to_index = DocumentParser.split_string(document['content'])
        words_count = dict()

        for index, word_to_index_raw in enumerate(words_to_index):
            if(len(word_to_index_raw) <= 1):
                continue

            terms_to_index = StringStemmingRuleProvider.snowball(
                word_to_index_raw.lower())

            for term_to_index in terms_to_index:
                if term_to_index in words_count:
                    words_count[term_to_index].append(index)
                else:
                    words_count[term_to_index] = [index]

        return words_count

    @staticmethod
    def compute_direct_positional_map(document,nr_threads):
        words_count = DocumentParser.compute_inverse_index(document)
        direct_position_map = {}

        for word, word_count in words_count.items():
            hash = DocumentParser.hash(word, nr_threads)

            if hash in direct_position_map:
                direct_position_map[hash]['docs'][document["_id"]].append({
                    "_id": word,
                    "freq": len(word_count),
                    "pos": word_count
                })
            else:
                direct_position_map[hash] = {
                    "_id": hash,
                    "docs": {
                        document["_id"]: [{
                            "_id": word,
                            "freq": len(word_count),
                            "pos": word_count
                        }]
                    }
                }

        return direct_position_map

    @staticmethod
    def hash(st, nr_reduce_threads):  # TODO: move it from here
        return str(int(hashlib.md5(st.encode('utf-8')).hexdigest()[:8], 16) % nr_reduce_threads)
