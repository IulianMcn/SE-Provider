class DictionaryHelper:

    @staticmethod
    def aggregate_dicts(dicts):
        aggregation = dict()

        for idx, dict_to_aggregate in enumerate(dicts):
            for key, value in dict_to_aggregate.items():
                if key not in aggregation:
                    aggregation[key] = [[] for i in range(len(dicts))]
                aggregation[key][idx] = value

        return aggregation
