class SearchEngineVariablesService:

    columns_to_index = ['title', 'body', 'comments']
    avg_len_suffix='_avg_len'

    def __init__(self, indexer_db):
        self.variables_collection = indexer_db.variables

    def get_avarages_variables(self):
        return self.get_variables(list(map(lambda column_name: column_name+SearchEngineVariablesService.avg_len_suffix, SearchEngineVariablesService.columns_to_index)))

    def get_variables(self, variable_names):
        result = {}

        for variable_name in variable_names:
            avg_variable = None
            avg_variable_cursor = self.variables_collection.find_one({
                "_id": variable_name
            })
            if avg_variable_cursor == None:
                avg_variable = 0
            else:
                avg_variable = avg_variable_cursor['value']

            result[variable_name] = avg_variable

        return result

    def set_variables(self, variables_dict):
        for variableName in list(variables_dict.keys()):
            self.variables_collection.update_one({
                "_id": variableName
            }, {
                "$set": {
                    "value": variables_dict[variableName]
                }
            },
                upsert=True
            )
