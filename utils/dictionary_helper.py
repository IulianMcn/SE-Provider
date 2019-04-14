
def aggregate_dicts(source, other):

    for key, value in other.items():
        if key not in source:
            source[key] = value
        else:
            source[key]['docs'].update(value['docs'])
