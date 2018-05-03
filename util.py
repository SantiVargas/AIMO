import pickle

def list_from_file(filename):
    results = []
    with open(filename) as f:
        for line in f:
            results.append(line)
    return results

def list_to_file(data_list, filename):
    with open(filename, 'w+') as f:
        for x in data_list:
            f.write('%s \n' % x)

def file_pickler(data, filename):
    with open(filename, 'wb+') as f:
        pickler = pickle.Pickler(f, -1)
        pickler.dump(data)