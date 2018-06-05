import pandas as pd

def pull(data, year):
    print "test pull, is working"
    store = pd.HDFStore('store_' + str(year) + '.h5')
    print "test pull, is working 1"
    x = store[data]
    print "test pull, is working 2"
    store.close()
    print "test pull, is working 3"
    return x

def push(data, year, dataframe):
    store = pd.HDFStore('store_' + str(year) + '.h5')
    store[data] = dataframe
    store.close()
    print "push completed "+str(data)+" on year "+str(year)