
import pandas as pd


keymaster = {

    "file_C": "ID"
}


prereqMaster = {
    "RANGO_IVE": ["file_C#RBD", "IVE#IVE"]


}

def rangeIVE():

    return True

lambdaMaster = {
    "RANGO_IVE": rangeIVE


}

class spanda:

    def __init__(self, anhos, Basefile=''):

        self.anhos = anhos
        self.active_year = anhos[0]
        self.dictpandas = {}

        for an in self.anhos:
            if Basefile > '':
                aux = ut.pull(Basefile, an)
            else:
                aux = pd.DataFrame()
            self.dictpandas.update({ an: aux for an in anhos})


    def callfield(self, item, reload = False):

        if (not item is self.mainPanda.columns.values) or reload:

            if "#" in item:

                parts = item.split("#")
                origFile = parts[0]
                field = parts[1]

                for an in self.anhos:
                    aux = ut.pull(origFile, an)
                    pandas = self.dictpandas[an]

                    pandas = pd.merge(pandas, aux, on=keymaster[origFile], how='left', suffixes=('', origFile + "#"))

                    self.dictpandas[an] = pandas

    def callLambda(self, item, reload = False):

        if (not item is self.mainPanda.columns.values) or reload:

            if "@" in item:

                parts = item.split("@")
                field = parts[1]

                prereq = prereqMaster[field]

                for prq in prereq:
                    self.callitem(prq)

                for an in self.anhos:
                    pandas = self.dictpandas[an]
                    pandas = pd.apply( lambdaMaster[field], axis=1)
                    self.dictpandas[an] = pandas


    def callitem(self, item):

        if not item is self.mainPanda.columns.values:

            if "#" in item:
                self.callfield(item)

            if "@" in item:
                self.callLambda(item)

    def reloaditem(self, item):

        if not item is self.mainPanda.columns.values:

            if "#" in item:
                self.callfield(item, reload = True)

            if "@" in item:
                self.callLambda(item, reload = True)


    def __getitem__(self, item):

        if not item is self.mainPanda.columns.values:

            if "#" in item:
                self.callfield(item)

            if "@" in item:
                self.callLambda(item)

        return self.dictpandas[self.active_year][item]

    def set_year(self, an):
        self.active_year = an

    def save_as(self, Basefile):

        for an in self.anhos:
            aux = self.dictpandas[an]
            ut.push(Basefile, an, aux)

    def showme(self):

        print "anhos: "+str(self.anhos)
        print "dictpandas keys: "+str(sorted(self.dictpandas.keys()))
        print "active_year: " + str(self.active_year)
        print "imprimiendo columnas"
        for an in self.anhos:
            print "columnas anho "+str(an)
            print self.dictpandas[an].columns.values

