"""
bisogna verificare come si definiscono i diversi [0], [0..1], [1..*], ...
"""

class IdentifiableEntity(object):
    def __init__(self, identifiers):
        self.id = set()
        for identifier in identifiers:
            self.id.append(identifier) # string[1..*]
    
    def getIds(self): # list[string]
        result = []
        for i in id:
            result.append(i)
        result.sort()
        return result 

class Journal(IdentifiableEntity):
    def __init__(self, id, title, languages, publisher, seal, license, apc, hasCategory, hasArea):
        super().__init__(id)
        self.title = title # string[1]
        self.languages = set()
        for language in languages:
            self.languages.append(language) # string[1..*]
        if publisher:
            self.publisher = publisher # string[0..1]
        self.seal = seal # boolean[1]
        self.license = license # string[1]
        self.apc = apc # boolean[1]
        if hasCategory:
            self.hasCategory = hasCategory # 0..*
        if hasArea:
            self.hasArea = hasArea # 0..*
    
    def getTitle(self):
        return str(self.title) # string
    
    def getLanguages(self): 
        result = []
        for language in self.languages:
            result.append(language)
        result.sort()
        return result # list[string]

    def getPublisher(self):
        if self.publisher:
            return self.publisher 
        else:
            return None # string or None 
    
    def hasDOAJSeal(self):
        if self.seal == True:
            return True
        else:   
            return False # boolean
    
    def getLicence(self):
        return str(self.license) # string
    
    def hasAPC(self):
        if self.apc == True:
            return True
        else:   
            return False # boolean
        
    def getCategories(self):
        result = []
        if self.hasCategory:
            result.append(self.hasCategory)
            return result # list[Category]

    def getAreas(self):
        result = []
        if self.hasArea:
            result.append(self.hasArea)
            return result # list[Area]
               
class Category(IdentifiableEntity):
    def __init__(self, id, quartile):
        super().__init__(id)
        if quartile:
            self.quartile = quartile  # string[0..1]
        
    def getQuartile(self):
        if self.quartile:
            return self.quartile  
        else:
            return None # string or None 

class Area(IdentifiableEntity):
    def __init__(self, id):
        super().__init__(id)
    

        


        