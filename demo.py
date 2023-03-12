#%%
from db.MongoDB import MongoDB
db = MongoDB()
#%% 
page_cursor = db.get_indexed_pages_by_token('sunday', skip=0, limit=1)
for page in page_cursor:
    print(page)
    break
#%%
from abc import ABC,abstractmethod

class one(ABC):
    @abstractmethod
    def move(self):
        pass
class child(one):
    def move(self):
        print('move')
obj_one=child()
obj_one.move()