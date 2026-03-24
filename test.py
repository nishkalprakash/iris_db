#%%
from biolab import IrisDB as DB
#%%
db=DB('iris_syn')
#%%
docs = list(db.find({},{'fv':1,'person_id':1,'sample_id':1}))

#%%
print(docs[0])
print(len(docs))
# %%
