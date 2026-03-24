# %% Imports
from biolab import *
from datetime import datetime
from pathlib import Path
from pymongo import ASCENDING

DB_BASE_ = Path("~/datasets/iris_db/").expanduser()
DS_ID = "iris_syn"
DS_NAME_ = Path(DS_ID)
DS_BASE_ = DB_BASE_ / DS_NAME_
from lib import FileManager as FM

fm = FM()

fm.ensure_exists(DS_BASE_)

# %%
ORIG_DB_BASE_ = Path("~/datasets/iris_datasets/iris_syn").expanduser()

# %% METADATA Dict

# insert into db
metadata_iris_syn = {
    "ds_id": DS_ID,
    "name": "Syntetic Iris Code using SIC-Gen",
    "doc": "https://docs.google.com/document/d/1N_ninklrJYcEyG0EUuSx_KmBeHvnOYrtBTEw1yMZXNM/edit?tab=t.59tbx9cqw173",
    "fv_tags": ["template", "mask", "feat"],
    "stats": {
        "num_images": 100_000,
        "num_people": 10_000,
        "num_eyes": 10_000,
        "num_eyes_per_person": 1,
        "num_samples_per_eye": 10,
        "num_sessions": 1,
    },
    "fv_specs": {"ext": ".txt", "width": 256, "height": 8},
    "fv": {
        "template": {
            "info": "Templates",
            "old_base_": str(ORIG_DB_BASE_),
        },
        "mask": {
            "info": "Mask",
            "old_base_": str(ORIG_DB_BASE_),
        },
        "feat": {
            "info": "Template & Mask = Features",
            "old_base_": str(ORIG_DB_BASE_),
        },
    },
}
# %% Drop METADATA
with IrisDB() as db:
    meta = db["meta"]
    # meta.delete_one({"ds_id": DS_ID})
# %% Insert METADATA to mongodb

# from datetime import datetime
with IrisDB() as db:
    # Use the db object to interact with the database
    meta = db["meta"]
    print(db.list_ds())
    # db.update(metadata_casia_v1)
    # db.insert(metadata_iris_syn)
    # print(db.update({
    #     'ds_id': db.find_ds('casia-v1'),
    #     'injested_at': datetime.now()
    # }))
    # meta.update_one({'ds_id': db.find_ds('casia-v1')},
    #     {
    #         '$set':{
    #         'ds_id': db.find_ds('casia-v1'),
    #         'injested_at': datetime.now()
    #         }
    #     }
    # )
    # print(db.find_ds('cas'))
    # meta.create_index([("ds_id", 1)], unique=True)
    # db.update_data(
    #     [{
    #         "ds_id": db.find_ds('casia-v1'),
    #         'orig_base': ORIG_DB_BASE_.as_posix(),
    #         'base': DS_BASE_.as_posix()
    #     }],
    #     key='ds_id'
    # )
    # print(db.mongo_conn[db.meta_coll].find({}, {'_id': 0, 'ds_id': 1}))

    pass
# %%
with IrisDB() as db:
    print(db.list_ds())
# %% Insert individual data to CASIA-v1 coll

feats = sorted(list(ORIG_DB_BASE_.rglob(f"*_feat.txt")))
print(len(feats))
templates = sorted(list(ORIG_DB_BASE_.rglob(f"*_template.txt")))
print(len(feats))
masks = sorted(list(ORIG_DB_BASE_.rglob(f"*_mask.txt")))
print(len(feats))
# %%
docs = []
# with IrisDB() as db:
# ds_id = db.find_ds(DS_ID)
ds_id = DS_ID
db = IrisDB(ds_id=DS_ID)
meta_doc = db.find_one({"ds_id": DS_ID})
# %%
db.coll
print(meta_doc)
# %%
try:
    db.coll.create_index([("unique_id", ASCENDING)], unique=True)
    # create a
    db.coll.create_index([("person_id", ASCENDING)])
    db.coll.create_index([("eye_id", ASCENDING)])
    # folder_tags can be one of 'orig','norm','segm'
    db.coll.create_index([("fv_tags", ASCENDING)])
    # db.coll.create_index([("status",ASCENDING)])
    # index path as well
    # db.coll.create_index([("paths.rel_path_", ASCENDING)], unique=True)
    # db.coll.create_index([("paths.path_", ASCENDING)], unique=True)
    # db.coll.create_index([("paths.common_path_",ASCENDING)],unique=True)
    # db.coll.create_index([("paths.full_path_",ASCENDING)],unique=True)
    # db.coll.create_index([("person_id",ASCENDING),("person_sample_id",ASCENDING)],unique=True)
    # db.coll.create_index([("eye_id",ASCENDING),("sample_id",ASCENDING)],unique=True)
except Exception as e:
    lg.error(f"Error creating indexes for {ds_id} collection: {e}")

base_ = DS_BASE_
fv_tags = metadata_iris_syn
session_id = 1
ext = metadata_iris_syn["fv_specs"]["ext"]
db.close()
# %%
# feats[0].relative_to(ORIG_DB_BASE_.parent)
# %%
import numpy as np
db = IrisDB(ds_id=ds_id)
eye = 'L'
for img_,temp_,mask_ in zip(feats,templates,masks):
    # name = YYY/E/S5YYYENN.jpg
    #              01234567
    img_stem = img_.stem  # S5YYYENN
    # person_id, eye, sample_id = int(img_stem[2:5]), img_stem[5], int(img_stem[6:])
    person_id = img_.parent.name
    sample_id = img_stem.split("_")[0]
    if person_id == 0:
        person_id = 1000
    eye_id = f"{person_id}_{eye}"

    # renaming the 2nd session images into continuous id_s
    # if session_id == 2:
    #     sample_id = 3+sample_id
    if sample_id == 0:
        sample_id = 10
    person_sample_id = sample_id
    # if eye == "R":
        # person_sample_id += 10

    new_filename_ = Path(f"{eye_id}_{sample_id}{ext}")
    # status = 'orig'

    rel_path_ = eye_id / new_filename_
    # orig_path_ = 'orig'/rel_path_
    # full_orig_path = DS_BASE_ / orig_path_
    # norm_path_ = 'norm'/rel_path_
    # full_norm_path_ = DS_BASE_ / norm_path_
    # seg_path_ = 'seg'/rel_path_
    # full_seg_path_ = DS_BASE_ / seg_path_
    path_ = base_ / "feat" / rel_path_
    template_path_ = base_ / "mask" / rel_path_
    mask_path_ = base_ / "template" / rel_path_

    # paths = {
    #     "base_": str(base_),
    #     "rel_path_": str(rel_path_),
    #     "path_": str(path_),
    #     # 'orig_path_': str(orig_path_),
    #     # 'full_orig_path_': str(full_orig_path),
    #     # 'norm_path_': str(norm_path_),
    #     # 'full_norm_path_': str(full_norm_path_),
    #     # 'seg_path_': str(seg_path_),
    #     # 'full_seg_path_': str(full_seg_path_)
    # }

    # orig_paths = {
    #     "base_": str(ORIG_DB_BASE_),
    #     "rel_path_": str(img_.relative_to(ORIG_DB_BASE_)),
    #     "path_": str(img_),
    # }
    data = img_.read_text().strip()
    doc = {
        "ds_id": ds_id,
        "person_id": str(person_id),
        "eye_id": eye_id,
        "sample_id": str(sample_id),
        "person_sample_id": str(person_sample_id),
        "unique_id": str(new_filename_.stem),
        "file_name": str(new_filename_),
        "session_id": str(session_id),
        # 'status': status,
        "eye": eye,
        "fvs":{
            "feat": {
                "data":data,
                "path":path_.relative_to(DS_BASE_.parent).as_posix(),
                "fv_dim":[8,256],
                "dim":[2048],
                # "w":256,
                # "h":8
            },
            "template": {
                "data":temp_.read_text().strip().replace('\n',''),
                "path":path_.relative_to(DS_BASE_.parent).as_posix(),
                "fv_dim":[8,256],
                "dim":[2048],
                # "w":256,
                # "h":8
            },
            "mask": {
                "data":mask_.read_text().strip().replace('\n',''),
                "path":path_.relative_to(DS_BASE_.parent).as_posix(),
                "fv_dim":[8,256],
                "dim":[2048],
                # "w":256,
                # "h":8
            }
        },
        'fv':data
        # "fv_specs": meta_doc["img_specs"],
        # "paths": paths,
        # "orig_paths": orig_paths,
        # "injested_at": datetime.now(),
    }
    docs.append(doc)
    # db.insert(doc)
    fm.copy_file(img_, path_)
    fm.copy_file(temp_, template_path_)
    fm.copy_file(mask_, mask_path_ )

db.insert(docs)
db.close()


# %%
len(docs)

# %% Export to jsonl
# db.export_to_jsonl()