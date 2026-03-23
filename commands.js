// Command to view all metadata excluding large fields
db.getCollection("meta").find(
  {},
  {
    orig: 0,
    mask_irisseg: 0,
    "norm_def.stats.num_eyes_per_person": 0,
    "norm_def.stats.num_samples_per_eye": 0,
  }
);

//db.getCollection("IITD_v1").find({})
db.getCollection("IITD_v1").updateMany(
  {}, // empty filter to update all documents
  {
    $rename: {
      "norm_def.orig_rel_path": "norm_def.orig_rel_",
      "mask_irisseg.orig_rel_path": "mask_irisseg.orig_rel_",
      "seg_irisseg.orig_rel_path": "seg_irisseg.orig_rel_",
      "overlay_irisseg.orig_rel_path": "overlay_irisseg.orig_rel_",
    },
  }
);
